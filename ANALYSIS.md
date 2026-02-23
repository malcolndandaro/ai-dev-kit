# Databricks AI Dev Kit - Staff Engineer Analysis

## Executive Summary

The AI Dev Kit is a well-structured monorepo providing AI coding assistants (Claude Code, Cursor, etc.) with tools and knowledge to build on Databricks. It includes a core Python library (`databricks-tools-core`), an MCP server (`databricks-mcp-server`), 19+ markdown skills, and a full-stack builder app. The project has solid product-market fit and good foundational architecture, but has significant gaps in reliability engineering, testing strategy, observability, and API design that would block it from being production-grade at scale.

Below are findings organized by severity and impact. Each section includes concrete, actionable recommendations.

---

## 1. CRITICAL: No Unit Tests for the Core Library

**Finding:** `databricks-tools-core` has **zero unit tests**. All 367 `def test_` occurrences are integration tests requiring a live Databricks connection. Only 2 files in the entire repo use `unittest.mock` (`test_middleware.py` and `test_scorers.py`).

**Impact:** Every change requires a live Databricks workspace to validate. Contributors cannot run tests locally without credentials. This makes CI nearly untestable (the CI workflow only runs linting, not tests). Regressions ship silently.

**Recommendations:**
- Add a `tests/unit/` directory to `databricks-tools-core` with mocked `WorkspaceClient` tests for every public function.
- Target >80% line coverage for the core library. Focus on:
  - `sql_utils/executor.py` - polling logic, error classification, timeout handling
  - `sql_utils/dependency_analyzer.py` - DAG construction and cycle detection
  - `sql_utils/parallel_executor.py` - group scheduling, error propagation
  - `jobs/jobs.py` and `jobs/runs.py` - state machine transitions
  - `agent_bricks/manager.py` (1415 lines) - the largest file, with complex polling and batch operations
- Add `pytest-cov` to dev dependencies and enforce coverage thresholds in CI.
- Use `responses` or `respx` to mock HTTP calls in `client.py` without needing a live workspace.

**Files affected:**
- `databricks-tools-core/tests/` (entire directory - only integration tests exist)
- `.github/workflows/ci.yml` (no test step at all)

---

## 2. CRITICAL: CI Pipeline Runs Only Linting

**Finding:** `.github/workflows/ci.yml` has exactly 2 jobs: `lint` (ruff check + format) and `validate-skills` (YAML structure). There is **no test execution, no type checking, no security scanning, no dependency audit**.

**Impact:** PRs are merged with only formatting validation. Logic bugs, type errors, security vulnerabilities, and dependency issues are not caught pre-merge.

**Recommendations:**
- Add a `test` job that runs unit tests (once they exist) on Python 3.9, 3.10, 3.11, 3.12.
- Add `mypy` or `pyright` for static type checking. The codebase uses type hints inconsistently -- this would catch real bugs.
- Add `pip-audit` or `safety` for dependency vulnerability scanning.
- Add a matrix test for the MCP server (`databricks-mcp-server/tests/`).
- Consider a nightly integration test job that runs against a Databricks workspace with a service principal.

**File:** `.github/workflows/ci.yml`

---

## 3. HIGH: Inconsistent Python Version Requirements Across Sub-packages

**Finding:** The three sub-packages declare different minimum Python versions:
- `databricks-tools-core`: `requires-python = ">=3.8"` (classifiers list 3.8-3.11)
- `databricks-mcp-server`: `requires-python = ">=3.9"` (classifiers list 3.9-3.12)
- `databricks-builder-app`: `requires-python = ">=3.11"`

**Impact:** `databricks-tools-core` claims Python 3.8 support but uses features like `str.removesuffix()` (`identity.py:139`) which was introduced in Python 3.9. This is a silent compatibility lie. Users on Python 3.8 will get runtime errors, not install-time failures.

**Recommendations:**
- Set minimum Python version to `>=3.10` across all packages. Python 3.8 and 3.9 are EOL or near-EOL. The Databricks SDK itself requires 3.8+ but practically targets 3.10+.
- Add Python version matrix testing in CI to validate claimed compatibility.
- Update classifiers to match the actual tested versions.

**Files:**
- `databricks-tools-core/pyproject.toml:10`
- `databricks-mcp-server/pyproject.toml:10`

---

## 4. HIGH: `WorkspaceClient` Created Per-Call With No Connection Pooling or Caching

**Finding:** Every function in `databricks-tools-core` calls `get_workspace_client()` which instantiates a **new `WorkspaceClient`** on every invocation. For example:
- `catalogs.py:23` - `w = get_workspace_client()`
- `jobs.py:37` - `w = get_workspace_client()`
- `indexes.py:58` - `client = get_workspace_client()`

This pattern is repeated across all ~40 source files. Each `WorkspaceClient()` call performs config resolution, potential subprocess calls for auth token refresh, and HTTP session setup.

**Impact:** Unnecessary overhead on every API call. In the MCP server context, sequential tool calls each create a fresh client. In the builder app, every request creates multiple clients.

**Recommendations:**
- Cache the `WorkspaceClient` per authentication context. For the MCP server (single-user), a module-level singleton suffices. For the builder app (multi-user), cache per `(host, token)` tuple with TTL-based eviction.
- Consider a `@contextmanager` pattern that creates one client per operation scope:
  ```python
  with workspace_client_scope() as client:
      # all calls within this scope reuse the same client
  ```
- At minimum, cache within `get_workspace_client()` when context variables haven't changed.

**File:** `databricks-tools-core/databricks_tools_core/auth.py:76-119`

---

## 5. HIGH: Dual HTTP Client Anti-Pattern

**Finding:** The codebase has two parallel HTTP abstractions:
1. `client.py` (`DatabricksClient`) - a raw `requests`-based client with manual auth header management
2. `auth.py` (`get_workspace_client()`) - returns a Databricks SDK `WorkspaceClient`

Most modules use `get_workspace_client()` and the SDK's typed methods. But `agent_bricks/manager.py` and `aibi_dashboards/dashboards.py` use raw `requests.get/post` calls with manual auth headers via `self.w.config.authenticate()`.

**Impact:** Two code paths for authentication, error handling, retry logic, and rate limiting. Bugs fixed in one path don't apply to the other. The raw `requests` calls lack the SDK's built-in retry, rate limiting, and error deserialization.

**Recommendations:**
- Standardize on the Databricks SDK `WorkspaceClient` for all API calls. For endpoints not yet supported by the SDK (Agent Bricks, AIBI dashboards), use `WorkspaceClient.api_client.do()` which preserves all SDK middleware.
- Deprecate and remove `client.py` (`DatabricksClient`) entirely. It reimplements what the SDK already provides.
- If raw HTTP is necessary, use `WorkspaceClient.config.authenticate()` + the SDK's `ApiClient` rather than bare `requests`.

**Files:**
- `databricks-tools-core/databricks_tools_core/client.py` (entire file)
- `databricks-tools-core/databricks_tools_core/agent_bricks/manager.py` (raw requests usage)
- `databricks-tools-core/databricks_tools_core/aibi_dashboards/dashboards.py` (raw requests usage)

---

## 6. HIGH: `_has_oauth_credentials()` Duplicated Across Files

**Finding:** The function `_has_oauth_credentials()` is defined identically in both `auth.py:42` and `client.py:20`:
```python
def _has_oauth_credentials() -> bool:
    return bool(os.environ.get("DATABRICKS_CLIENT_ID") and os.environ.get("DATABRICKS_CLIENT_SECRET"))
```

**Impact:** DRY violation. If the OAuth detection logic needs to change (e.g., to support workload identity federation), it must be updated in two places.

**Recommendation:** Define once in `auth.py` and import in `client.py`, or better yet, eliminate `client.py` entirely (see #5).

---

## 7. HIGH: Global Mutable State With No Thread Safety

**Finding:** Several modules use `global` variables for caching:
- `identity.py:39-40` - `_cached_project`, `_cached_config`
- `auth.py:38-39` - `_current_username`, `_current_username_fetched`
- `agent_bricks/manager.py:1410` - `_tile_example_queue`

None of these use locks or thread-safe patterns.

**Impact:** In the builder app (FastAPI with multiple async workers) or the parallel SQL executor (`ThreadPoolExecutor`), concurrent writes to these globals can cause race conditions. The username cache is particularly risky: one request's auth context could leak into another.

**Recommendations:**
- Replace global caches with `threading.local()` or `contextvars.ContextVar` for per-request state.
- For truly process-global caches (project name, config), use `functools.lru_cache` or a lock-protected pattern.
- The `_current_username` cache is architecturally wrong in a multi-user context -- it caches one user's identity process-wide. Replace with a per-context approach.

**Files:**
- `databricks-tools-core/databricks_tools_core/identity.py:39-40`
- `databricks-tools-core/databricks_tools_core/auth.py:38-39`

---

## 8. HIGH: No Retry Logic on HTTP Calls

**Finding:** All HTTP calls in `client.py` and raw `requests` usage elsewhere perform a single attempt with no retry on transient failures (429, 503, connection errors). The `execute_sql` path polls via the SDK which has its own retry, but direct REST calls do not.

**Impact:** Transient network issues or rate limiting cause immediate user-visible failures. This is particularly problematic for long-running operations like pipeline updates or job runs where a single polling failure aborts the entire operation.

**Recommendations:**
- Use `urllib3.util.Retry` with `requests.adapters.HTTPAdapter` or `tenacity` for raw HTTP calls.
- Better yet, consolidate all HTTP calls through the SDK (see #5), which has built-in retry logic.
- For polling loops (e.g., `pipelines.py`, `jobs/runs.py`), add retry-on-transient-error around individual status checks.

---

## 9. MEDIUM: f-string Logging Throughout Codebase (378 Occurrences)

**Finding:** Logging calls use f-strings everywhere:
```python
logger.debug(f"Executing SQL query: {sql_query[:100]}...")
logger.info(f"Auto-selected warehouse: {warehouse_id}")
```

**Impact:** f-strings are evaluated eagerly regardless of log level. At `INFO` level, all `DEBUG` f-strings still compute string interpolation. In hot paths (SQL execution, polling loops), this adds unnecessary overhead. More importantly, it prevents log aggregation tools from grouping messages by template.

**Recommendation:** Use lazy `%`-style formatting:
```python
logger.debug("Executing SQL query: %s...", sql_query[:100])
```

This is a widespread issue (378 occurrences across 37 files) best addressed incrementally with a ruff rule (`G004`).

---

## 10. MEDIUM: `agent_bricks/manager.py` is 1415 Lines -- God Class

**Finding:** `AgentBricksManager` at 1415 lines is a single class handling Genie Spaces, Knowledge Assistants, and Supervisor Agents. It contains 40+ methods, thread pool management, polling loops, and batch operations.

**Impact:** Difficult to test, review, and maintain. A bug fix in Genie code risks breaking KA code. New contributors face a steep learning curve.

**Recommendations:**
- Split into three focused classes: `GenieManager`, `KnowledgeAssistantManager`, `SupervisorManager`.
- Extract polling and batch utilities into a shared base class or standalone utilities.
- Each class should be in its own file within the `agent_bricks/` package.

**File:** `databricks-tools-core/databricks_tools_core/agent_bricks/manager.py`

---

## 11. MEDIUM: No Input Validation at API Boundaries

**Finding:** MCP tool functions accept user input directly with minimal validation. For example:
- `execute_sql()` accepts raw SQL strings with no parameterization or sanitization
- `table_stat_level` is a raw string converted to enum via `TableStatLevel[table_stat_level.upper()]` -- an invalid value causes an unhandled `KeyError`
- `get_table_details()` accepts GLOB patterns that are passed directly to API calls

**Impact:** Cryptic error messages for invalid input. MCP tool callers (LLMs) get Python tracebacks instead of actionable error messages.

**Recommendations:**
- Validate inputs at the MCP tool layer and raise descriptive errors.
- Use Pydantic models for MCP tool input validation (FastMCP supports this natively).
- Convert `KeyError` on enum lookups to descriptive `ValueError` with valid options listed.
- For SQL execution, this is by-design (users need to run arbitrary SQL), but add documentation about the trust model.

**File:** `databricks-mcp-server/databricks_mcp_server/tools/sql.py:145`

---

## 12. MEDIUM: Monorepo Without Workspace Management

**Finding:** The repo contains 4 independent Python packages but has no workspace management (no `uv` workspace config, no `pip install -e` instructions for cross-package development, no monorepo tool like `hatch` or `pdm`). Each package has its own `pyproject.toml` but no declared dependency path to siblings.

**Impact:** Developers must manually install packages in dependency order. The builder app bundles `databricks-tools-core` and `databricks-mcp-server` via a `packages/` directory copy (fragile). There's no single command to set up the dev environment.

**Recommendations:**
- Add a root `pyproject.toml` with `uv` workspace configuration:
  ```toml
  [tool.uv.workspace]
  members = ["databricks-tools-core", "databricks-mcp-server", "databricks-builder-app"]
  ```
- Use path dependencies: `databricks-mcp-server` should declare `databricks-tools-core = {path = "../databricks-tools-core", develop = true}`.
- Add a `Makefile` or `justfile` with `dev-setup`, `lint`, `test`, `build` targets.
- Document the development setup in `CONTRIBUTING.md`.

---

## 13. MEDIUM: Version Hardcoded in Multiple Places

**Finding:** Version `0.1.0` appears in:
- `VERSION` (root)
- `databricks-tools-core/pyproject.toml:8`
- `databricks-tools-core/databricks_tools_core/__init__.py:8`
- `databricks-tools-core/databricks_tools_core/identity.py:35` (`PRODUCT_VERSION`)
- `databricks-mcp-server/pyproject.toml:8`
- `databricks-builder-app/pyproject.toml:8`

**Impact:** Version bumps require editing 6+ files manually. Easy to miss one, causing version skew between what's reported in user-agent headers vs. what's in the package metadata.

**Recommendations:**
- Use `setuptools-scm` or `hatch-vcs` to derive version from git tags.
- Or use dynamic versioning: read from the root `VERSION` file in all `pyproject.toml` files.
- At minimum, `identity.py` should read `__version__` from `__init__.py` rather than hardcoding.

---

## 14. MEDIUM: Error Handling Leaks Implementation Details

**Finding:** Many functions let SDK exceptions propagate directly:
```python
def list_catalogs() -> List[CatalogInfo]:
    w = get_workspace_client()
    return list(w.catalogs.list())  # DatabricksError propagates raw
```

The docstring says `Raises: DatabricksError` but the actual exception includes internal details (HTTP status codes, raw error JSON, request IDs) that are unhelpful to MCP tool callers.

**Impact:** LLMs receive raw Python tracebacks with SDK internals instead of actionable error messages. This degrades the AI assistant's ability to recover gracefully.

**Recommendations:**
- Define a domain error hierarchy in `databricks-tools-core` (e.g., `PermissionError`, `ResourceNotFoundError`, `QuotaExceededError`).
- Catch SDK exceptions at the core library boundary and translate to domain errors with user-friendly messages.
- The MCP server layer should further format these for LLM consumption.

---

## 15. MEDIUM: Builder App Exposes Internal Errors to Clients

**Finding:** `app.py:97-103`:
```python
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception(f'Unhandled exception for {request.method} {request.url}: {exc}')
    return JSONResponse(
        status_code=500,
        content={'detail': 'Internal Server Error', 'error': str(exc)},
    )
```

`str(exc)` can contain stack traces, file paths, database connection strings, or token fragments.

**Impact:** Information disclosure vulnerability. Internal implementation details are exposed to HTTP clients.

**Recommendations:**
- Remove `'error': str(exc)` from the production error response. Log it server-side only.
- In development mode, optionally include error details.
- Add request correlation IDs so users can reference specific errors in bug reports.

**File:** `databricks-builder-app/server/app.py:97-103`

---

## 16. LOW: Dependency Pinning Strategy is Inconsistent

**Finding:**
- `databricks-tools-core` uses floor pins: `requests>=2.31.0`, `pydantic>=2.0.0`
- `databricks-builder-app` mixes floor pins with exact pins: `tenacity==9.0.0`, `pillow==11.1.0`, `pandas==2.3.0`
- The builder app pins are commented as "Conflict resolution pins for Databricks Apps pre-installed packages"
- `uv.lock` files exist but only in `databricks-tools-core` and `databricks-builder-app`

**Recommendations:**
- Use floor pins in `pyproject.toml` for libraries (`databricks-tools-core`, `databricks-mcp-server`).
- Use exact pins only in application lockfiles (`uv.lock`).
- Add `uv.lock` to `databricks-mcp-server`.
- Document the pinning strategy in `CONTRIBUTING.md`.

---

## 17. LOW: AGPL-3.0 Dependency (pymupdf) Could Be a Licensing Concern

**Finding:** `pymupdf>=1.24.0` is listed as a dependency. PyMuPDF is licensed under AGPL-3.0, which requires that any software that links to it (even indirectly) must also be distributed under AGPL-3.0 or a compatible license. This project uses the Databricks License (proprietary).

**Impact:** Potential license incompatibility. Legal review recommended.

**Recommendations:**
- Consult with legal on AGPL-3.0 compatibility with the Databricks License.
- Consider making `pymupdf` an optional dependency (`pip install databricks-tools-core[pdf]`).
- Alternatively, evaluate MIT/Apache-licensed alternatives like `pypdf` or `pdfplumber`.

**File:** `databricks-tools-core/pyproject.toml:30`

---

## 18. LOW: No Rate Limiting or Concurrency Controls in MCP Server

**Finding:** The MCP server registers 50+ tools with no rate limiting, concurrency limits, or request queuing. All tools run synchronously on Windows (with `asyncio.to_thread` wrapper) and directly on the event loop on Unix.

**Impact:** A misbehaving AI client could fire 50 concurrent SQL queries, overwhelming the warehouse. There's no backpressure mechanism.

**Recommendations:**
- Add a semaphore-based concurrency limiter at the MCP server level (e.g., max 5 concurrent tool calls).
- Consider per-tool-type limits (e.g., max 3 concurrent SQL executions, max 1 concurrent pipeline update).
- The `TimeoutHandlingMiddleware` is a good pattern -- extend it with a `ConcurrencyMiddleware`.

---

## Summary of Recommendations by Priority

### P0 (Ship Blockers)
1. Add unit tests for `databricks-tools-core` with >80% coverage
2. Add test execution to CI pipeline
3. Fix Python version claims (bump to >=3.10)

### P1 (Next Quarter)
4. Cache `WorkspaceClient` instances per auth context
5. Eliminate dual HTTP client pattern -- standardize on SDK
6. Fix thread safety of global caches
7. Add retry logic for HTTP calls
8. Fix error information disclosure in builder app

### P2 (Roadmap)
9. Convert f-string logging to lazy formatting
10. Split `AgentBricksManager` god class
11. Add input validation at MCP tool boundaries
12. Implement monorepo workspace management
13. Single-source version management
14. Domain error hierarchy
15. AGPL-3.0 license review for pymupdf
16. MCP server concurrency controls
