import json
import os
import pytest

from databricks_ai_functions.sdp import build_pipeline
from databricks_ai_functions._shared import llm as llm_module
from databricks_ai_functions._shared import skills as skills_module


def _fake_plan_json():
    return json.dumps(
        {
            "name": "unit_test_pipeline",
            "catalog": "main",
            "schema": "ai_dev_kit",
            "files": [
                {
                    "rel_path": "transformations/01_bronze_customers.sql",
                    "language": "SQL",
                    "content": "CREATE OR REFRESH MATERIALIZED VIEW bronze_customers AS SELECT 1 AS id;",
                },
                {
                    "rel_path": "transformations/02_silver_customers.sql",
                    "language": "SQL",
                    "content": "CREATE OR REFRESH MATERIALIZED VIEW silver_customers AS SELECT * FROM bronze_customers;",
                },
            ],
            "start_run": False,
            "full_refresh": True,
        }
    )


@pytest.fixture(autouse=True)
def mock_skills(monkeypatch):
    monkeypatch.setattr(skills_module, "load_skills", lambda path: [{"path": "x.md", "text": "skill"}])
    monkeypatch.setattr(
        skills_module, "retrieve_relevant_skills", lambda query, skills, model, k=6: [{"path": "x.md", "snippet": "skill"}]
    )


def test_build_pipeline_unit(monkeypatch):
    # Mock LLM to return a deterministic plan
    monkeypatch.setattr(llm_module, "call_llm", lambda **kwargs: _fake_plan_json())

    # Mock tools-core create_or_update_pipeline to avoid real calls
    class FakeRunResult:
        def __init__(self):
            self.pipeline_id = "123"
            self.success = True
            self.update_id = None
            self.created = True
            self.message = "ok"

    import databricks_tools_core.spark_declarative_pipelines.pipelines as pmod

    monkeypatch.setattr(pmod, "create_or_update_pipeline", lambda **kwargs: FakeRunResult())

    # Mock upload to be a no-op
    import databricks_tools_core.file as fmod

    class FakeFolderUploadResult:
        def __init__(self):
            self.successful = 2
            self.failed = 0
            self.total_files = 2

    monkeypatch.setattr(fmod, "upload_folder", lambda **kwargs: FakeFolderUploadResult())

    result = build_pipeline("Create a simple pipeline", start_run=False)

    assert result["status"] == "success"
    assert result["pipeline_id"] == "123"


@pytest.mark.skipif(
    not (os.getenv("DATABRICKS_HOST") and os.getenv("DATABRICKS_TOKEN")),
    reason="Databricks auth not configured",
)
def test_build_pipeline_integration():
    result = build_pipeline("Create a pipeline from customers to customers_summary")
    assert result["status"] == "success"
    assert "pipeline_id" in result


