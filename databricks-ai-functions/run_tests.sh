#!/bin/bash
# Quick test runner for build_pipeline tests

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Set up Python path
export PYTHONPATH="$PROJECT_ROOT/databricks-tools-core:$PROJECT_ROOT/databricks-ai-functions:$PYTHONPATH"

echo "=================================="
echo "Build Pipeline Test Runner"
echo "=================================="
echo ""

# Check which test to run
case "${1:-unit}" in
  unit)
    echo "Running UNIT tests (no auth required)..."
    cd "$SCRIPT_DIR"
    python test_build_pipeline_unit.py
    ;;
  
  integration)
    echo "Running INTEGRATION tests (requires Databricks auth)..."
    echo ""
    
    # Check authentication
    if ! databricks auth env &>/dev/null; then
      echo "⚠️  Warning: Databricks authentication may be required"
      echo "   Run: databricks auth login --host <your-host>"
      echo ""
    fi
    
    cd "$SCRIPT_DIR"
    python test_build_pipeline_integration.py
    ;;
  
  manual)
    echo "Running MANUAL tests..."
    echo ""
    cd "$SCRIPT_DIR"
    python test_build_pipeline.py
    ;;
  
  all)
    echo "Running ALL tests..."
    echo ""
    
    echo "1. Unit Tests..."
    cd "$SCRIPT_DIR"
    python test_build_pipeline_unit.py
    echo ""
    
    echo "2. Integration Tests..."
    python test_build_pipeline_integration.py
    ;;
  
  *)
    echo "Usage: $0 [unit|integration|manual|all]"
    echo ""
    echo "  unit         - Run unit tests with mocking (default, no auth required)"
    echo "  integration  - Run integration tests against Databricks (requires auth)"
    echo "  manual       - Run manual test script"
    echo "  all          - Run both unit and integration tests"
    exit 1
    ;;
esac

echo ""
echo "✅ Tests complete!"

