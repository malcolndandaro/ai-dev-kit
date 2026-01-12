"""
Integration tests for compute execution functions.

Tests run_python_file_on_databricks with a real cluster.
"""

import tempfile
import pytest
from pathlib import Path

from databricks_tools_core.compute import (
    run_python_file_on_databricks,
    execute_databricks_command,
)

# Test cluster ID
CLUSTER_ID = "0304-162117-qgsi1x04"


@pytest.mark.integration
class TestRunPythonFileOnDatabricks:
    """Tests for run_python_file_on_databricks function."""

    def test_simple_print(self):
        """Should execute a simple Python file and return output."""
        code = """
print("Hello from Databricks!")
print(1 + 1)
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(code)
            f.flush()
            temp_path = f.name

        try:
            result = run_python_file_on_databricks(
                cluster_id=CLUSTER_ID,
                file_path=temp_path,
                timeout=120
            )

            print(f"\n=== Execution Result ===")
            print(f"Success: {result.success}")
            print(f"Output: {result.output}")
            print(f"Error: {result.error}")

            assert result.success, f"Execution failed: {result.error}"
            assert "Hello from Databricks!" in result.output

        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_spark_code(self):
        """Should execute Spark code and return results."""
        code = """
# Test Spark is available
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.range(5)
print(f"Row count: {df.count()}")
print("Spark execution successful!")
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(code)
            f.flush()
            temp_path = f.name

        try:
            result = run_python_file_on_databricks(
                cluster_id=CLUSTER_ID,
                file_path=temp_path,
                timeout=120
            )

            print(f"\n=== Spark Execution Result ===")
            print(f"Success: {result.success}")
            print(f"Output: {result.output}")
            print(f"Error: {result.error}")

            assert result.success, f"Spark execution failed: {result.error}"
            assert "Row count: 5" in result.output

        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_error_handling(self):
        """Should capture Python errors with details."""
        code = """
# This will raise an error
x = 1 / 0
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(code)
            f.flush()
            temp_path = f.name

        try:
            result = run_python_file_on_databricks(
                cluster_id=CLUSTER_ID,
                file_path=temp_path,
                timeout=120
            )

            print(f"\n=== Error Handling Result ===")
            print(f"Success: {result.success}")
            print(f"Output: {result.output}")
            print(f"Error: {result.error}")

            assert not result.success, "Should have failed with division by zero"
            assert result.error is not None
            assert "ZeroDivisionError" in result.error or "division" in result.error.lower()

        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_file_not_found(self):
        """Should handle missing file gracefully."""
        result = run_python_file_on_databricks(
            cluster_id=CLUSTER_ID,
            file_path="/nonexistent/path/to/file.py",
            timeout=120
        )

        print(f"\n=== File Not Found Result ===")
        print(f"Success: {result.success}")
        print(f"Error: {result.error}")

        assert not result.success
        assert "not found" in result.error.lower() or "nonexistent" in result.error.lower()


@pytest.mark.integration
class TestExecuteDatabricksCommand:
    """Tests for execute_databricks_command function."""

    def test_simple_python_command(self):
        """Should execute a simple Python command and return output."""
        code = """
print("Hello from execute_databricks_command!")
print(2 + 2)
"""
        result = execute_databricks_command(
            cluster_id=CLUSTER_ID,
            language="python",
            code=code,
            timeout=120
        )

        print(f"\n=== Execute Command Result ===")
        print(f"Success: {result.success}")
        print(f"Output: {result.output}")
        print(f"Error: {result.error}")

        assert result.success, f"Execution failed: {result.error}"
        assert "Hello from execute_databricks_command!" in result.output
        assert "4" in result.output

    def test_spark_command(self):
        """Should execute Spark code via execute_databricks_command."""
        code = """
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.range(10)
print(f"Count: {df.count()}")
"""
        result = execute_databricks_command(
            cluster_id=CLUSTER_ID,
            language="python",
            code=code,
            timeout=120
        )

        print(f"\n=== Spark Command Result ===")
        print(f"Success: {result.success}")
        print(f"Output: {result.output}")
        print(f"Error: {result.error}")

        assert result.success, f"Spark execution failed: {result.error}"
        assert "Count: 10" in result.output


if __name__ == "__main__":
    # Run tests directly for quick debugging
    test = TestRunPythonFileOnDatabricks()

    print("\n" + "="*50)
    print("Running: test_simple_print")
    print("="*50)
    try:
        test.test_simple_print()
        print("✓ PASSED")
    except Exception as e:
        print(f"✗ FAILED: {e}")

    print("\n" + "="*50)
    print("Running: test_spark_code")
    print("="*50)
    try:
        test.test_spark_code()
        print("✓ PASSED")
    except Exception as e:
        print(f"✗ FAILED: {e}")

    print("\n" + "="*50)
    print("Running: test_error_handling")
    print("="*50)
    try:
        test.test_error_handling()
        print("✓ PASSED")
    except Exception as e:
        print(f"✗ FAILED: {e}")

    print("\n" + "="*50)
    print("Running: test_file_not_found")
    print("="*50)
    try:
        test.test_file_not_found()
        print("✓ PASSED")
    except Exception as e:
        print(f"✗ FAILED: {e}")

    print("\n" + "="*50)
    print("Running: TestExecuteDatabricksCommand")
    print("="*50)
    test_execute = TestExecuteDatabricksCommand()

    print("\n" + "="*50)
    print("Running: test_simple_python_command")
    print("="*50)
    try:
        test_execute.test_simple_python_command()
        print("✓ PASSED")
    except Exception as e:
        print(f"✗ FAILED: {e}")

    print("\n" + "="*50)
    print("Running: test_spark_command")
    print("="*50)
    try:
        test_execute.test_spark_command()
        print("✓ PASSED")
    except Exception as e:
        print(f"✗ FAILED: {e}")
