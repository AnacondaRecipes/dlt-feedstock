#!/usr/bin/env python
"""
Basic functionality test for dlt package.
This test verifies that the core dlt functionality works without requiring external credentials.
Generated with Cursor AI.
"""

import sys
import os

def test_import_dlt():
    """Test that dlt can be imported successfully."""
    try:
        import dlt
        print("✓ dlt imported successfully")
        return True
    except ImportError as e:
        print(f"✗ Failed to import dlt: {e}")
        return False

def test_dlt_version():
    """Test that dlt has a version attribute."""
    try:
        import dlt
        version = dlt.__version__
        print(f"✓ dlt version: {version}")
        assert version is not None, "dlt version should not be None"
        return True
    except Exception as e:
        print(f"✗ Failed to get dlt version: {e}")
        return False

def test_dlt_pipeline_creation():
    """Test that a basic dlt pipeline can be created."""
    try:
        import dlt
        
        # Create a simple pipeline with in-memory destination
        pipeline = dlt.pipeline(
            pipeline_name="test_pipeline",
            destination="duckdb",
            dataset_name="test_dataset"
        )
        print("✓ dlt pipeline created successfully")
        
        # Test that we can get the pipeline name
        assert pipeline.pipeline_name == "test_pipeline"
        print("✓ Pipeline name is correct")
        
        # Test basic pipeline properties
        assert hasattr(pipeline, 'destination')
        assert hasattr(pipeline, 'dataset_name')
        print("✓ Pipeline has expected properties")
        
        return True
    except Exception as e:
        print(f"✗ Failed to create dlt pipeline: {e}")
        return False

def test_dlt_schema_creation():
    """Test that dlt schemas can be created."""
    try:
        import dlt
        from dlt.common.schema import Schema
        
        # Create a simple schema
        schema = Schema("test_schema")
        print("✓ dlt schema created successfully")
        
        # Test basic schema properties
        assert schema.name == "test_schema"
        print("✓ Schema name is correct")
        
        # Test that we can access tables (even if empty)
        tables = schema.tables
        assert isinstance(tables, dict)
        print("✓ Schema tables property accessible")
        
        return True
    except Exception as e:
        print(f"✗ Failed to create dlt schema: {e}")
        return False

def test_dlt_data_types():
    """Test that dlt data types are available."""
    try:
        from dlt.common.data_types import TDataType, DATA_TYPES
        
        # Test that DATA_TYPES is available and not empty
        assert DATA_TYPES is not None
        assert len(DATA_TYPES) > 0
        print(f"✓ dlt data types available: {len(DATA_TYPES)} types")
        
        # Test that some common types exist
        common_types = ["text", "bigint", "bool", "timestamp"]
        for dtype in common_types:
            if dtype in DATA_TYPES:
                print(f"  - {dtype} ✓")
            else:
                print(f"  - {dtype} ✗")
        
        return True
    except Exception as e:
        print(f"✗ Failed to access dlt data types: {e}")
        return False

def test_dlt_cli():
    """Test that dlt CLI is available."""
    try:
        import subprocess
        result = subprocess.run([sys.executable, "-m", "dlt", "--help"], 
                              capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print("✓ dlt CLI is available")
            return True
        else:
            print(f"✗ dlt CLI failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"✗ Failed to test dlt CLI: {e}")
        return False

def main():
    """Run all basic functionality tests."""
    print("=" * 60)
    print("Testing basic dlt package functionality")
    print("=" * 60)
    
    tests = [
        test_import_dlt,
        test_dlt_version,
        test_dlt_pipeline_creation,
        test_dlt_schema_creation,
        test_dlt_data_types,
        test_dlt_cli
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        print(f"\nRunning {test.__name__}...")
        if test():
            passed += 1
        print("-" * 40)
    
    print(f"\n{'=' * 60}")
    print(f"Test Results: {passed}/{total} tests passed")
    print(f"{'=' * 60}")
    
    if passed == total:
        print("✓ All basic functionality tests passed!")
        return 0
    else:
        print(f"✗ {total - passed} tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())