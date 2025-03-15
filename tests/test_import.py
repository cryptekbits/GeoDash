#!/usr/bin/env python3
"""
Simple test to verify that the GeoDash module can be imported correctly
and that city data is available.
"""
import logging
import sys
import types
import os
import tempfile
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_import():
    """Test importing the GeoDash module."""
    logger.info("Testing import of GeoDash module...")
    
    try:
        import GeoDash
        from GeoDash import CityData
        from GeoDash.data.importer import download_city_data
        
        logger.info("Successfully imported GeoDash module")
        
        # Check that CityData is a class
        assert isinstance(CityData, type), "CityData is not a class"
        logger.info("CityData is a class")
        
        # Check for module attributes and functions
        assert hasattr(GeoDash, 'start_server'), "start_server function missing"
        logger.info("Module has required functions")
        
        logger.info("All import tests passed!")
        return True
    except ImportError as e:
        logger.error(f"Failed to import from GeoDash module: {e}")
        return False
    except AssertionError as e:
        logger.error(f"Assertion failed: {e}")
        return False

def test_city_data_availability():
    """Test that city data can be downloaded and accessed."""
    logger.info("Testing city data availability...")
    
    try:
        from GeoDash.data.importer import download_city_data
        
        # Test with a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Try to download the city data to the temp directory
            try:
                csv_path = download_city_data(force=True)
                assert os.path.exists(csv_path), "CSV file was not downloaded"
                logger.info(f"Successfully downloaded city data to {csv_path}")
                
                # Check file size to ensure it's not empty
                file_size = os.path.getsize(csv_path)
                assert file_size > 1000, f"CSV file too small ({file_size} bytes)"
                logger.info(f"CSV file size: {file_size / (1024*1024):.2f} MB")
                
                return True
            except Exception as e:
                logger.error(f"Failed to download city data: {e}")
                return False
    except ImportError as e:
        logger.error(f"Failed to import download_city_data: {e}")
        return False

if __name__ == '__main__':
    import_success = test_import()
    data_success = test_city_data_availability()
    
    if import_success and data_success:
        logger.info("All tests passed!")
        sys.exit(0)
    else:
        logger.error("Tests failed")
        sys.exit(1) 