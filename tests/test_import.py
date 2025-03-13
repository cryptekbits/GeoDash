#!/usr/bin/env python3
"""
Simple test to verify that the geo module can be imported correctly.
"""
import logging
import sys
import types
import os

# Add the project root directory to Python path
# This allows importing the geo module regardless of where script is run from
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '../..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_import():
    """Test importing the geo module."""
    logger.info("Testing import of geo module...")
    
    try:
        from geo import CityData, start_server
        logger.info("Successfully imported CityData and start_server from geo module")
        
        # Check that CityData is a class
        assert isinstance(CityData, type), "CityData is not a class"
        logger.info("CityData is a class")
        
        # Check that start_server is a function
        assert isinstance(start_server, types.FunctionType), "start_server is not a function"
        logger.info("start_server is a function")
        
        logger.info("All import tests passed!")
        return True
    except ImportError as e:
        logger.error(f"Failed to import from geo module: {e}")
        return False
    except AssertionError as e:
        logger.error(f"Assertion failed: {e}")
        return False

if __name__ == '__main__':
    success = test_import()
    sys.exit(0 if success else 1) 