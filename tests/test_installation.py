#!/usr/bin/env python3
"""
Test script to verify the installation process and data file inclusion.
This test should be run after the package is installed with pip.
"""
import logging
import os
import sys
import importlib
import pkg_resources

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_package_installation():
    """Test that the package is installed correctly."""
    logger.info("Testing package installation...")
    
    try:
        # Check if GeoDash is installed
        import GeoDash
        logger.info(f"GeoDash version: {GeoDash.__version__ if hasattr(GeoDash, '__version__') else 'unknown'}")
        
        # Get the package location
        package_location = os.path.dirname(os.path.abspath(GeoDash.__file__))
        logger.info(f"Package installed at: {package_location}")
        
        # Check for data directory
        data_dir = os.path.join(package_location, 'data')
        assert os.path.isdir(data_dir), f"Data directory not found at {data_dir}"
        
        # Check for static directory
        static_dir = os.path.join(package_location, 'static')
        assert os.path.isdir(static_dir), f"Static directory not found at {static_dir}"
        
        logger.info("Package installation test passed!")
        return True
    except ImportError as e:
        logger.error(f"GeoDash package not installed: {e}")
        return False
    except AssertionError as e:
        logger.error(f"Package structure test failed: {e}")
        return False

def test_data_files_inclusion():
    """Test that data files are included in the installation."""
    logger.info("Testing data files inclusion...")
    
    try:
        import GeoDash
        
        # Get the package location
        package_location = os.path.dirname(os.path.abspath(GeoDash.__file__))
        
        # Check for potential city data file locations
        data_dir = os.path.join(package_location, 'data')
        csv_path = os.path.join(data_dir, 'cities.csv')
        db_path = os.path.join(data_dir, 'cities.db')
        
        # Check if either the CSV or DB file exists
        if os.path.exists(csv_path):
            logger.info(f"Found cities.csv at {csv_path}")
            file_size = os.path.getsize(csv_path)
            logger.info(f"CSV file size: {file_size / (1024*1024):.2f} MB")
            assert file_size > 1000, f"CSV file too small ({file_size} bytes)"
        elif os.path.exists(db_path):
            logger.info(f"Found cities.db at {db_path}")
            file_size = os.path.getsize(db_path)
            logger.info(f"DB file size: {file_size / 1024:.2f} KB")
            assert file_size > 1000, f"DB file too small ({file_size} bytes)"
        else:
            # If no data file exists, test that we can download it
            from GeoDash.data.importer import download_city_data
            logger.info("No city data file found, testing download capability...")
            csv_path = download_city_data()
            assert os.path.exists(csv_path), "Failed to download city data"
            logger.info(f"Successfully downloaded city data to {csv_path}")
        
        # Test that static files are included
        static_dir = os.path.join(package_location, 'static')
        static_files = os.listdir(static_dir)
        logger.info(f"Found {len(static_files)} files in static directory")
        assert len(static_files) > 0, "No static files found"
        
        logger.info("Data files inclusion test passed!")
        return True
    except ImportError as e:
        logger.error(f"Failed to import GeoDash: {e}")
        return False
    except Exception as e:
        logger.error(f"Data files test failed: {e}")
        return False

def test_runtime_data_access():
    """Test that city data can be accessed at runtime."""
    logger.info("Testing runtime data access...")
    
    try:
        from GeoDash import CityData
        
        # Create a CityData instance
        city_data = CityData()
        
        # Check if we can get data
        table_info = city_data.get_table_info()
        count = table_info.get('count', 0)
        logger.info(f"City data table contains {count} records")
        
        if count == 0:
            # If table is empty, try to import data
            logger.info("Table is empty, testing import capability...")
            result = city_data.import_city_data()
            assert result, "Failed to import city data"
            
            # Check again after import
            table_info = city_data.get_table_info()
            count = table_info.get('count', 0)
            logger.info(f"After import, table contains {count} records")
            assert count > 0, "Table is still empty after import"
        
        # Test search function
        cities = city_data.search_cities("London", limit=5)
        logger.info(f"Found {len(cities)} cities matching 'London'")
        assert len(cities) > 0, "No cities found in search"
        
        # Close the connection
        city_data.close()
        
        logger.info("Runtime data access test passed!")
        return True
    except ImportError as e:
        logger.error(f"Failed to import CityData: {e}")
        return False
    except Exception as e:
        logger.error(f"Runtime data access test failed: {e}")
        return False

if __name__ == '__main__':
    installation_success = test_package_installation()
    data_files_success = test_data_files_inclusion()
    runtime_success = test_runtime_data_access()
    
    if installation_success and data_files_success and runtime_success:
        logger.info("All installation tests passed!")
        sys.exit(0)
    else:
        logger.error("Installation tests failed")
        sys.exit(1) 