#!/usr/bin/env python3
"""
Test script for the GeoDash CityData class.
Tests include data loading, searching, and automatic downloading capabilities.
"""
import logging
import time
import os
import sys
import tempfile
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_city_data_basic():
    """Test basic functionality of the CityData class."""
    logger.info("Testing CityData basic functionality...")
    
    # Create a CityData instance
    start_time = time.time()
    from GeoDash import CityData
    city_data = CityData()
    logger.info(f"Initialization time: {time.time() - start_time:.2f} seconds")
    
    # Test search
    logger.info("Testing search...")
    start_time = time.time()
    cities = city_data.search_cities('New York')
    logger.info(f"Search time: {time.time() - start_time:.2f} seconds")
    logger.info(f"Found {len(cities)} cities")
    
    # Verify we have results
    assert len(cities) > 0, "No cities found in search"
    assert cities[0]['name'], "City name is missing"
    
    # Test coordinates
    logger.info("Testing coordinates...")
    start_time = time.time()
    cities = city_data.get_cities_by_coordinates(40.7128, -74.0060, radius_km=10)
    logger.info(f"Coordinates time: {time.time() - start_time:.2f} seconds")
    logger.info(f"Found {len(cities)} cities")
    
    # Test country filter
    logger.info("Testing country filter...")
    start_time = time.time()
    cities = city_data.search_cities('Delhi', country='India')
    logger.info(f"India search time: {time.time() - start_time:.2f} seconds")
    logger.info(f"Found {len(cities)} cities")
    assert len(cities) > 0, "No cities found with country filter"
    
    # Test countries list
    logger.info("Testing countries...")
    start_time = time.time()
    countries = city_data.get_countries()
    logger.info(f"Countries time: {time.time() - start_time:.2f} seconds")
    logger.info(f"Found {len(countries)} countries")
    assert len(countries) > 0, "No countries found"
    
    # Test table info
    logger.info("Testing table info...")
    table_info = city_data.get_table_info()
    logger.info(f"Table has {table_info.get('count', 0)} records")
    assert table_info.get('count', 0) > 0, "Table is empty"
    
    # Close the connection
    city_data.close()
    
    logger.info("Basic CityData tests passed!")
    return True

def test_automatic_download():
    """Test the automatic download and import functionality."""
    logger.info("Testing automatic download and import...")
    
    try:
        # Use a temporary directory for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            # Set up a clean environment for testing automatic download
            db_path = os.path.join(temp_dir, 'test_cities.db')
            db_uri = f"sqlite:///{db_path}"
            
            # Initialize with empty database
            from GeoDash import CityData
            from GeoDash.data.importer import download_city_data
            
            # First ensure we can download city data
            csv_path = download_city_data(force=True)
            assert os.path.exists(csv_path), "Failed to download city data"
            
            # Now test with new CityData instance pointed to empty database
            city_data = CityData(db_uri=db_uri)
            
            # It should have auto-initialized
            table_info = city_data.get_table_info()
            count = table_info.get('count', 0)
            logger.info(f"Database has {count} records after auto-initialization")
            
            # Verify we have cities in the database
            assert count > 0, "Database is empty after initialization"
            
            # Test that we can search
            cities = city_data.search_cities("London", limit=5)
            assert len(cities) > 0, "No cities found in search after auto-initialization"
            
            # Close the connection
            city_data.close()
            
            logger.info("Automatic download and import test passed!")
            return True
    except Exception as e:
        logger.error(f"Automatic download test failed: {e}")
        return False

if __name__ == '__main__':
    basic_success = test_city_data_basic()
    auto_success = test_automatic_download()
    
    if basic_success and auto_success:
        logger.info("All CityData tests passed!")
        sys.exit(0)
    else:
        logger.error("CityData tests failed")
        sys.exit(1) 