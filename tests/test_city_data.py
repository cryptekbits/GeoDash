#!/usr/bin/env python3
"""
Simple test script for the CityData class.
"""
import logging
import time
import os
import sys

# Add the project root directory to Python path
# This allows importing the geo module regardless of where script is run from
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '../..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Now import from geo
from geo import CityData

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_city_data():
    """Test the CityData class."""
    logger.info("Testing CityData class...")
    
    # Create a CityData instance
    start_time = time.time()
    city_data = CityData()
    logger.info(f"Initialization time: {time.time() - start_time:.2f} seconds")
    
    # Test search
    logger.info("Testing search...")
    start_time = time.time()
    cities = city_data.search_cities('New York')
    logger.info(f"Search time: {time.time() - start_time:.2f} seconds")
    logger.info(f"Found {len(cities)} cities")
    
    # Test coordinates
    logger.info("Testing coordinates...")
    start_time = time.time()
    cities = city_data.get_cities_by_coordinates(40.7128, -74.0060, radius_km=10)
    logger.info(f"Coordinates time: {time.time() - start_time:.2f} seconds")
    logger.info(f"Found {len(cities)} cities")
    
    # Test India cache
    logger.info("Testing India cache...")
    start_time = time.time()
    cities = city_data.search_cities('Delhi', country='India')
    logger.info(f"India search time: {time.time() - start_time:.2f} seconds")
    logger.info(f"Found {len(cities)} cities")
    
    # Test countries
    logger.info("Testing countries...")
    start_time = time.time()
    countries = city_data.get_countries()
    logger.info(f"Countries time: {time.time() - start_time:.2f} seconds")
    logger.info(f"Found {len(countries)} countries")
    
    # Close the connection
    city_data.close()
    
    logger.info("All tests passed!")

if __name__ == '__main__':
    test_city_data() 