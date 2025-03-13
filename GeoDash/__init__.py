"""
CitiZen - A Python module for managing city data with fast coordinate queries and autocomplete functionality.

This module provides tools for working with city data, including database-backed storage,
coordinate-based queries, autocomplete search, and a REST API.

Key Components:
- CityData: Core class for managing city data with fast coordinate and name lookups
- API Server: REST API for accessing city data programmatically or via web interface
- CLI: Command-line interface for working with city data

Usage Examples:
    # Basic usage with CityData
    from citizen import CityData
    cities = CityData()
    results = cities.search_cities("new york")
    
    # Starting the API server
    from citizen import start_server
    start_server(host='localhost', port=8080)
"""

import os
import logging
from importlib import import_module
from pathlib import Path

__version__ = '1.0.0'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def _check_city_data():
    """
    Check if city data exists and download it if not found.
    This function is called when the module is first imported.
    """
    # Try to find cities.csv in standard locations
    standard_locations = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'cities.csv'),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'cities.csv'),
    ]
    
    for path in standard_locations:
        if os.path.exists(path):
            logger.debug(f"City data found at {path}")
            return
    
    # If we got here, the data file isn't found - try to download it
    try:
        # Import the download function - this avoids circular imports
        importer_module = import_module('citizen.data.importer')
        download_func = getattr(importer_module, 'download_city_data')
        
        logger.info("City data not found, attempting to download...")
        download_func()
        logger.info("City data downloaded successfully")
    except Exception as e:
        logger.warning(f"Could not download city data: {e}")
        logger.info("You can manually download city data later using: citizen.data.importer.download_city_data()")

# Check for city data on import
_check_city_data()

# Import public-facing classes and functions
from citizen.data.city_manager import CityData
from citizen.api.server import start_server

# Define what's imported with `from citizen import *`
__all__ = ['CityData', 'start_server', '__version__'] 