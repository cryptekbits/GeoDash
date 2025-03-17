"""
GeoDash - A Python module for managing city data with fast coordinate queries and autocomplete functionality.

This module provides tools for working with city data, including database-backed storage,
coordinate-based queries, autocomplete search, and a REST API.

Key Components:
- CityData: Core class for managing city data with fast coordinate and name lookups
- API Server: REST API for accessing city data programmatically or via web interface
- CLI: Command-line interface for working with city data

Usage Examples:
    # Basic usage with CityData
    from GeoDash import CityData
    cities = CityData()
    results = cities.search_cities("new york")
    
    # Starting the API server
    from GeoDash import start_server
    start_server(host='localhost', port=8080)
    
    # Initializing city data
    from GeoDash import initialize
    initialize()
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

def initialize():
    """
    Check if city data exists and download it if not found.
    
    This function checks standard locations for the city data file and
    downloads it if not found. It should be called explicitly when
    the application needs to ensure city data is available.
    
    Returns:
        bool: True if city data was found or successfully downloaded,
              False if download failed
    """
    # Try to find cities.csv in standard locations
    standard_locations = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'cities.csv'),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'cities.csv'),
    ]
    
    for path in standard_locations:
        if os.path.exists(path):
            logger.debug(f"City data found at {path}")
            return True
    
    # If we got here, the data file isn't found - try to download it
    try:
        # Import the download function - this avoids circular imports
        importer_module = import_module('GeoDash.data.importer')
        download_func = getattr(importer_module, 'download_city_data')
        
        logger.info("City data not found, attempting to download...")
        download_func()
        logger.info("City data downloaded successfully")
        return True
    except Exception as e:
        logger.warning(f"Could not download city data: {e}")
        logger.info("You can manually download city data later using: GeoDash.data.importer.download_city_data()")
        return False

# Import public-facing classes and functions
from GeoDash.data.city_manager import CityData
from GeoDash.api.server import start_server

# Define what's imported with `from GeoDash import *`
__all__ = [
    'CityData',      # Facade for accessing and managing city data
    'start_server',  # Function to start the GeoDash API server
    'initialize',    # Function to check and download city data
    '__version__'    # Package version
] 