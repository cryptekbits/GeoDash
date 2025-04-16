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
    
    # Setting the log level
    from GeoDash import set_log_level
    set_log_level('debug')  # Show more detailed logs
"""

import os
from importlib import import_module
from pathlib import Path
from typing import List, Optional, Dict, Any, Union, Tuple

# Import configuration system first
from GeoDash.config import get_config

# Import and configure logging early
from GeoDash.utils.logging import get_logger, set_log_level, configure_logging

__version__ = '1.0.0'

# Get a logger for the main package
logger = get_logger(__name__)

def initialize_config() -> bool:
    """
    Initialize the GeoDash configuration system.
    
    This function searches for a configuration file in standard locations
    and loads it if found. If not found, defaults are used.
    
    Returns:
        bool: True if a config file was found and loaded, False if using defaults
    """
    logger.debug("Initializing configuration system")
    return get_config().load_config()

def initialize() -> bool:
    """
    Check if city data exists and download it if not found.
    
    This function checks standard locations for the city data file and
    downloads it if not found. It should be called explicitly when
    the application needs to ensure city data is available.
    
    Returns:
        bool: True if city data was found or successfully downloaded,
              False if download failed
    """
    # Initialize configuration first
    initialize_config()
    
    # Try to find cities.csv in standard locations
    standard_locations: List[str] = [
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

# Import key components to expose in the package namespace
# These imports are done after logging configuration to ensure they use the configured logging
from GeoDash.data.city_manager import CityData
from GeoDash.api.server import start_server

# Export key functions for public API
__all__ = ['CityData', 'start_server', 'initialize', 'initialize_config', 'set_log_level', 'get_config'] 