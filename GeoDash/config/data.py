"""
Data configuration utilities for GeoDash.

This module provides utilities for working with GeoDash data configuration,
including functions for filtering and accessing data configuration options.
"""

from typing import List, Dict, Any, Optional, Union
from GeoDash.config import get_config

def is_country_enabled(country_code: str) -> bool:
    """
    Check if a specific country is enabled in the configuration.
    
    Args:
        country_code: ISO country code to check
        
    Returns:
        True if the country is enabled, False otherwise
    """
    if not country_code:
        return False
        
    # Get the list of enabled countries from configuration
    enabled_countries = get_config().get_enabled_countries()
    
    # If None is returned, all countries are enabled
    if enabled_countries is None:
        return True
        
    # Check if the country code is in the enabled list
    return country_code.upper() in enabled_countries

def filter_cities_by_countries(cities: List[Dict[str, Any]], enabled_countries: Optional[List[str]]) -> List[Dict[str, Any]]:
    """
    Filter a list of city dictionaries by country.
    
    Args:
        cities: List of city dictionaries, each containing at least a 'country_code' key
        enabled_countries: List of enabled country codes or None to include all
        
    Returns:
        Filtered list of cities
    """
    # If all countries are enabled, return the original list
    if enabled_countries is None:
        return cities
        
    # Convert enabled countries to set for faster lookup
    enabled_countries_set = set(enabled_countries)
    
    # Filter cities by country code
    return [
        city for city in cities
        if 'country_code' in city and city['country_code'].upper() in enabled_countries_set
    ]

def get_download_url() -> str:
    """
    Get the configured download URL for city data.
    
    Returns:
        Download URL for city data
    """
    return get_config().get(
        "data.download_url", 
        "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/csv/cities.csv"
    )

def get_batch_size() -> int:
    """
    Get the configured batch size for data imports.
    
    Returns:
        Batch size for data imports
    """
    return get_config().get("data.batch_size", 5000) 