"""
Utility functions for the CitiZen package.

This module provides utility functions used across the CitiZen package, 
including JSON formatting, output formatting, and other helper functions.
"""

import json
import logging
import sys
from typing import Any, Dict, List, Optional, Union

# Configure logging with proper format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def format_json(data: Any, indent: int = 2, sort_keys: bool = False) -> str:
    """
    Format data as JSON string with proper encoding.
    
    This function converts Python data structures to a formatted JSON string,
    ensuring proper UTF-8 encoding and handling of special characters.
    
    Args:
        data: The data to format as JSON
        indent: Number of spaces for indentation (default: 2)
        sort_keys: Whether to sort dictionary keys (default: False)
        
    Returns:
        A formatted JSON string
        
    Example:
        >>> data = {'name': 'New York', 'coordinates': {'lat': 40.7128, 'lng': -74.0060}}
        >>> formatted = format_json(data)
        >>> print(formatted)
        {
          "name": "New York",
          "coordinates": {
            "lat": 40.7128,
            "lng": -74.006
          }
        }
    """
    try:
        return json.dumps(
            data,
            indent=indent,
            ensure_ascii=False,
            sort_keys=sort_keys,
            default=str  # Handle non-serializable types
        )
    except Exception as e:
        logger.error(f"Error formatting JSON: {str(e)}")
        return json.dumps({"error": "Failed to format JSON data"})

def print_json(data: Any, indent: int = 2, sort_keys: bool = False) -> None:
    """
    Print data as formatted JSON to stdout.
    
    This function formats data as JSON and prints it to standard output,
    handling encoding properly.
    
    Args:
        data: The data to print as JSON
        indent: Number of spaces for indentation (default: 2)
        sort_keys: Whether to sort dictionary keys (default: False)
        
    Example:
        >>> cities = [{'name': 'Paris'}, {'name': 'Tokyo'}]
        >>> print_json(cities)
        [
          {
            "name": "Paris"
          },
          {
            "name": "Tokyo"
          }
        ]
    """
    try:
        formatted = format_json(data, indent, sort_keys)
        print(formatted)
    except Exception as e:
        logger.error(f"Error printing JSON: {str(e)}")
        print(f"Error: Failed to print JSON data - {str(e)}")

def safe_get(data: Dict[str, Any], key_path: str, default: Any = None) -> Any:
    """
    Safely get a value from a nested dictionary using a dot-separated path.
    
    This function provides a safe way to access nested dictionary values without
    raising exceptions if intermediate keys don't exist.
    
    Args:
        data: The dictionary to extract values from
        key_path: A dot-separated path to the desired value (e.g., 'user.address.city')
        default: The default value to return if the path doesn't exist
        
    Returns:
        The value at the specified path or the default value if not found
        
    Example:
        >>> data = {'user': {'name': 'John', 'address': {'city': 'New York'}}}
        >>> safe_get(data, 'user.address.city')
        'New York'
        >>> safe_get(data, 'user.phone', 'Unknown')
        'Unknown'
    """
    if not data or not isinstance(data, dict):
        return default
        
    keys = key_path.split('.')
    value = data
    
    try:
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value
    except Exception:
        return default 