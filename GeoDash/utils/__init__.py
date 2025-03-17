"""
Utility functions for the GeoDash package.

This module provides utility functions used across the GeoDash package, 
including JSON formatting, output formatting, and other helper functions.
"""

import json
import sys
import traceback
from typing import Any, Dict, List, Optional, Union

from GeoDash.utils.logging import get_logger

# Get a logger for this module
logger = get_logger(__name__)

# GitHub repository information
GITHUB_REPO_URL = "https://github.com/cryptekbits/GeoDash-py"
GITHUB_ISSUES_URL = f"{GITHUB_REPO_URL}/issues"

def log_error_with_github_info(error: Exception, additional_context: Optional[str] = None) -> None:
    """
    Log an error and provide GitHub issue creation information.
    
    This function logs the error and provides a message with a link to create a GitHub issue,
    making it easier for users to report blocking errors.
    
    Args:
        error: The exception that occurred
        additional_context: Optional additional context about where the error occurred
        
    Example:
        >>> try:
        ...     some_risky_function()
        ... except Exception as e:
        ...     log_error_with_github_info(e, "Error occurred while processing city data")
    """
    error_traceback = traceback.format_exc()
    error_message = str(error)
    
    # Log the error with traceback
    if additional_context:
        logger.error(f"{additional_context}: {error_message}")
    else:
        logger.error(f"Error: {error_message}")
    
    # Log traceback at debug level
    logger.debug(f"Traceback: {error_traceback}")
    
    # Provide GitHub issue information to the user
    issue_title = f"Error: {error.__class__.__name__}"
    if additional_context:
        issue_title = f"{additional_context}: {issue_title}"
        
    issue_body = f"""
**Error Details**
- Error Type: {error.__class__.__name__}
- Error Message: {error_message}

**Traceback**
```
{error_traceback}
```

<!-- Please add any additional information about what you were doing when the error occurred -->
"""
    
    # URL-encode the issue title and body for the GitHub URL
    import urllib.parse
    encoded_title = urllib.parse.quote(issue_title)
    encoded_body = urllib.parse.quote(issue_body)
    
    issue_url = f"{GITHUB_ISSUES_URL}/new?title={encoded_title}&body={encoded_body}"
    
    # Print the message with the GitHub link for the user
    github_message = (
        f"\n=== BLOCKING ERROR ===\n"
        f"This error is preventing normal operation.\n"
        f"Please report this issue to the developer (Manan Ramnani - cryptekbits) by creating a GitHub issue:\n"
        f"{issue_url}\n"
        f"===========================\n"
    )
    
    # Print to stderr for better visibility
    print(github_message, file=sys.stderr)

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