"""
Utility functions for the GeoDash configuration system.

This module provides helper functions used by the configuration system, 
including deep dictionary merging and other utilities.
"""

from typing import Any, Dict, List, Union


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively merge two dictionaries, with override values taking precedence.
    
    Rules:
    - If both values are dictionaries, recursively merge them
    - If the value is a list, replace it completely (no merging)
    - Otherwise, override the base value with the override value
    
    Args:
        base: Base dictionary
        override: Dictionary with values to override
        
    Returns:
        New dictionary with merged values
    """
    result = base.copy()
    
    for key, value in override.items():
        # If both values are dictionaries, recursively merge them
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            # For lists and other types, replace the value completely
            result[key] = value
            
    return result 