"""
Search configuration utilities for GeoDash.

This module provides utilities for configuring and working with
search-related settings in GeoDash, including fuzzy search,
location-aware search, and search result caching.
"""

from typing import Dict, Any, Optional, Union, List, TypeVar
from GeoDash.config.manager import get_config

T = TypeVar('T')  # Generic type for repository

def apply_search_config(repository: T) -> T:
    """
    Apply search configuration settings to a repository instance.
    
    This function loads the search configuration from the ConfigManager
    and applies the settings to the provided repository instance.
    
    Args:
        repository: Repository instance to configure
        
    Returns:
        Configured repository instance
    """
    # Get configuration manager
    config = get_config()
    
    # Apply fuzzy search settings
    fuzzy_settings = config.get_fuzzy_settings()
    if hasattr(repository, 'set_fuzzy_threshold'):
        repository.set_fuzzy_threshold(fuzzy_settings["threshold"])
    
    if hasattr(repository, 'enable_fuzzy_search'):
        if fuzzy_settings["enabled"]:
            repository.enable_fuzzy_search()
        else:
            repository.disable_fuzzy_search()
    
    # Apply location-aware search settings
    location_settings = config.get_location_settings()
    if hasattr(repository, 'set_location_settings'):
        repository.set_location_settings(
            enabled=location_settings["enabled"],
            distance_weight=location_settings["distance_weight"],
            country_boost=location_settings["country_boost"]
        )
    
    # Apply cache settings
    cache_settings = config.get_cache_settings()
    if hasattr(repository, 'set_cache_settings'):
        repository.set_cache_settings(
            enabled=cache_settings["enabled"],
            size=cache_settings["size"],
            ttl=cache_settings["ttl"]
        )
    
    # Apply search limits
    search_limits = config.get_search_limits()
    if hasattr(repository, 'set_search_limits'):
        repository.set_search_limits(
            default=search_limits["default"],
            max=search_limits["max"]
        )
    
    return repository

def get_fuzzy_threshold() -> int:
    """
    Get the configured fuzzy search threshold.
    
    Returns:
        Fuzzy search threshold value (0-100)
    """
    config = get_config()
    return config.get("search.fuzzy.threshold", 70)

def should_use_fuzzy_search() -> bool:
    """
    Check if fuzzy search should be used based on configuration.
    
    This function checks both the dedicated fuzzy search setting and
    the feature flag for backward compatibility.
    
    Returns:
        True if fuzzy search should be used, False otherwise
    """
    config = get_config()
    
    # Check the specific search configuration first
    fuzzy_enabled = config.get("search.fuzzy.enabled", True)
    
    # Also check the feature flag for backward compatibility
    feature_enabled = config.is_feature_enabled("enable_fuzzy_search")
    
    # Only use fuzzy search if both settings allow it
    return fuzzy_enabled and feature_enabled

def should_use_location_aware() -> bool:
    """
    Check if location-aware search should be used based on configuration.
    
    This function checks both the dedicated location-aware setting and
    the feature flag for backward compatibility.
    
    Returns:
        True if location-aware search should be used, False otherwise
    """
    config = get_config()
    
    # Check the specific search configuration first
    location_enabled = config.get("search.location_aware.enabled", True)
    
    # Also check the feature flag for backward compatibility
    feature_enabled = config.is_feature_enabled("enable_location_aware")
    
    # Only use location-aware search if both settings allow it
    return location_enabled and feature_enabled

def get_default_search_limit() -> int:
    """
    Get the default number of search results to return.
    
    Returns:
        Default number of search results
    """
    config = get_config()
    return config.get("search.limits.default", 10)

def get_max_search_limit() -> int:
    """
    Get the maximum allowed number of search results.
    
    Returns:
        Maximum number of search results
    """
    config = get_config()
    return config.get("search.limits.max", 100)

def get_cache_size() -> int:
    """
    Get the configured search cache size.
    
    Returns:
        Maximum number of entries in the search cache
    """
    config = get_config()
    return config.get("search.cache.size", 5000)

def get_cache_ttl() -> int:
    """
    Get the configured search cache time-to-live.
    
    Returns:
        Cache TTL in seconds
    """
    config = get_config()
    return config.get("search.cache.ttl", 3600)

def should_use_cache() -> bool:
    """
    Check if search result caching should be used.
    
    This function checks both the dedicated cache setting and
    the feature flag for backward compatibility.
    
    Returns:
        True if search caching should be used, False otherwise
    """
    config = get_config()
    
    # Check the specific search configuration
    cache_enabled = config.get("search.cache.enabled", True)
    
    # Also check the feature flag for backward compatibility
    feature_enabled = config.is_feature_enabled("enable_memory_caching")
    
    # Only use caching if both settings allow it
    return cache_enabled and feature_enabled 