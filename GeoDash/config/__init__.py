"""
GeoDash Configuration System.

This package provides a centralized configuration system for GeoDash with
support for hierarchical keys, deep merging, and validation.

Usage:
    from GeoDash.config import get_config
    
    # Get a configuration value
    db_type = get_config().get("database.type")
    
    # Set a configuration value
    get_config().set("logging.level", "debug")
    
    # Load configuration from standard locations
    get_config().load_config()
"""

from GeoDash.config.manager import ConfigManager, get_config
from GeoDash.config.schema import validate_config
from GeoDash.config.utils import deep_merge

__all__ = ["ConfigManager", "get_config", "validate_config", "deep_merge"] 