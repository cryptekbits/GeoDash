"""
Configuration manager for GeoDash.

This module implements the ConfigManager class that provides a centralized 
configuration system with support for hierarchical keys, deep merging, and
loading from files.
"""

import os
import yaml
import json
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Set, Tuple, Type, cast, TypeVar, overload
import copy

from GeoDash.config.defaults import DEFAULT_CONFIG
from GeoDash.config.schema import validate_config
from GeoDash.config.utils import deep_merge
from GeoDash.utils.logging import get_logger

T = TypeVar('T')

class ConfigManager:
    """
    Configuration manager for GeoDash.
    
    Implements a singleton pattern to ensure only one configuration
    instance exists across the application.
    
    Features:
    - Hierarchical key access (e.g., "database.type")
    - Deep merging of configuration dictionaries
    - Loading from YAML or JSON files
    - Configuration validation
    - Feature flags management
    - Mode-based configuration
    
    Attributes:
        _instance (ConfigManager): The singleton instance
        _config (Dict[str, Any]): The configuration dictionary
        logger: The logger instance
        _feature_cache (Dict[str, bool]): Cache for feature flag checks
        _mode_features_applied (bool): Track if mode features have been applied
    """
    _instance = None
    
    def __new__(cls) -> 'ConfigManager':
        """
        Implement singleton pattern to ensure only one configuration manager exists.
        
        Returns:
            ConfigManager: The singleton instance of the ConfigManager.
        """
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self) -> None:
        """
        Initialize the configuration with default values.
        
        Sets up the configuration with default values from DEFAULT_CONFIG,
        initializes the logger, and sets up the feature flag cache.
        """
        self._config = copy.deepcopy(DEFAULT_CONFIG)
        self.logger = get_logger(__name__)
        self._feature_cache = {}  # Cache for feature flag checks
        self._mode_features_applied = False  # Track if mode features have been applied
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value using dot notation.
        
        Retrieves a value from the configuration dictionary using dot notation
        to access nested values. Returns the default value if the key is not found.
        
        Args:
            key (str): Hierarchical key using dot notation (e.g., "database.type")
            default (Any, optional): Default value to return if key is not found. Defaults to None.
            
        Returns:
            Any: The configuration value if found, otherwise the default value.
            
        Examples:
            >>> config = get_config()
            >>> db_type = config.get("database.type", "sqlite")
            >>> api_port = config.get("api.port", 5000)
        """
        if not key:
            return default
        
        # Split the key into parts for hierarchical access
        parts = key.split('.')
        value = self._config
        
        # Traverse the configuration dictionary
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return default
                
        return value
    
    def set(self, key: str, value: Any) -> None:
        """
        Set a configuration value using dot notation.
        
        Updates a value in the configuration dictionary using dot notation
        to access nested values. Creates intermediate dictionaries if they
        don't exist.
        
        Args:
            key (str): Hierarchical key using dot notation (e.g., "database.type")
            value (Any): Value to set
            
        Examples:
            >>> config = get_config()
            >>> config.set("database.type", "postgresql")
            >>> config.set("api.port", 8080)
        """
        if not key:
            return
            
        # Split the key into parts for hierarchical access
        parts = key.split('.')
        config = self._config
        
        # Traverse the configuration dictionary and create intermediate dictionaries
        for i, part in enumerate(parts[:-1]):
            if part not in config or not isinstance(config[part], dict):
                config[part] = {}
            config = config[part]
                
        # Set the final value
        config[parts[-1]] = value
        
        # Clear feature cache if modifying features
        if parts[0] == "features" or (parts[0] == "mode" and len(parts) == 1):
            self._clear_feature_cache()
    
    def is_feature_enabled(self, feature_name: str) -> bool:
        """
        Check if a feature flag is enabled.
        
        Checks the current value of a feature flag, taking into account the
        current mode (simple/advanced) and any overrides. Results are cached
        for better performance.
        
        Args:
            feature_name (str): Name of the feature flag to check
            
        Returns:
            bool: True if the feature is enabled, False otherwise
            
        Examples:
            >>> config = get_config()
            >>> if config.is_feature_enabled("enable_fuzzy_search"):
            ...     # Use fuzzy search
        """
        # Apply mode-based configuration if not already applied
        if not self._mode_features_applied:
            self._apply_mode_features()
        
        # Check cache first
        if feature_name in self._feature_cache:
            return self._feature_cache[feature_name]
        
        # Get feature flag value
        enabled = self.get(f"features.{feature_name}", False)
        
        # Cache the result
        self._feature_cache[feature_name] = enabled
        
        return enabled
    
    def enable_feature(self, feature_name: str) -> None:
        """
        Enable a feature flag.
        
        Enables a feature flag by setting its value to True. Clears the
        feature cache to ensure the new value is used.
        
        Args:
            feature_name (str): Name of the feature flag to enable
            
        Examples:
            >>> config = get_config()
            >>> config.enable_feature("enable_memory_caching")
        """
        self.set(f"features.{feature_name}", True)
        self._feature_cache[feature_name] = True
    
    def disable_feature(self, feature_name: str) -> None:
        """
        Disable a feature flag.
        
        Disables a feature flag by setting its value to False. Clears the
        feature cache to ensure the new value is used.
        
        Args:
            feature_name (str): Name of the feature flag to disable
            
        Examples:
            >>> config = get_config()
            >>> config.disable_feature("enable_fuzzy_search")
        """
        self.set(f"features.{feature_name}", False)
        self._feature_cache[feature_name] = False
    
    def _clear_feature_cache(self) -> None:
        """
        Clear the feature flag cache.
        
        Clears the internal cache of feature flag values to ensure that
        subsequent calls to is_feature_enabled() use the current values.
        """
        self._feature_cache = {}
        self._mode_features_applied = False
    
    def set_mode(self, mode: str) -> bool:
        """
        Set the application mode (simple/advanced).
        
        Sets the application mode and applies the appropriate feature flag
        settings for that mode. In simple mode, certain features are
        automatically disabled.
        
        Args:
            mode (str): The mode to set, either "simple" or "advanced"
            
        Returns:
            bool: True if the mode was valid and set successfully, False otherwise
            
        Examples:
            >>> config = get_config()
            >>> config.set_mode("simple")  # Disable resource-intensive features
        """
        if mode not in ("simple", "advanced"):
            self.logger.warning(f"Invalid mode: {mode}. Must be 'simple' or 'advanced'")
            return False
        
        self._config["mode"] = mode
        self._apply_mode_features()
        self.logger.info(f"Set GeoDash mode to: {mode}")
        return True
    
    def _apply_mode_features(self) -> None:
        """
        Apply feature flag settings based on the current mode.
        
        In simple mode, certain features are automatically disabled regardless
        of their individual settings. This method enforces those mode-specific
        settings.
        """
        mode = self._config.get("mode", "advanced")
        
        if mode == "simple":
            # In simple mode, disable these specific features
            features_to_disable = {
                "enable_fuzzy_search",
                "enable_shared_memory",
                "enable_advanced_db"
            }
            
            # Only modify the runtime values, preserve the original config
            for feature in features_to_disable:
                self._feature_cache[feature] = False
        
        self._mode_features_applied = True
    
    def get_data_location(self) -> Optional[str]:
        """
        Get the configured data location path.
        
        Returns the path configured for city data storage or None if not set.
        
        Returns:
            Optional[str]: The configured data location path or None
        """
        return self.get("data.location")
    
    def get_enabled_countries(self) -> Optional[List[str]]:
        """
        Get the list of enabled countries.
        
        Returns the list of countries that should be included in the database.
        Returns None (meaning all countries) if the setting is "ALL".
        
        Returns:
            Optional[List[str]]: List of country codes or None for all countries
        """
        countries = self.get("data.countries", "ALL")
        
        # If "ALL" is specified, return None to indicate no filtering
        if countries.upper() == "ALL":
            return None
            
        # Parse comma-separated list into array of uppercase codes
        return [code.strip().upper() for code in countries.split(",")]
    
    def should_auto_download(self) -> bool:
        """
        Check if automatic data downloading is enabled.
        
        Returns True if the auto_fetch_data feature flag is enabled, indicating
        that GeoDash should automatically download city data when needed.
        
        Returns:
            bool: True if auto-download is enabled, False otherwise
        """
        return self.is_feature_enabled("auto_fetch_data")
    
    def get_database_uri(self) -> str:
        """
        Get the database URI for connecting to the database.
        
        Constructs a database URI based on the current configuration.
        For SQLite, this includes the path to the database file.
        For PostgreSQL, this includes the connection parameters.
        
        Returns:
            str: The database URI for SQLAlchemy
            
        Examples:
            >>> config = get_config()
            >>> uri = config.get_database_uri()
            >>> engine = create_engine(uri)
        """
        db_type = self.get("database.type", "sqlite")
        
        if db_type == "sqlite":
            path = self.get("database.sqlite.path")
            
            # If path is not specified, use default location
            if path is None:
                # Use data directory in user's home directory
                path = str(Path.home() / ".geodash" / "data" / "geodash.db")
                
                # Ensure directory exists
                os.makedirs(os.path.dirname(path), exist_ok=True)
            
            return f"sqlite:///{path}"
            
        elif db_type == "postgresql":
            host = self.get("database.postgresql.host", "localhost")
            port = self.get("database.postgresql.port", 5432)
            database = self.get("database.postgresql.database", "geodash")
            user = self.get("database.postgresql.user")
            password = self.get("database.postgresql.password")
            
            # Build connection string
            uri = f"postgresql://"
            
            # Add credentials if provided
            if user:
                uri += user
                if password:
                    uri += f":{password}"
                uri += "@"
                
            # Add host, port, and database
            uri += f"{host}:{port}/{database}"
            
            return uri
            
        else:
            # Fallback to a default SQLite database
            self.logger.warning(f"Unsupported database type: {db_type}, falling back to SQLite")
            return "sqlite:///geodash.db"
    
    def is_pooling_enabled(self) -> bool:
        """
        Check if database connection pooling is enabled.
        
        Returns:
            bool: True if pooling is enabled, False otherwise
        """
        return self.get("database.pool.enabled", True)
    
    def get_pool_settings(self) -> Dict[str, Any]:
        """
        Get database connection pool settings.
        
        Returns a dictionary with pool configuration parameters.
        
        Returns:
            Dict[str, Any]: Dictionary with pool settings (min_size, max_size, timeout)
        """
        return {
            "min_size": self.get("database.pool.min_size", 2),
            "max_size": self.get("database.pool.max_size", 10),
            "timeout": self.get("database.pool.timeout", 30)
        }
    
    def get_fuzzy_settings(self) -> Dict[str, Any]:
        """
        Get fuzzy search settings.
        
        Returns a dictionary with fuzzy search configuration parameters.
        
        Returns:
            Dict[str, Any]: Dictionary with fuzzy search settings (threshold, enabled)
        """
        return {
            "threshold": self.get("search.fuzzy.threshold", 70),
            "enabled": self.get("search.fuzzy.enabled", True)
        }
    
    def get_location_settings(self) -> Dict[str, Any]:
        """
        Get location-aware search settings.
        
        Returns a dictionary with location-aware search configuration parameters.
        
        Returns:
            Dict[str, Any]: Dictionary with location settings (enabled, distance_weight, country_boost)
        """
        return {
            "enabled": self.get("search.location_aware.enabled", True),
            "distance_weight": self.get("search.location_aware.distance_weight", 0.3),
            "country_boost": self.get("search.location_aware.country_boost", 25000)
        }
    
    def get_cache_settings(self) -> Dict[str, Any]:
        """
        Get search cache settings.
        
        Returns a dictionary with search cache configuration parameters.
        
        Returns:
            Dict[str, Any]: Dictionary with cache settings (enabled, size, ttl)
        """
        return {
            "enabled": self.get("search.cache.enabled", True),
            "size": self.get("search.cache.size", 5000),
            "ttl": self.get("search.cache.ttl", 3600)
        }
    
    def get_search_limits(self) -> Dict[str, int]:
        """
        Get search result limit settings.
        
        Returns a dictionary with search limit configuration parameters.
        
        Returns:
            Dict[str, int]: Dictionary with search limits (default, max)
        """
        return {
            "default": self.get("search.limits.default", 10),
            "max": self.get("search.limits.max", 100)
        }
    
    def find_config_file(self) -> Optional[Path]:
        """
        Find the configuration file in standard locations.
        
        Searches for a configuration file in standard locations in the following order:
        1. Current working directory: ./geodash.yml
        2. User's home directory: ~/.geodash/geodash.yml
        3. Package directory: [package_path]/data/geodash.yml
        
        Returns:
            Optional[Path]: Path to the configuration file if found, None otherwise
        """
        # Standard locations to search for the config file
        search_locations = [
            # Current working directory
            Path.cwd() / 'geodash.yml',
            
            # User's home directory
            Path.home() / '.geodash' / 'geodash.yml',
            
            # GeoDash package directory
            Path(__file__).parent.parent / 'data' / 'geodash.yml'
        ]
        
        # Search for the config file
        for path in search_locations:
            if path.is_file():
                self.logger.debug(f"Found configuration file at: {path}")
                return path
                
        self.logger.debug("No configuration file found in standard locations")
        return None
    
    def load_config(self) -> bool:
        """
        Load configuration from the first available standard location.
        
        Searches for a configuration file in standard locations and loads the first one found.
        If no file is found, continues using the default configuration.
        
        Returns:
            bool: True if a configuration file was found and loaded, False otherwise
            
        Examples:
            >>> config = get_config()
            >>> success = config.load_config()
            >>> if success:
            ...     print("Configuration loaded from file")
            ... else:
            ...     print("Using default configuration")
        """
        config_path = self.find_config_file()
        
        if not config_path:
            self.logger.info("No configuration file found, using defaults")
            return False
            
        try:
            # Load the YAML file
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                
            # Validate the configuration
            errors = validate_config(config)
            
            if errors:
                self.logger.warning(f"Configuration validation errors: {errors}")
                return False
                
            # Merge with defaults
            self._config = deep_merge(copy.deepcopy(DEFAULT_CONFIG), config)
            
            # Clear feature cache
            self._clear_feature_cache()
            
            self.logger.info(f"Loaded configuration from {config_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load configuration file: {e}")
            return False
    
    def load_from_file(self, path: Union[str, Path]) -> Dict[str, List[str]]:
        """
        Load configuration from a specific file.
        
        Loads configuration from a YAML or JSON file at the specified path.
        Performs validation on the loaded configuration.
        
        Args:
            path (Union[str, Path]): Path to the configuration file
            
        Returns:
            Dict[str, List[str]]: Dictionary of validation errors, if any
            
        Examples:
            >>> config = get_config()
            >>> errors = config.load_from_file("/path/to/config.yml")
            >>> if errors:
            ...     print("Configuration validation errors:", errors)
        """
        path = Path(path) if isinstance(path, str) else path
        
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")
            
        # Determine file format from extension
        if path.suffix.lower() in ('.yaml', '.yml'):
            with open(path, 'r') as f:
                config = yaml.safe_load(f)
        elif path.suffix.lower() == '.json':
            with open(path, 'r') as f:
                config = json.load(f)
        else:
            raise ValueError(f"Unsupported configuration file format: {path.suffix}")
            
        # Validate the loaded configuration
        errors = validate_config(config)
        
        # Only merge if there are no validation errors
        if not errors:
            self._config = deep_merge(self._config, config)
            self._clear_feature_cache()
            
        return errors
    
    def get_all(self) -> Dict[str, Any]:
        """
        Get the entire configuration dictionary.
        
        Returns a copy of the entire configuration dictionary.
        
        Returns:
            Dict[str, Any]: The entire configuration dictionary
            
        Examples:
            >>> config = get_config()
            >>> full_config = config.get_all()
            >>> print(json.dumps(full_config, indent=2))
        """
        return copy.deepcopy(self._config)


# Global function to get the config manager instance
def get_config() -> ConfigManager:
    """
    Get the singleton ConfigManager instance.
    
    Returns the singleton instance of the ConfigManager, creating it
    if it doesn't already exist.
    
    Returns:
        ConfigManager: The singleton ConfigManager instance
        
    Examples:
        >>> from GeoDash.config import get_config
        >>> config = get_config()
        >>> db_type = config.get("database.type")
    """
    return ConfigManager() 