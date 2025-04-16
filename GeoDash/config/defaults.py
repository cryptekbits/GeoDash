"""
Default configuration values for GeoDash.

This module defines the default configuration settings used when no custom
configuration is provided. These values serve as fallbacks and define the
base configuration structure.

Each section contains the default values for a specific part of the GeoDash
configuration. The complete configuration is assembled in the DEFAULT_CONFIG
dictionary.

Default configuration values can be overridden by:
1. Configuration files (geodash.yml)
2. Environment variables (GEODASH_SECTION_KEY)
3. Programmatic configuration via the ConfigManager
"""

from typing import Dict, Any

# Default database configuration
DATABASE_DEFAULTS: Dict[str, Any] = {
    # Database backend: 'sqlite' (default) or 'postgresql'
    "type": "sqlite",
    # SQLite-specific configuration
    "sqlite": {
        # Path to the SQLite database file (null = auto-generated path)
        "path": None,
        # Enable R-Tree spatial index for location queries
        "rtree": True,
        # Enable FTS (Full-Text Search) for text search
        "fts": True
    },
    # PostgreSQL-specific configuration
    "postgresql": {
        # Host for PostgreSQL connection
        "host": "localhost",
        # Port for PostgreSQL connection
        "port": 5432,
        # Database name for PostgreSQL connection
        "database": "geodash",
        # User for PostgreSQL connection (null = use system user)
        "user": None,
        # Password for PostgreSQL connection (null = use system auth)
        "password": None,
        # Enable PostGIS extension for spatial operations
        "postgis": True
    },
    # Connection pool configuration
    "pool": {
        # Enable connection pooling
        "enabled": True,
        # Minimum number of connections in the pool
        "min_size": 2,
        # Maximum number of connections in the pool
        "max_size": 10,
        # Connection timeout in seconds
        "timeout": 30
    }
}

# Default search configuration
SEARCH_DEFAULTS: Dict[str, Any] = {
    # Fuzzy search configuration
    "fuzzy": {
        # Fuzzy matching threshold (0-100, higher values require closer matches)
        "threshold": 70,
        # Enable fuzzy matching
        "enabled": True
    },
    # Location-aware search configuration
    "location_aware": {
        # Enable location-aware search
        "enabled": True,
        # Weight for distance in result sorting (0-1)
        "distance_weight": 0.3,
        # Boost value for matches in user's country
        "country_boost": 25000
    },
    # Search cache configuration
    "cache": {
        # Enable search caching
        "enabled": True,
        # Maximum number of entries in the cache
        "size": 5000,
        # Cache time-to-live in seconds
        "ttl": 3600
    },
    # Search result limits
    "limits": {
        # Default number of results to return
        "default": 10,
        # Maximum allowed number of results
        "max": 100
    }
}

# Default logging configuration
LOGGING_DEFAULTS: Dict[str, Any] = {
    # Logging level: 'debug', 'info', 'warning', 'error', 'critical'
    "level": "info",
    # Logging format: 'json', 'text'
    "format": "text",
    # Log file path (null = log to stdout)
    "file": None,
    # Use structured logging in JSON format
    "structured_logging": False
}

# Default feature flags
FEATURES_DEFAULTS: Dict[str, bool] = {
    # Enable fuzzy search (automatically disabled in simple mode)
    "enable_fuzzy_search": True,
    # Enable location-aware search
    "enable_location_aware": True,
    # Enable in-memory caching for search results
    "enable_memory_caching": True,
    # Enable shared memory for inter-process communication (automatically disabled in simple mode)
    "enable_shared_memory": True,
    # Enable advanced database features (automatically disabled in simple mode)
    "enable_advanced_db": True,
    # Automatically download missing data when needed
    "auto_fetch_data": True
}

# Default data configuration
DATA_DEFAULTS: Dict[str, Any] = {
    # Path to cities.csv file or download directory (null = use default location)
    "location": None,
    # Countries to include: "ALL" or comma-separated list of ISO country codes
    "countries": "ALL",
    # URL to download city data from
    "download_url": "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/csv/cities.csv",
    # Batch size for importing city data
    "batch_size": 5000
}

# Default API configuration
API_DEFAULTS: Dict[str, Any] = {
    # Host to bind the API server to
    "host": "0.0.0.0",
    # Port to run the API server on
    "port": 5000,
    # Enable debug mode for development
    "debug": False,
    # Number of worker processes (null = use CPU count)
    "workers": None,
    # CORS configuration
    "cors": {
        # Enable CORS support
        "enabled": True,
        # List of allowed origins
        "origins": ["*"],
        # List of allowed HTTP methods
        "methods": ["GET"]
    },
    # Rate limiting configuration
    "rate_limit": {
        # Enable rate limiting
        "enabled": False,
        # Number of requests allowed
        "limit": 100,
        # Time window in seconds for the limit
        "window": 60
    }
}

# Complete default configuration structure
DEFAULT_CONFIG: Dict[str, Dict[str, Any]] = {
    "database": DATABASE_DEFAULTS,
    "logging": LOGGING_DEFAULTS,
    "features": FEATURES_DEFAULTS,
    "data": DATA_DEFAULTS,
    "search": SEARCH_DEFAULTS,
    "api": API_DEFAULTS,
    "mode": "advanced"  # Default mode is advanced
} 