from typing import TypedDict, Literal, Optional, Dict, Any, Union, List
import re
import os
from pathlib import Path

# Define valid options as literals for type checking
DatabaseType = Literal["sqlite", "postgresql"]
LoggingLevel = Literal["debug", "info", "warning", "error", "critical"]
LoggingFormat = Literal["json", "text"]
GeoDashMode = Literal["simple", "advanced"]

class SQLiteConfig(TypedDict):
    """TypedDict for SQLite configuration validation"""
    path: Optional[str]
    rtree: bool
    fts: bool

class PostgreSQLConfig(TypedDict):
    """TypedDict for PostgreSQL configuration validation"""
    host: str
    port: int
    database: str
    user: Optional[str]
    password: Optional[str]
    postgis: bool

class DatabasePoolConfig(TypedDict):
    """TypedDict for database pool configuration validation"""
    enabled: bool
    min_size: int
    max_size: int
    timeout: int

class DatabaseConfig(TypedDict):
    """TypedDict for database configuration validation"""
    type: DatabaseType
    sqlite: SQLiteConfig
    postgresql: PostgreSQLConfig
    pool: DatabasePoolConfig

class FuzzySearchConfig(TypedDict):
    """TypedDict for fuzzy search configuration validation"""
    threshold: int
    enabled: bool

class LocationAwareConfig(TypedDict):
    """TypedDict for location-aware search configuration validation"""
    enabled: bool
    distance_weight: float
    country_boost: int

class SearchCacheConfig(TypedDict):
    """TypedDict for search cache configuration validation"""
    enabled: bool
    size: int
    ttl: int

class SearchLimitsConfig(TypedDict):
    """TypedDict for search limits configuration validation"""
    default: int
    max: int

class SearchConfig(TypedDict):
    """TypedDict for search configuration validation"""
    fuzzy: FuzzySearchConfig
    location_aware: LocationAwareConfig
    cache: SearchCacheConfig
    limits: SearchLimitsConfig

class LoggingConfig(TypedDict):
    """TypedDict for logging configuration validation"""
    level: LoggingLevel
    format: LoggingFormat
    file: Optional[str]
    structured_logging: bool

class FeaturesConfig(TypedDict):
    """TypedDict for feature flags validation"""
    enable_fuzzy_search: bool
    enable_location_aware: bool
    enable_memory_caching: bool
    enable_shared_memory: bool
    enable_advanced_db: bool
    auto_fetch_data: bool

class DataConfig(TypedDict):
    """TypedDict for data configuration validation"""
    location: Optional[str]
    countries: str
    download_url: str
    batch_size: int

class ApiCorsConfig(TypedDict):
    """TypedDict for API CORS configuration validation"""
    enabled: bool
    origins: List[str]
    methods: List[str]

class ApiRateLimitConfig(TypedDict):
    """TypedDict for API rate limiting configuration validation"""
    enabled: bool
    limit: int
    window: int

class ApiConfig(TypedDict):
    """TypedDict for API configuration validation"""
    host: str
    port: int
    debug: bool
    workers: Optional[int]
    cors: ApiCorsConfig
    rate_limit: ApiRateLimitConfig

class ConfigSchema(TypedDict):
    """Root configuration schema that includes all config sections"""
    database: DatabaseConfig
    logging: LoggingConfig
    features: FeaturesConfig
    data: DataConfig
    search: SearchConfig
    api: ApiConfig
    mode: GeoDashMode

# Schema validation functions
def is_valid_database_type(db_type: str) -> bool:
    """Validate the database type against allowed values"""
    return db_type in ("sqlite", "postgresql")

def is_valid_logging_level(level: str) -> bool:
    """Validate the logging level against allowed values"""
    return level in ("debug", "info", "warning", "error", "critical")

def is_valid_logging_format(fmt: str) -> bool:
    """Validate the logging format against allowed values"""
    return fmt in ("json", "text")

def is_valid_mode(mode: str) -> bool:
    """Validate the GeoDash mode against allowed values"""
    return mode in ("simple", "advanced")

def is_valid_country_list(countries: str) -> bool:
    """
    Validate a country list string. Valid formats:
    - "ALL" (case-insensitive)
    - Comma-separated list of 2-letter ISO country codes
    """
    if countries.upper() == "ALL":
        return True
    
    # Check if it's a comma-separated list of 2-letter country codes
    country_pattern = re.compile(r'^([A-Za-z]{2},)*[A-Za-z]{2}$')
    return bool(country_pattern.match(countries))

def is_valid_url(url: str) -> bool:
    """
    Basic validation for URLs. Checks for common URL patterns.
    """
    # Simple URL validation regex
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain
        r'localhost|'  # localhost
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # or IPv4
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return bool(url_pattern.match(url))

def validate_features(features: Dict[str, Any]) -> List[str]:
    """
    Validate the feature flags configuration.
    
    Args:
        features: Feature flags configuration dictionary
        
    Returns:
        List of validation error messages
    """
    errors = []
    
    # List of all expected feature flags
    expected_features = [
        "enable_fuzzy_search",
        "enable_location_aware", 
        "enable_memory_caching",
        "enable_shared_memory",
        "enable_advanced_db",
        "auto_fetch_data"
    ]
    
    # Check that all flags are boolean values
    for feature, value in features.items():
        if feature in expected_features and not isinstance(value, bool):
            errors.append(f"Feature flag '{feature}' must be a boolean value, got {type(value).__name__}")
    
    return errors

def validate_data_config(data_config: Dict[str, Any]) -> List[str]:
    """
    Validate the data configuration section.
    
    Args:
        data_config: Data configuration dictionary
        
    Returns:
        List of validation error messages
    """
    errors = []
    
    # Validate location (if provided)
    if "location" in data_config and data_config["location"] is not None:
        if not isinstance(data_config["location"], str):
            errors.append(f"Data location must be a string or null, got {type(data_config['location']).__name__}")
    
    # Validate countries
    if "countries" in data_config:
        if not isinstance(data_config["countries"], str):
            errors.append(f"Countries must be a string, got {type(data_config['countries']).__name__}")
        elif not is_valid_country_list(data_config["countries"]):
            errors.append(f"Invalid countries format: {data_config['countries']}. Must be 'ALL' or comma-separated ISO country codes")
    
    # Validate download_url
    if "download_url" in data_config:
        if not isinstance(data_config["download_url"], str):
            errors.append(f"Download URL must be a string, got {type(data_config['download_url']).__name__}")
        elif not is_valid_url(data_config["download_url"]):
            errors.append(f"Invalid download URL: {data_config['download_url']}")
    
    # Validate batch_size
    if "batch_size" in data_config:
        if not isinstance(data_config["batch_size"], int):
            errors.append(f"Batch size must be an integer, got {type(data_config['batch_size']).__name__}")
        elif data_config["batch_size"] < 100 or data_config["batch_size"] > 50000:
            errors.append(f"Batch size must be between 100 and 50000, got {data_config['batch_size']}")
    
    return errors

def is_valid_sqlite_path(path: Optional[str]) -> bool:
    """Validate SQLite database path or None"""
    if path is None:
        return True
    
    try:
        # Check if the directory exists or can be created
        db_path = Path(path)
        parent_dir = db_path.parent
        
        # Path must be absolute or relative to current directory
        if not (db_path.is_absolute() or str(db_path).startswith("./")):
            return False
            
        # Check if parent directory exists or can be created
        return parent_dir.exists() or parent_dir.parent.exists()
    except:
        return False

def is_valid_hostname(hostname: str) -> bool:
    """
    Validate hostname according to RFC 1123.
    """
    if not hostname:
        return False
        
    # Simple hostname validation
    hostname_pattern = re.compile(
        r'^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*'
        r'([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$'
    )
    
    # Special case for localhost
    if hostname == "localhost":
        return True
        
    # IP address pattern
    ip_pattern = re.compile(
        r'^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$'
    )
    
    if ip_pattern.match(hostname):
        # Validate each octet is in range 0-255
        octets = hostname.split('.')
        for octet in octets:
            if int(octet) > 255:
                return False
        return True
        
    return bool(hostname_pattern.match(hostname))

def is_valid_port(port: int) -> bool:
    """Validate that a port number is within the allowed range."""
    return isinstance(port, int) and 1 <= port <= 65535

def validate_database_config(db_config: Dict[str, Any]) -> List[str]:
    """
    Validate the database configuration section.
    
    Args:
        db_config: Database configuration dictionary
        
    Returns:
        List of validation error messages
    """
    errors = []
    
    # Validate database type
    if "type" in db_config:
        if not is_valid_database_type(db_config["type"]):
            errors.append(f"Invalid database type: {db_config['type']}. Must be one of: sqlite, postgresql")
    
    # Validate SQLite config
    if "sqlite" in db_config and isinstance(db_config["sqlite"], dict):
        sqlite_config = db_config["sqlite"]
        
        # Validate path
        if "path" in sqlite_config and not is_valid_sqlite_path(sqlite_config["path"]):
            errors.append(f"Invalid SQLite path: {sqlite_config['path']}")
            
        # Validate rtree and fts settings
        for setting in ["rtree", "fts"]:
            if setting in sqlite_config and not isinstance(sqlite_config[setting], bool):
                errors.append(f"SQLite {setting} setting must be a boolean")
    
    # Validate PostgreSQL config
    if "postgresql" in db_config and isinstance(db_config["postgresql"], dict):
        pg_config = db_config["postgresql"]
        
        # Validate host
        if "host" in pg_config and not is_valid_hostname(pg_config["host"]):
            errors.append(f"Invalid PostgreSQL host: {pg_config['host']}")
            
        # Validate port
        if "port" in pg_config and not is_valid_port(pg_config["port"]):
            errors.append(f"Invalid PostgreSQL port: {pg_config['port']}. Must be between 1 and 65535")
            
        # Validate database name
        if "database" in pg_config and not isinstance(pg_config["database"], str):
            errors.append("PostgreSQL database name must be a string")
            
        # Validate postgis setting
        if "postgis" in pg_config and not isinstance(pg_config["postgis"], bool):
            errors.append("PostgreSQL postgis setting must be a boolean")
    
    # Validate pool config
    if "pool" in db_config and isinstance(db_config["pool"], dict):
        pool_config = db_config["pool"]
        
        # Validate enabled setting
        if "enabled" in pool_config and not isinstance(pool_config["enabled"], bool):
            errors.append("Pool enabled setting must be a boolean")
            
        # Validate min_size
        if "min_size" in pool_config:
            if not isinstance(pool_config["min_size"], int):
                errors.append("Pool min_size must be an integer")
            elif pool_config["min_size"] < 1:
                errors.append(f"Pool min_size must be at least 1, got {pool_config['min_size']}")
                
        # Validate max_size
        if "max_size" in pool_config and "min_size" in pool_config:
            if not isinstance(pool_config["max_size"], int):
                errors.append("Pool max_size must be an integer")
            elif pool_config["max_size"] < pool_config["min_size"]:
                errors.append(f"Pool max_size must be at least min_size ({pool_config['min_size']}), got {pool_config['max_size']}")
                
        # Validate timeout
        if "timeout" in pool_config:
            if not isinstance(pool_config["timeout"], int):
                errors.append("Pool timeout must be an integer")
            elif pool_config["timeout"] < 1:
                errors.append(f"Pool timeout must be at least 1 second, got {pool_config['timeout']}")
    
    return errors

def validate_search_config(search_config: Dict[str, Any]) -> List[str]:
    """
    Validate the search configuration section.
    
    Args:
        search_config: Search configuration dictionary
        
    Returns:
        List of validation error messages
    """
    errors = []
    
    # Validate fuzzy search settings
    if "fuzzy" in search_config and isinstance(search_config["fuzzy"], dict):
        fuzzy_config = search_config["fuzzy"]
        
        # Validate threshold
        if "threshold" in fuzzy_config:
            if not isinstance(fuzzy_config["threshold"], int):
                errors.append("Fuzzy threshold must be an integer")
            elif fuzzy_config["threshold"] < 0 or fuzzy_config["threshold"] > 100:
                errors.append(f"Fuzzy threshold must be between 0 and 100, got {fuzzy_config['threshold']}")
        
        # Validate enabled flag
        if "enabled" in fuzzy_config and not isinstance(fuzzy_config["enabled"], bool):
            errors.append("Fuzzy search enabled flag must be a boolean")
    
    # Validate location-aware settings
    if "location_aware" in search_config and isinstance(search_config["location_aware"], dict):
        location_config = search_config["location_aware"]
        
        # Validate enabled flag
        if "enabled" in location_config and not isinstance(location_config["enabled"], bool):
            errors.append("Location-aware search enabled flag must be a boolean")
        
        # Validate distance weight
        if "distance_weight" in location_config:
            if not isinstance(location_config["distance_weight"], (int, float)):
                errors.append("Distance weight must be a number")
            elif location_config["distance_weight"] < 0 or location_config["distance_weight"] > 1:
                errors.append(f"Distance weight must be between 0 and 1, got {location_config['distance_weight']}")
        
        # Validate country boost
        if "country_boost" in location_config and not isinstance(location_config["country_boost"], int):
            errors.append("Country boost must be an integer")
    
    # Validate cache settings
    if "cache" in search_config and isinstance(search_config["cache"], dict):
        cache_config = search_config["cache"]
        
        # Validate enabled flag
        if "enabled" in cache_config and not isinstance(cache_config["enabled"], bool):
            errors.append("Cache enabled flag must be a boolean")
        
        # Validate size
        if "size" in cache_config:
            if not isinstance(cache_config["size"], int):
                errors.append("Cache size must be an integer")
            elif cache_config["size"] < 100 or cache_config["size"] > 50000:
                errors.append(f"Cache size must be between 100 and 50000, got {cache_config['size']}")
        
        # Validate TTL
        if "ttl" in cache_config:
            if not isinstance(cache_config["ttl"], int):
                errors.append("Cache TTL must be an integer")
            elif cache_config["ttl"] < 60 or cache_config["ttl"] > 86400:
                errors.append(f"Cache TTL must be between 60 and 86400 seconds (1 day), got {cache_config['ttl']}")
    
    # Validate limits settings
    if "limits" in search_config and isinstance(search_config["limits"], dict):
        limits_config = search_config["limits"]
        
        # Validate default limit
        if "default" in limits_config:
            if not isinstance(limits_config["default"], int):
                errors.append("Default limit must be an integer")
            elif limits_config["default"] < 1:
                errors.append(f"Default limit must be at least 1, got {limits_config['default']}")
        
        # Validate max limit
        if "max" in limits_config:
            if not isinstance(limits_config["max"], int):
                errors.append("Maximum limit must be an integer")
            elif limits_config["max"] < 1:
                errors.append(f"Maximum limit must be at least 1, got {limits_config['max']}")
        
        # Check that default <= max
        if "default" in limits_config and "max" in limits_config:
            if limits_config["default"] > limits_config["max"]:
                errors.append(f"Default limit ({limits_config['default']}) cannot exceed maximum limit ({limits_config['max']})")
    
    return errors

def validate_api_config(api_config: Dict[str, Any]) -> List[str]:
    """
    Validate API configuration.
    
    Args:
        api_config: The API configuration dictionary
        
    Returns:
        List of error messages for invalid configurations
    """
    errors = []
    
    # Validate host
    if "host" not in api_config:
        errors.append("API host is required")
    elif not isinstance(api_config["host"], str):
        errors.append("API host must be a string")
        
    # Validate port
    if "port" not in api_config:
        errors.append("API port is required")
    elif not is_valid_port(api_config.get("port", 0)):
        errors.append("API port must be an integer between 1 and 65535")
        
    # Validate debug
    if "debug" not in api_config:
        errors.append("API debug setting is required")
    elif not isinstance(api_config["debug"], bool):
        errors.append("API debug setting must be a boolean")
        
    # Validate workers
    if "workers" in api_config and api_config["workers"] is not None:
        if not isinstance(api_config["workers"], int) or api_config["workers"] <= 0:
            errors.append("API workers must be null or a positive integer")
            
    # Validate CORS configuration
    if "cors" not in api_config:
        errors.append("API CORS configuration is required")
    else:
        cors_config = api_config["cors"]
        
        if "enabled" not in cors_config:
            errors.append("CORS enabled setting is required")
        elif not isinstance(cors_config["enabled"], bool):
            errors.append("CORS enabled setting must be a boolean")
            
        if "origins" not in cors_config:
            errors.append("CORS origins list is required")
        elif not isinstance(cors_config["origins"], list):
            errors.append("CORS origins must be a list of strings")
        else:
            for origin in cors_config["origins"]:
                if not isinstance(origin, str):
                    errors.append("All CORS origins must be strings")
                    break
                    
        if "methods" not in cors_config:
            errors.append("CORS methods list is required")
        elif not isinstance(cors_config["methods"], list):
            errors.append("CORS methods must be a list of strings")
        else:
            for method in cors_config["methods"]:
                if not isinstance(method, str):
                    errors.append("All CORS methods must be strings")
                    break
                if not is_valid_http_method(method):
                    errors.append(f"Invalid HTTP method in CORS methods: {method}")
                    break
                    
    # Validate rate limit configuration
    if "rate_limit" not in api_config:
        errors.append("API rate limit configuration is required")
    else:
        rate_limit_config = api_config["rate_limit"]
        
        if "enabled" not in rate_limit_config:
            errors.append("Rate limit enabled setting is required")
        elif not isinstance(rate_limit_config["enabled"], bool):
            errors.append("Rate limit enabled setting must be a boolean")
            
        if "limit" not in rate_limit_config:
            errors.append("Rate limit value is required")
        elif not isinstance(rate_limit_config["limit"], int) or rate_limit_config["limit"] <= 0:
            errors.append("Rate limit must be a positive integer")
            
        if "window" not in rate_limit_config:
            errors.append("Rate limit window is required")
        elif not isinstance(rate_limit_config["window"], int) or rate_limit_config["window"] <= 0:
            errors.append("Rate limit window must be a positive integer")
            
    return errors

def is_valid_http_method(method: str) -> bool:
    """Validate that a string is a valid HTTP method."""
    valid_methods = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]
    return method in valid_methods

def validate_config(config: Dict[str, Any]) -> Dict[str, list]:
    """
    Validate the complete configuration structure.
    
    Args:
        config: The configuration dictionary to validate
        
    Returns:
        Dictionary mapping sections to lists of error messages
    """
    errors = {}
    
    # Check database configuration
    if "database" in config:
        db_errors = validate_database_config(config["database"])
        if db_errors:
            errors["database"] = db_errors
    else:
        errors["database"] = ["Database configuration is missing"]
        
    # Check logging configuration
    if "logging" in config:
        logging_errors = validate_logging_config(config["logging"])
        if logging_errors:
            errors["logging"] = logging_errors
    else:
        errors["logging"] = ["Logging configuration is missing"]
        
    # Check features configuration
    if "features" in config:
        features_errors = validate_features(config["features"])
        if features_errors:
            errors["features"] = features_errors
    else:
        errors["features"] = ["Features configuration is missing"]
        
    # Check data configuration
    if "data" in config:
        data_errors = validate_data_config(config["data"])
        if data_errors:
            errors["data"] = data_errors
    else:
        errors["data"] = ["Data configuration is missing"]
        
    # Check search configuration
    if "search" in config:
        search_errors = validate_search_config(config["search"])
        if search_errors:
            errors["search"] = search_errors
    else:
        errors["search"] = ["Search configuration is missing"]
        
    # Check API configuration
    if "api" in config:
        api_errors = validate_api_config(config["api"])
        if api_errors:
            errors["api"] = api_errors
    else:
        errors["api"] = ["API configuration is missing"]
        
    # Check mode setting
    if "mode" not in config:
        errors["mode"] = ["Mode setting is missing"]
    elif not is_valid_mode(config["mode"]):
        errors["mode"] = [f"Invalid mode: {config['mode']}. Must be one of: simple, advanced"]
        
    return errors 