# GeoDash Configuration Guide

This document provides a detailed explanation of GeoDash's configuration system, including all available options, their default values, and usage examples.

## Configuration Modes

GeoDash supports two operation modes:

- **Simple Mode**: Optimized for lower-end systems by disabling resource-intensive features.
- **Advanced Mode**: Enables all features for maximum functionality (default).

You can set the mode in your configuration file:

```yaml
mode: "advanced"  # or "simple"
```

### Simple vs Advanced Mode

When "simple" mode is enabled, the following features are automatically disabled regardless of their individual settings:

- Fuzzy search
- Shared memory
- Advanced database features

This provides better performance on systems with limited resources while maintaining core functionality.

## Configuration File Locations

GeoDash looks for configuration files in the following locations (in order of priority):

1. Current working directory: `./geodash.yml`
2. User's home directory: `~/.geodash/geodash.yml`
3. GeoDash package directory: `[package_path]/data/geodash.yml`

You can also specify a custom configuration file path when initializing GeoDash:

```python
from GeoDash import CityData
city_data = CityData(config_path='/path/to/your/config.yml')
```

## Configuration Format

GeoDash uses YAML or JSON format for configuration files. YAML is recommended for its readability and comment support.

## Configuration Sections

### Feature Flags

Control which features are enabled or disabled.

```yaml
features:
  enable_fuzzy_search: true
  enable_location_aware: true
  enable_memory_caching: true
  enable_shared_memory: true
  enable_advanced_db: true
  auto_fetch_data: true
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enable_fuzzy_search` | boolean | `true` | Enable fuzzy search for city names (improves search but increases CPU usage). Automatically disabled in simple mode. |
| `enable_location_aware` | boolean | `true` | Enable location-aware features for geolocation-based queries. |
| `enable_memory_caching` | boolean | `true` | Enable in-memory caching for faster repeated queries. |
| `enable_shared_memory` | boolean | `true` | Enable shared memory for inter-process communication. Automatically disabled in simple mode. |
| `enable_advanced_db` | boolean | `true` | Enable advanced database features (indexes, full-text search). Automatically disabled in simple mode. |
| `auto_fetch_data` | boolean | `true` | Automatically fetch missing data when needed. |

### Data Configuration

Configure how GeoDash handles city data.

```yaml
data:
  location: "/path/to/cities.csv"
  countries: "US,CA,MX"
  download_url: "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/csv/cities.csv"
  batch_size: 5000
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `location` | string | `null` | Path to cities.csv file or directory where it should be downloaded. If not specified, the data will be downloaded to the default location. |
| `countries` | string | `"ALL"` | Countries to include: "ALL" or comma-separated list of ISO country codes. |
| `download_url` | string | `"https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/csv/cities.csv"` | URL to download city data from. |
| `batch_size` | integer | `5000` | Batch size for importing city data. Larger values use more memory but import faster. Must be between 100 and 50000. |

### Database Configuration

Configure the database backend.

```yaml
database:
  type: sqlite
  sqlite:
    path: "/path/to/geodash.db"
    rtree: true
    fts: true
  postgresql:
    host: "localhost"
    port: 5432
    database: "geodash"
    user: "postgres"
    password: "your_password"
    postgis: true
  pool:
    enabled: true
    min_size: 2
    max_size: 10
    timeout: 30
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `type` | string | `"sqlite"` | Database type: 'sqlite' or 'postgresql'. |

#### SQLite Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `path` | string | `null` | Path to SQLite database file (null means default location). |
| `rtree` | boolean | `true` | Enable R-Tree spatial index for location queries. |
| `fts` | boolean | `true` | Enable FTS (Full-Text Search) for text search. |

#### PostgreSQL Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `host` | string | `"localhost"` | Host for PostgreSQL connection. |
| `port` | integer | `5432` | Port for PostgreSQL connection. |
| `database` | string | `"geodash"` | Database name for PostgreSQL connection. |
| `user` | string | `null` | User for PostgreSQL connection (null means use system user). |
| `password` | string | `null` | Password for PostgreSQL connection (null means use system auth). |
| `postgis` | boolean | `true` | Enable PostGIS extension for spatial operations. |

#### Connection Pool Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | `true` | Enable connection pooling. |
| `min_size` | integer | `2` | Minimum number of connections in the pool. |
| `max_size` | integer | `10` | Maximum number of connections in the pool. |
| `timeout` | integer | `30` | Connection timeout in seconds. |

### Search Configuration

Configure search behavior.

```yaml
search:
  fuzzy:
    threshold: 70
    enabled: true
  location_aware:
    enabled: true
    distance_weight: 0.3
    country_boost: 25000
  cache:
    enabled: true
    size: 5000
    ttl: 3600
  limits:
    default: 10
    max: 100
```

#### Fuzzy Search Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `threshold` | integer | `70` | Fuzzy matching threshold (0-100). Higher values require closer matches. 100: Exact matches only, 70: Recommended default for city names, Below 50: May generate too many false positives. |
| `enabled` | boolean | `true` | Whether to enable fuzzy matching. |

#### Location-Aware Search Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | `true` | Whether to enable location-aware search. |
| `distance_weight` | float | `0.3` | Weight for distance in result sorting (0-1). Higher values give more importance to closer locations. 0: Distance doesn't affect results, 0.3: Balanced default, 1: Distance is the primary factor. |
| `country_boost` | integer | `25000` | Boost value for matches in user's country. Higher values favor cities in the same country. |

#### Search Cache Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | `true` | Whether to enable search caching. |
| `size` | integer | `5000` | Maximum number of entries in the cache. Higher values use more memory but improve performance for repeated searches. |
| `ttl` | integer | `3600` | Cache time-to-live in seconds. How long search results stay valid before being recalculated. |

#### Search Limits Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `default` | integer | `10` | Default number of results to return. |
| `max` | integer | `100` | Maximum allowed number of results. |

### Logging Configuration

Configure logging behavior.

```yaml
logging:
  level: info
  format: json
  file: /var/log/geodash.log
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `level` | string | `"info"` | Logging level: 'debug', 'info', 'warning', 'error', 'critical'. Set to 'debug' for development environments. Set to 'info' or 'warning' for production environments. |
| `format` | string | `"json"` | Logging format: 'json', 'text'. JSON format is recommended for production environments. Text format is more readable for development. |
| `file` | string | `null` | Optional log file path. If not specified, logs will be sent to stdout. |

### API Configuration

Configure the API server.

```yaml
api:
  host: "0.0.0.0"
  port: 5000
  debug: false
  workers: 4
  cors:
    enabled: true
    origins: ["*"]
    methods: ["GET"]
  rate_limit:
    enabled: false
    limit: 100
    window: 60
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `host` | string | `"0.0.0.0"` | Host to bind the server to. |
| `port` | integer | `5000` | Port to run the server on. |
| `debug` | boolean | `false` | Enable debug mode for development. |
| `workers` | integer | `null` | Number of worker processes. If null, uses CPU count. |

#### CORS Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | `true` | Enable CORS support. |
| `origins` | list of strings | `["*"]` | List of allowed origins. |
| `methods` | list of strings | `["GET"]` | List of allowed HTTP methods. |

#### Rate Limiting Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable rate limiting. |
| `limit` | integer | `100` | Number of requests allowed. |
| `window` | integer | `60` | Time window in seconds for the limit. |

## Configuration Examples

### Simple Configuration

Minimal configuration for basic usage:

```yaml
mode: "simple"
features:
  enable_fuzzy_search: false
  enable_shared_memory: false
database:
  type: sqlite
  sqlite:
    path: "/path/to/geodash.db"
logging:
  level: "info"
  format: "text"
```

### Production Configuration

Comprehensive configuration for production use:

```yaml
mode: "advanced"
features:
  enable_fuzzy_search: true
  enable_location_aware: true
  enable_memory_caching: true
  enable_shared_memory: true
  enable_advanced_db: true
database:
  type: postgresql
  postgresql:
    host: "db.example.com"
    port: 5432
    database: "geodash_prod"
    user: "geodash_user"
    password: "secure_password"
    postgis: true
  pool:
    enabled: true
    min_size: 5
    max_size: 20
    timeout: 30
search:
  fuzzy:
    threshold: 75
    enabled: true
  cache:
    enabled: true
    size: 10000
    ttl: 7200
logging:
  level: "warning"
  format: "json"
  file: "/var/log/geodash.log"
api:
  host: "0.0.0.0"
  port: 8080
  workers: 8
  cors:
    enabled: true
    origins: ["https://example.com"]
    methods: ["GET"]
  rate_limit:
    enabled: true
    limit: 200
    window: 60
```

## Using Configuration in Code

```python
from GeoDash import CityData

# Using default configuration
cities = CityData()

# Using a custom configuration file
cities = CityData(config_path='/path/to/your/config.yml')

# Overriding configuration options programmatically
cities = CityData(config_overrides={
    'database': {
        'type': 'postgresql',
        'postgresql': {
            'host': 'localhost',
            'database': 'geodash_test'
        }
    }
})
```

## Environment Variables

GeoDash also supports configuration via environment variables, which take precedence over file-based configuration. The environment variable format is `GEODASH_SECTION_KEY` (uppercase with underscores).

Examples:

- `GEODASH_DATABASE_TYPE=postgresql` sets the database type
- `GEODASH_API_PORT=8080` sets the API port
- `GEODASH_FEATURES_ENABLE_FUZZY_SEARCH=false` disables fuzzy search

## Configuration Validation

GeoDash validates all configuration settings at startup. If invalid settings are found, GeoDash will log warnings and use default values as fallbacks. 