# GeoDash Feature Flags Reference

This document provides detailed information about all feature flags available in GeoDash, including their purpose, implications when enabled or disabled, and guidance on when to use each flag.

## Overview

Feature flags allow you to enable or disable specific GeoDash functionality. They can be configured in the `features` section of your configuration file:

```yaml
features:
  enable_fuzzy_search: true
  enable_location_aware: true
  enable_memory_caching: true
  enable_shared_memory: true
  enable_advanced_db: true
  auto_fetch_data: true
```

You can also toggle features dynamically in code:

```python
from GeoDash import get_config

config = get_config()
# Enable a feature
config.enable_feature("enable_fuzzy_search")
# Disable a feature
config.disable_feature("enable_shared_memory")
# Check if a feature is enabled
if config.is_feature_enabled("enable_location_aware"):
    # Do something
```

## Available Feature Flags

### `enable_fuzzy_search`

**Purpose**: Enables fuzzy matching for city name searches.

**When enabled**:
- Searches will match city names that are similar but not exact matches
- Misspelled queries will still return relevant results
- Search operation will be more CPU-intensive

**When disabled**:
- Only exact or prefix matches will be returned
- Misspelled queries may return no results
- Search operation will be faster and use less CPU

**Usage guidance**:
- Enable for user-facing applications where query inputs may contain typos
- Disable on low-resource systems or when exact matching is preferable
- **Automatically disabled** in "simple" mode

### `enable_location_aware`

**Purpose**: Enables location-aware search functionality.

**When enabled**:
- Searches can be performed with a location context (coordinates or country)
- Results will be sorted by relevance to the provided location
- Cities in the same country or nearby will be prioritized

**When disabled**:
- Searches will be performed without location context
- Results will be sorted by population or exact match quality only

**Usage guidance**:
- Enable when building applications that have access to user location
- Enable for travel or weather applications where location context matters
- Can be enabled in both "simple" and "advanced" modes

### `enable_memory_caching`

**Purpose**: Enables in-memory caching of search results.

**When enabled**:
- Recent search results will be cached in memory
- Repeated identical searches will be significantly faster
- Memory usage will increase based on the cache size setting

**When disabled**:
- Every search will query the database
- Memory usage will be lower
- Performance may be slower for repeated searches

**Usage guidance**:
- Enable for applications with repetitive search patterns
- Disable if memory is limited or if search patterns are highly variable
- Can be enabled in both "simple" and "advanced" modes

### `enable_shared_memory`

**Purpose**: Enables shared memory for inter-process communication.

**When enabled**:
- Multiple GeoDash instances can share cached data
- Useful for multi-process deployments (e.g., with Gunicorn)
- Higher memory efficiency when running multiple workers

**When disabled**:
- Each GeoDash instance will maintain its own separate cache
- No inter-process communication overhead
- Higher total memory usage across multiple workers

**Usage guidance**:
- Enable in production environments with multiple worker processes
- Disable for single-process applications or development environments
- **Automatically disabled** in "simple" mode

### `enable_advanced_db`

**Purpose**: Enables advanced database features like indexes and full-text search.

**When enabled**:
- Creates and uses database indexes for faster queries
- Enables full-text search capabilities (SQLite FTS or PostgreSQL text search)
- Improves query performance but increases database size

**When disabled**:
- Uses basic queries without specialized indexes
- Database initialization is faster
- Database size is smaller

**Usage guidance**:
- Enable for production environments where query performance is important
- Disable for testing or when database size is a concern
- **Automatically disabled** in "simple" mode

### `auto_fetch_data`

**Purpose**: Controls automatic downloading of city data when needed.

**When enabled**:
- GeoDash will automatically download city data if not available
- No manual data preparation required
- Application will work out-of-the-box

**When disabled**:
- GeoDash will raise an error if city data is not available
- Useful for environments without internet access
- Allows for manual data preparation

**Usage guidance**:
- Enable for development environments or when convenience is important
- Disable in production or air-gapped environments where data should be prepared in advance
- Can be enabled in both "simple" and "advanced" modes

## Feature Flags and Modes

The "simple" mode automatically disables certain feature flags regardless of their individual settings:

| Feature Flag | Status in "simple" mode |
|--------------|-------------------------|
| `enable_fuzzy_search` | Always disabled |
| `enable_shared_memory` | Always disabled |
| `enable_advanced_db` | Always disabled |
| `enable_location_aware` | Configurable |
| `enable_memory_caching` | Configurable |
| `auto_fetch_data` | Configurable |

## Recommended Feature Flag Combinations

### Low-Resource Environments

```yaml
mode: "simple"
features:
  enable_location_aware: false
  enable_memory_caching: true
  auto_fetch_data: false
```

### Development Environment

```yaml
mode: "advanced"
features:
  enable_fuzzy_search: true
  enable_location_aware: true
  enable_memory_caching: true
  enable_shared_memory: false
  enable_advanced_db: true
  auto_fetch_data: true
```

### Production API Server

```yaml
mode: "advanced"
features:
  enable_fuzzy_search: true
  enable_location_aware: true
  enable_memory_caching: true
  enable_shared_memory: true
  enable_advanced_db: true
  auto_fetch_data: false
```

## Performance Implications

Feature flags have varying impacts on performance:

| Feature Flag | CPU Impact | Memory Impact | Database Size Impact |
|--------------|------------|---------------|----------------------|
| `enable_fuzzy_search` | High | Low | None |
| `enable_location_aware` | Medium | Low | None |
| `enable_memory_caching` | Low | High | None |
| `enable_shared_memory` | Medium | Medium | None |
| `enable_advanced_db` | Low | Low | High |
| `auto_fetch_data` | N/A | N/A | N/A |

## Feature Flags API

The GeoDash configuration manager provides methods for working with feature flags programmatically:

```python
from GeoDash import get_config

config = get_config()

# Check if a feature is enabled
if config.is_feature_enabled("enable_fuzzy_search"):
    # Feature-specific code

# Enable a feature
config.enable_feature("enable_memory_caching")

# Disable a feature
config.disable_feature("enable_fuzzy_search")

# Apply a specific mode (affects multiple features)
config.set_mode("simple")
``` 