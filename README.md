<div align="center">
  <img src="resources/banner.png" alt="GeoDash Banner" width="100%">
</div>

# GeoDash

A Python module for managing city data with fast coordinate queries and autocomplete functionality.

## Installation

```bash
# Install from PyPI
pip install GeoDash
```

### Installing from Git

```bash
# Clone the repository
git clone https://github.com/cryptekbits/GeoDash-py.git
cd GeoDash-py

# Install directly
pip install .

# OR install in development mode
pip install -e .
```

### Installing from Source Directory

If you already have the source code directory (e.g., downloaded as a ZIP and extracted):

```bash
# Navigate to the GeoDash directory
cd /path/to/GeoDash-py

# Install the package
pip install .

# OR install in development mode (for development)
pip install -e .
```

When installing from source, the city data CSV file will be automatically downloaded. If you're in an environment without internet access, you can manually place the `cities.csv` file in `GeoDash-py/data/` before installation.

## Features

- Fast queries for coordinates
- Smart autocomplete that works with just a few characters (3+)
- Prefix-based search for better UI autocomplete experience
- Population-based sorting for more relevant results
- Location-aware search that prioritizes results based on user's coordinates or country
- Support for both SQLite and PostgreSQL databases
- Comprehensive configuration system with YAML/JSON support
- Feature flags for fine-grained control over functionality
- Simple and Advanced operation modes for different resource requirements
- Command-line interface with configuration management
- API server mode with CORS and rate limiting support
- Structured logging with JSON format option

## Usage

### Direct Import

```python
# Import from the GeoDash package
from GeoDash import CityData

# Create a CityData instance with a SQLite database
city_data = CityData()

# Or with a specific database URI
city_data = CityData(db_uri='sqlite:///path/to/db.sqlite')
city_data = CityData(db_uri='postgresql://user:password@localhost:5432/dbname')

# Search for cities with autocomplete support (works with just a few characters)
cities = city_data.search_cities('New')  # Will find "New York", "New Delhi", etc.

# Location-aware search (prioritizes results near user's location)
cities = city_data.search_cities('San', user_lat=37.7749, user_lng=-122.4194)  # Prioritizes "San Francisco"
cities = city_data.search_cities('New', user_country='United States')  # Prioritizes US cities like "New York"

# Get cities near coordinates
nearby_cities = city_data.get_cities_by_coordinates(40.7128, -74.0060, radius_km=10)

# Get a city by ID
city = city_data.get_city_by_id(1234)

# Get a list of countries
countries = city_data.get_countries()

# Get states in a country
states = city_data.get_states('United States')

# Get cities in a state
cities = city_data.get_cities_in_state('California', 'United States')

# Close the connection when done
city_data.close()
```

### UI Autocomplete Integration

The module is designed to work seamlessly with UI autocomplete components:

```javascript
// Example of how to integrate with a frontend framework
async function handleCityInputChange(value) {
  if (value.length >= 3) {
    // Start searching after 3 characters are typed
    const response = await fetch(`/api/cities/search?query=${value}&limit=10`);
    const cities = await response.json();

    // Display suggestions to the user
    updateSuggestions(cities);
  }
}
```

### As a Command-Line Tool

```bash
# Use the installed console script
GeoDash search "New York" --limit 5 --country "United States"

# Get a city by ID
GeoDash city 1234

# Get cities near coordinates
GeoDash coordinates 40.7128 -74.0060 --radius 10

# Get a list of countries
GeoDash countries

# Get states in a country
GeoDash states "United States"

# Get cities in a state
GeoDash cities-in-state "California" "United States"

# Start the API server
GeoDash server --host 0.0.0.0 --port 5000 --debug

# Import city data from a CSV file
GeoDash import --csv-path /path/to/cities.csv

# Configuration management
GeoDash config show                      # Show current configuration
GeoDash config show --section database   # Show only database configuration
GeoDash config show --format json        # Show configuration in JSON format
GeoDash config init --output ~/my-config.yml  # Create a template configuration file
GeoDash config validate ~/my-config.yml  # Validate a configuration file
```

#### Log Level Control

All CLI commands support the `--log-level` option to control verbosity:

```bash
# Run any command with increased verbosity
GeoDash search "New York" --log-level debug

# Run with minimal logging
GeoDash server --log-level error
```

### As an API Server

```bash
# Start the API server using the console script
GeoDash server --host 0.0.0.0 --port 5000 --debug

# Or use configuration-driven settings (no need to specify parameters)
GeoDash server
```

The server can be configured through the configuration file:

```yaml
# API configuration in geodash.yml
api:
  host: "0.0.0.0"
  port: 5000
  debug: false
  workers: 4  # Number of worker processes
  cors:
    enabled: true
    origins: ["*"]
    methods: ["GET"]
  rate_limit:
    enabled: true
    limit: 100
    window: 60  # seconds
```

#### API Endpoints

- `GET /api/city/<city_id>` - Get a city by ID
- `GET /api/cities/search?query=<query>&limit=<limit>&country=<country>` - Search for cities with autocomplete support (works with just a few characters)
- `GET /api/cities/search?query=<query>&user_lat=<lat>&user_lng=<lng>&user_country=<country>` - Location-aware search that prioritizes results based on user's location
- `GET /api/cities/coordinates?lat=<lat>&lng=<lng>&radius_km=<radius>` - Get cities near coordinates
- `GET /api/countries` - Get a list of countries
- `GET /api/states?country=<country>` - Get states in a country
- `GET /api/cities/state?state=<state>&country=<country>` - Get cities in a state
- `GET /health` - Health check endpoint

## Database

The module will create a SQLite database if no database URI is provided. The database will be populated with data from the included CSV file.

## City Data Download

GeoDash requires city data to function properly. When you first use GeoDash, it will automatically:

1. Check if the city data CSV file exists in standard locations
   - Inside the package at `GeoDash/data/cities.csv`
   - In the current working directory's `data/` folder
   - In the user's home directory at `~/.geodash/data/cities.csv`
2. If not found, it will attempt to download the file from our servers
3. The download happens only once, when the module is first used

When running in server mode, the database is initialized during application startup, ensuring that:
- The database schema is created
- City data is imported if needed
- The application is ready to handle requests immediately

The city data is sourced from the [countries-states-cities-database](https://github.com/dr5hn/countries-states-cities-database) repository maintained by [Darshan Gada](https://github.com/dr5hn).

You can also manage the city data manually:

```python
# Import the download function
from GeoDash.data.importer import download_city_data

# Manually download/update the city data at any time
download_city_data(force=True)  # force=True to redownload even if it exists
```

If you're using GeoDash in an environment without internet access, you can:
1. Pre-download the city data by including `--download-city-data` when installing:
   ```bash
   pip install GeoDash --download-city-data
   ```
2. Or manually place the `cities.csv` file in one of these locations:
   - Inside the installed package at `GeoDash/data/cities.csv`
   - In the user's home directory at `~/.geodash/data/cities.csv`
   - In the top-level `data/` directory if running from source
   - In the current working directory's `data/` folder
3. Or manually download the file from [here](https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/csv/cities.csv) and place it in one of the above locations

## Data Format

The CSV file for city data should contain the following columns:
- `id` (required): Unique identifier for the city
- `name` (required): City name
- `country_name` (required): Country name
- `latitude` (required): Latitude coordinate
- `longitude` (required): Longitude coordinate
- `state_name` (optional): State or province name
- `population` (optional): City population (used for sorting)

## Location-Aware Search

GeoDash provides a location-aware search feature that prioritizes search results based on the user's location:

### How it works

1. **Text matching remains the primary factor** - Results must match the search query text
2. **Location prioritization** - From the matching cities, the results are prioritized based on:
   - User's country (if provided via `user_country` parameter)
   - Proximity to user's coordinates (if provided via `user_lat` and `user_lng` parameters)

### Examples

```python
# Prioritize "San" cities near San Francisco coordinates
cities = city_data.search_cities(
    'San',
    user_lat=37.7749,
    user_lng=-122.4194
)

# Prioritize "New" cities in the United States
cities = city_data.search_cities(
    'New',
    user_country='United States'
)

# Combined approach - prioritize "New" cities in the US and near New York coordinates
cities = city_data.search_cities(
    'New',
    user_country='United States',
    user_lat=40.7128,
    user_lng=-74.0060
)
```

### API Integration

You can use this feature in your frontend applications to provide personalized search results:

```javascript
// Get user's coordinates from browser
navigator.geolocation.getCurrentPosition(position => {
  const lat = position.coords.latitude;
  const lng = position.coords.longitude;

  // Call the API with user's coordinates
  fetch(`/api/cities/search?query=${searchText}&user_lat=${lat}&user_lng=${lng}`)
    .then(response => response.json())
    .then(data => {
      // Display personalized results
      updateResults(data.results);
    });
});
```

## Configuration

GeoDash provides a flexible configuration system that allows you to customize its behavior to fit your specific needs. You can configure database settings, search options, feature flags, API parameters, and more.

### Basic Configuration

To use a custom configuration file:

```python
from GeoDash import CityData

# Load configuration from a custom file
cities = CityData(config_path='/path/to/your/geodash.yml')
```

You can also override specific configuration options programmatically:

```python
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

### Simple vs Advanced Mode

GeoDash supports two operation modes:

- **Simple Mode**: Optimized for lower-end systems by disabling resource-intensive features.
- **Advanced Mode**: Enables all features for maximum functionality (default).

```python
# Use simple mode
cities = CityData(config_overrides={'mode': 'simple'})
```

### Configuration File Locations

GeoDash looks for configuration files in the following locations (in order of priority):

1. Custom path specified with `config_path` parameter
2. Path specified in the `GEODASH_CONFIG` environment variable
3. Current working directory: `./geodash.yml`
4. User's home directory: `~/.geodash/geodash.yml`
5. GeoDash package directory: `[package_path]/data/geodash.yml`

### Environment Variables

GeoDash supports configuration through environment variables:

- `GEODASH_CONFIG`: Path to a configuration file
- `GEODASH_MODE`: Operation mode (`simple` or `advanced`)
- `GEODASH_LOG_LEVEL`: Logging level (`debug`, `info`, `warning`, `error`, `critical`)
- `GEODASH_LOG_FORMAT`: Logging format (`json` or `text`)
- `GEODASH_LOG_FILE`: Path to a log file
- `GEODASH_DB_URI`: Database URI (overrides configuration file)
- `GEODASH_API_HOST`: API server host
- `GEODASH_API_PORT`: API server port
- `GEODASH_API_DEBUG`: Enable API debug mode (`true` or `false`)
- `GEODASH_WORKERS`: Number of worker processes for the API server

### Example Configuration

Here's a simple example configuration file in YAML format:

```yaml
# Set the operation mode
mode: "advanced"

# Feature flags
features:
  enable_fuzzy_search: true
  enable_location_aware: true
  enable_memory_caching: true
  enable_shared_memory: true
  enable_advanced_db: true
  auto_fetch_data: true

# Database configuration
database:
  type: sqlite
  sqlite:
    path: "/path/to/geodash.db"
    rtree: true
    fts: true

# Search configuration
search:
  fuzzy:
    threshold: 70
    enabled: true
  limits:
    default: 10
    max: 100
```

### Detailed Configuration Guide

For comprehensive documentation of all configuration options, please see the [Configuration Guide](docs/configuration.md).

Example configuration files are available in the `examples/` directory:
- [Simple Configuration](examples/geodash_simple.yml)
- [Advanced Configuration](examples/geodash_advanced.yml)
- [PostgreSQL Configuration](examples/geodash_postgres.yml)

### Feature Flags

GeoDash uses feature flags to enable or disable specific functionality. For detailed information about available flags and their effects, see the [Feature Flags Reference](docs/feature_flags.md).

## Production Deployment

For production environments, GeoDash includes a helper script:

```bash
# Start the GeoDash API server in production mode with Gunicorn
./run_production.sh
```

The `run_production.sh` script:
- Sets the necessary environment variables
- Starts the application with Gunicorn using the included configuration
- Provides better performance, reliability, and security for production deployments

Note: Make sure Gunicorn is installed (`pip install gunicorn`) before using this script.

## Dependencies

- pandas
- flask (for API server mode)
- psycopg2-binary (for PostgreSQL support)

## Development

For development, clone the repository and install in development mode:

```bash
# Clone the repository
git clone https://github.com/cryptekbits/GeoDash-py.git
cd GeoDash-py

# Install in development mode
pip install -e .
```

## License

MIT

## Logging

GeoDash uses a centralized logging system that provides structured logging with consistent fields. The logging system can output in either JSON format (for better machine processing) or traditional text format.

### Basic Usage

```python
from GeoDash.utils.logging import get_logger

# Get a logger with the current module name
logger = get_logger(__name__)

# Log at different levels
logger.debug("Debug message")
logger.info("Processing started")
logger.warning("Warning about something")
logger.error("An error occurred")
```

### Structured Logging

The logging system supports structured logging with consistent fields. You can add context to your logs:

```python
# Add component context when getting the logger
logger = get_logger(__name__, {"component": "data_importer"})

# Add context to individual log messages
logger.info("Importing cities", extra={"city_count": 500, "source": "cities.csv"})
```

### Configuration

You can configure the logging system:

```python
from GeoDash.utils.logging import configure_logging, set_log_level

# Set log level
set_log_level('debug')  # or 'info', 'warning', 'error', 'critical'

# More advanced configuration
configure_logging(
    level='info',
    use_json=False,  # Optional: Use JSON structured logging (defaults to False)
    log_file='/path/to/log.txt'  # Optional log file
)
```

You can also configure logging via environment variables:

- `GEODASH_LOG_LEVEL`: Set the log level ('debug', 'info', 'warning', 'error', 'critical')
- `GEODASH_LOG_FORMAT`: Set the log format ('json' or 'text')
- `GEODASH_LOG_FILE`: Path to a log file

## Logging Configuration

GeoDash uses a centralized logging system that can be configured in several ways:

### Setting the Log Level Programmatically

```python
from GeoDash import set_log_level

# Use string constants
set_log_level('debug')  # Show all debug logs
set_log_level('info')   # Default level
set_log_level('warning')
set_log_level('error')
set_log_level('critical')

# Or use Python's logging constants
import logging
set_log_level(logging.DEBUG)
```

### Setting the Log Level via Command Line

All CLI commands support the `--log-level` option:

```bash
# Search for cities with debug logs enabled
python -m GeoDash search "New York" --log-level debug

# Start the server with warning level (fewer logs)
python -m GeoDash server --log-level warning
```

### Setting the Log Level via Environment Variable

You can set the log level using the `GEODASH_LOG_LEVEL` environment variable:

```bash
# Set log level to debug
export GEODASH_LOG_LEVEL=debug
python -m GeoDash search "New York"

# One-time setting
GEODASH_LOG_LEVEL=debug python -m GeoDash search "New York"
```

### Using the Logger in Your Code

If you're extending GeoDash or writing plugins, use the provided logging utilities:

```python
from GeoDash.utils.logging import get_logger

# Get a logger for your module
logger = get_logger(__name__)

# Use standard logging methods
logger.debug("Detailed debugging information")
logger.info("General information")
logger.warning("Warning message")
logger.error("Error message")
logger.critical("Critical error")
```