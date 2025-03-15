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
git clone https://github.com/yourusername/GeoDash.git
cd GeoDash

# Install directly
pip install .

# OR install in development mode
pip install -e .
```

### Installing from Source Directory

If you already have the source code directory (e.g., downloaded as a ZIP and extracted):

```bash
# Navigate to the GeoDash directory
cd /path/to/GeoDash

# Install the package
pip install .

# OR install in development mode (for development)
pip install -e .
```

When installing from source, the city data CSV file will be automatically downloaded. If you're in an environment without internet access, you can manually place the `cities.csv` file in `GeoDash/data/` before installation.

## Features

- Fast queries for coordinates
- Smart autocomplete that works with just a few characters (3+)
- Prefix-based search for better UI autocomplete experience
- Population-based sorting for more relevant results
- Support for both SQLite and PostgreSQL databases
- Command-line interface
- API server mode

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
GeoDash get-city 1234

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
```

### As an API Server

```bash
# Start the API server using the console script
GeoDash server --host 0.0.0.0 --port 5000 --debug
```

#### API Endpoints

- `GET /api/city/<city_id>` - Get a city by ID
- `GET /api/cities/search?query=<query>&limit=<limit>&country=<country>` - Search for cities with autocomplete support (works with just a few characters)
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
2. If not found, it will attempt to download the file from our servers
3. The download happens only once, when the module is first used

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
   - In the top-level `data/` directory if running from source
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

## Dependencies

- pandas
- flask (for API server mode)
- psycopg2-binary (for PostgreSQL support)

## Development

For development, clone the repository and install in development mode:

```bash
# Clone the repository
git clone https://github.com/yourusername/GeoDash.git
cd GeoDash

# Install in development mode
pip install -e .
```

## License

MIT 