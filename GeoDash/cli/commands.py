"""
Command-line interface (CLI) commands for the GeoDash package.

This module provides CLI commands for accessing city data from the command line.
"""

import sys
import json
import logging
from typing import Any, Dict, List, Optional

import click

from GeoDash.data import CityData
from GeoDash.api.server import start_server
from GeoDash.utils import log_error_with_github_info

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Common options
def db_uri_option(f):
    return click.option('--db-uri', help='Database URI (e.g., sqlite:///cities.db or postgresql://user:pass@localhost/cities)')(f)

@click.group()
def cli():
    """GeoDash CLI for accessing city data."""
    pass

@cli.command('search')
@click.argument('query')
@click.option('--limit', type=int, default=10, help='Maximum number of results (default: 10)')
@click.option('--country', help='Filter by country name')
@db_uri_option
def search_command(query, limit, country, db_uri):
    """Search for cities by name with optional country filter."""
    try:
        with CityData(db_uri) as city_data:
            results = city_data.search_cities(
                query=query, 
                limit=limit, 
                country=country
            )
            
            # Print results as JSON
            click.echo(json.dumps(results, indent=2, ensure_ascii=False))
            
            return 0
    except Exception as e:
        logger.error(f"Error during search: {str(e)}")
        return 1

@cli.command('city')
@click.argument('city_id', type=int)
@db_uri_option
def city_command(city_id, db_uri):
    """Get a city by its ID."""
    try:
        with CityData(db_uri) as city_data:
            city = city_data.get_city(city_id=city_id)
            
            if city:
                click.echo(json.dumps(city, indent=2, ensure_ascii=False))
                return 0
            else:
                click.echo(f"City with ID {city_id} not found")
                return 1
    except Exception as e:
        logger.error(f"Error getting city: {str(e)}")
        return 1

@cli.command('coordinates')
@click.argument('lat', type=float)
@click.argument('lng', type=float)
@click.option('--radius', type=float, default=10, help='Search radius in kilometers (default: 10)')
@db_uri_option
def coordinates_command(lat, lng, radius, db_uri):
    """Find cities within a radius of specified coordinates."""
    try:
        with CityData(db_uri) as city_data:
            cities = city_data.get_cities_by_coordinates(
                lat=lat,
                lng=lng,
                radius_km=radius
            )
            
            click.echo(json.dumps(cities, indent=2, ensure_ascii=False))
            return 0
    except Exception as e:
        logger.error(f"Error finding cities by coordinates: {str(e)}")
        return 1

@cli.command('countries')
@db_uri_option
def countries_command(db_uri):
    """List all countries."""
    try:
        with CityData(db_uri) as city_data:
            countries = city_data.get_countries()
            
            click.echo(json.dumps(countries, indent=2, ensure_ascii=False))
            return 0
    except Exception as e:
        logger.error(f"Error getting countries: {str(e)}")
        return 1

@cli.command('states')
@click.argument('country')
@db_uri_option
def states_command(country, db_uri):
    """List all states in a country."""
    try:
        with CityData(db_uri) as city_data:
            states = city_data.get_states(country=country)
            
            click.echo(json.dumps(states, indent=2, ensure_ascii=False))
            return 0
    except Exception as e:
        logger.error(f"Error getting states: {str(e)}")
        return 1

@cli.command('cities-in-state')
@click.argument('state')
@click.argument('country')
@db_uri_option
def cities_in_state_command(state, country, db_uri):
    """List all cities in a state within a country."""
    try:
        with CityData(db_uri) as city_data:
            cities = city_data.get_cities_in_state(
                state=state,
                country=country
            )
            
            click.echo(json.dumps(cities, indent=2, ensure_ascii=False))
            return 0
    except Exception as e:
        logger.error(f"Error getting cities in state: {str(e)}")
        return 1

@cli.command('import')
@click.option('--csv-path', help='Path to CSV file (optional)')
@click.option('--batch-size', type=int, default=5000, help='Batch size for import (default: 5000)')
@db_uri_option
def import_data_command(csv_path, batch_size, db_uri):
    """Import city data from CSV file."""
    try:
        with CityData(db_uri) as city_data:
            success = city_data.import_city_data(
                csv_path=csv_path,
                batch_size=batch_size
            )
            
            if success:
                click.echo("Data import completed successfully")
                return 0
            else:
                click.echo("Data import failed")
                return 1
    except Exception as e:
        logger.error(f"Error importing data: {str(e)}")
        return 1

@cli.command('table-info')
@db_uri_option
def table_info_command(db_uri):
    """Get information about the city_data table."""
    try:
        with CityData(db_uri) as city_data:
            info = city_data.get_table_info()
            
            click.echo(json.dumps(info, indent=2, ensure_ascii=False))
            return 0
    except Exception as e:
        logger.error(f"Error getting table info: {str(e)}")
        return 1

@cli.command('server')
@click.option('--host', type=str, default='0.0.0.0', help='The host to bind to')
@click.option('--port', type=int, default=5000, help='The port to bind to')
@click.option('--debug/--no-debug', default=False, help='Enable debug mode')
@db_uri_option
def server_command(host, port, debug, db_uri):
    """Start the GeoDash API server."""
    try:
        logger.info(f"Starting GeoDash API server at {host}:{port}")
        
        start_server(
            host=host,
            port=port,
            db_uri=db_uri,
            debug=debug
        )
        return 0
    except Exception as e:
        log_error_with_github_info(e, "Error starting server")
        return 1

def main():
    """Main entry point for the GeoDash command-line interface."""
    try:
        return cli()
    except Exception as e:
        log_error_with_github_info(e, "Error in CLI command")
        return 1

if __name__ == '__main__':
    sys.exit(main()) 