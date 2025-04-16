"""
Command-line interface (CLI) commands for the GeoDash package.

This module provides CLI commands for accessing city data from the command line.
"""

import sys
import json
from typing import Any, Dict, List, Optional

import click

from GeoDash.services.city_service import CityService
from GeoDash.api.server import start_server
from GeoDash.utils import log_error_with_github_info
from GeoDash.utils.logging import get_logger, set_log_level
from GeoDash.config import get_config
from GeoDash.cli.config_commands import config_show, config_init, config_validate

# Get a logger for this module
logger = get_logger(__name__)

# Common options
def db_uri_option(f):
    return click.option('--db-uri', help='Database URI (e.g., sqlite:///cities.db or postgresql://user:pass@localhost/cities)')(f)

# Add an option for setting the log level to all commands
def log_level_option(f):
    return click.option('--log-level', 
                      type=click.Choice(['debug', 'info', 'warning', 'error', 'critical'], case_sensitive=False),
                      help='Set the logging level')(f)

# Apply the log level if specified in the command options
def apply_log_level(ctx, param, value):
    if value:
        set_log_level(value)
    return value

@click.group()
def cli():
    """GeoDash CLI for accessing city data."""
    pass

# Configuration commands group
@cli.group('config')
def config_group():
    """
    Manage GeoDash configuration.
    
    Commands for working with the configuration system, including viewing,
    creating, and validating configuration files.
    """
    pass

@config_group.command('show')
@click.option('--format', 'format_type', type=click.Choice(['yaml', 'json'], case_sensitive=False), 
              default='yaml', help='Output format (yaml or json)')
@click.option('--section', help='Show only a specific configuration section')
@log_level_option
def show_config_command(format_type, section, log_level):
    """Display the current active configuration."""
    return config_show(format_type, section)

@config_group.command('init')
@click.option('--output', 'output_path', help='Path where to create the configuration file')
@log_level_option
def init_config_command(output_path, log_level):
    """Create a template configuration file with explanatory comments."""
    return config_init(output_path)

@config_group.command('validate')
@click.argument('config_path')
@log_level_option
def validate_config_command(config_path, log_level):
    """Validate a configuration file."""
    return config_validate(config_path)

@cli.command('search')
@click.argument('query')
@click.option('--limit', type=int, default=10, help='Maximum number of results (default: 10)')
@click.option('--country', help='Filter by country name')
@db_uri_option
@log_level_option
def search_command(query, limit, country, db_uri, log_level):
    """Search for cities by name with optional country filter."""
    try:
        with CityService(db_uri) as city_service:
            results = city_service.search_cities(
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
@log_level_option
def city_command(city_id, db_uri, log_level):
    """Get a city by its ID."""
    try:
        with CityService(db_uri) as city_service:
            city = city_service.get_city(city_id=city_id)
            
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
@log_level_option
def coordinates_command(lat, lng, radius, db_uri, log_level):
    """Find cities within a radius of specified coordinates."""
    try:
        with CityService(db_uri) as city_service:
            cities = city_service.get_cities_by_coordinates(
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
@log_level_option
def countries_command(db_uri, log_level):
    """List all countries."""
    try:
        with CityService(db_uri) as city_service:
            countries = city_service.get_countries()
            
            click.echo(json.dumps(countries, indent=2, ensure_ascii=False))
            return 0
    except Exception as e:
        logger.error(f"Error getting countries: {str(e)}")
        return 1

@cli.command('states')
@click.argument('country')
@db_uri_option
@log_level_option
def states_command(country, db_uri, log_level):
    """List all states in a country."""
    try:
        with CityService(db_uri) as city_service:
            states = city_service.get_states(country=country)
            
            click.echo(json.dumps(states, indent=2, ensure_ascii=False))
            return 0
    except Exception as e:
        logger.error(f"Error getting states: {str(e)}")
        return 1

@cli.command('cities-in-state')
@click.argument('state')
@click.argument('country')
@db_uri_option
@log_level_option
def cities_in_state_command(state, country, db_uri, log_level):
    """List all cities in a state within a country."""
    try:
        with CityService(db_uri) as city_service:
            cities = city_service.get_cities_in_state(
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
@log_level_option
def import_data_command(csv_path, batch_size, db_uri, log_level):
    """Import city data from CSV file."""
    try:
        with CityService(db_uri) as city_service:
            success = city_service.import_city_data(
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
@log_level_option
def table_info_command(db_uri, log_level):
    """Get information about the city_data table."""
    try:
        with CityService(db_uri) as city_service:
            info = city_service.get_table_info()
            
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
@log_level_option
def server_command(host, port, debug, db_uri, log_level):
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
        # Load configuration before executing commands
        config = get_config()
        config.load_config()
        
        # Setup logging from configuration if not overridden by command line
        log_level = config.get("logging.level", "info")
        set_log_level(log_level)
        
        logger.debug("Configuration loaded successfully")
        
        # Run the CLI
        return cli()
    except Exception as e:
        log_error_with_github_info(e, "Error in CLI command")
        return 1

if __name__ == '__main__':
    sys.exit(main()) 