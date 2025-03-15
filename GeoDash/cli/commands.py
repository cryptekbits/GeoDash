"""
Command-line interface (CLI) commands for the GeoDash package.

This module provides CLI commands for accessing city data from the command line.
"""

import sys
import json
import logging
import argparse
from typing import Any, Dict, List, Optional

from GeoDash.data import CityData
from GeoDash.api.server import start_server

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def search_command(args: argparse.Namespace) -> int:
    """
    Search for cities by name with optional country filter.
    
    Args:
        args: Command-line arguments with query, limit, country parameters
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        with CityData(args.db_uri) as city_data:
            results = city_data.search_cities(
                query=args.query, 
                limit=args.limit, 
                country=args.country
            )
            
            # Print results as JSON
            print(json.dumps(results, indent=2, ensure_ascii=False))
            
            return 0
    except Exception as e:
        logger.error(f"Error during search: {str(e)}")
        return 1

def city_command(args: argparse.Namespace) -> int:
    """
    Get a city by its ID.
    
    Args:
        args: Command-line arguments with city_id parameter
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        with CityData(args.db_uri) as city_data:
            city = city_data.get_city(city_id=args.city_id)
            
            if city:
                print(json.dumps(city, indent=2, ensure_ascii=False))
                return 0
            else:
                print(f"City with ID {args.city_id} not found")
                return 1
    except Exception as e:
        logger.error(f"Error getting city: {str(e)}")
        return 1

def coordinates_command(args: argparse.Namespace) -> int:
    """
    Find cities within a radius of specified coordinates.
    
    Args:
        args: Command-line arguments with lat, lng, radius parameters
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        with CityData(args.db_uri) as city_data:
            cities = city_data.get_cities_by_coordinates(
                lat=args.lat,
                lng=args.lng,
                radius_km=args.radius
            )
            
            print(json.dumps(cities, indent=2, ensure_ascii=False))
            return 0
    except Exception as e:
        logger.error(f"Error finding cities by coordinates: {str(e)}")
        return 1

def countries_command(args: argparse.Namespace) -> int:
    """
    List all countries.
    
    Args:
        args: Command-line arguments
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        with CityData(args.db_uri) as city_data:
            countries = city_data.get_countries()
            
            print(json.dumps(countries, indent=2, ensure_ascii=False))
            return 0
    except Exception as e:
        logger.error(f"Error getting countries: {str(e)}")
        return 1

def states_command(args: argparse.Namespace) -> int:
    """
    List all states in a country.
    
    Args:
        args: Command-line arguments with country parameter
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        with CityData(args.db_uri) as city_data:
            states = city_data.get_states(country=args.country)
            
            print(json.dumps(states, indent=2, ensure_ascii=False))
            return 0
    except Exception as e:
        logger.error(f"Error getting states: {str(e)}")
        return 1

def cities_in_state_command(args: argparse.Namespace) -> int:
    """
    List all cities in a state within a country.
    
    Args:
        args: Command-line arguments with state and country parameters
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        with CityData(args.db_uri) as city_data:
            cities = city_data.get_cities_in_state(
                state=args.state,
                country=args.country
            )
            
            print(json.dumps(cities, indent=2, ensure_ascii=False))
            return 0
    except Exception as e:
        logger.error(f"Error getting cities in state: {str(e)}")
        return 1

def import_data_command(args: argparse.Namespace) -> int:
    """
    Import city data from CSV file.
    
    Args:
        args: Command-line arguments with csv_path and batch_size parameters
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        with CityData(args.db_uri) as city_data:
            success = city_data.import_city_data(
                csv_path=args.csv_path,
                batch_size=args.batch_size
            )
            
            if success:
                print("Data import completed successfully")
                return 0
            else:
                print("Data import failed")
                return 1
    except Exception as e:
        logger.error(f"Error importing data: {str(e)}")
        return 1

def table_info_command(args: argparse.Namespace) -> int:
    """
    Get information about the city_data table.
    
    Args:
        args: Command-line arguments
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        with CityData(args.db_uri) as city_data:
            info = city_data.get_table_info()
            
            print(json.dumps(info, indent=2, ensure_ascii=False))
            return 0
    except Exception as e:
        logger.error(f"Error getting table info: {str(e)}")
        return 1

def server_command(args: argparse.Namespace) -> int:
    """
    Start the GeoDash API server.
    
    Args:
        args: Command-line arguments with host, port, debug parameters
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        logger.info(f"Starting server on {args.host}:{args.port}")
        start_server(
            host=args.host,
            port=args.port,
            db_uri=args.db_uri,
            debug=args.debug
        )
        return 0
    except Exception as e:
        logger.error(f"Error starting server: {str(e)}")
        return 1

def main() -> int:
    """
    Main entry point for the GeoDash command-line interface.
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    parser = argparse.ArgumentParser(description='GeoDash CLI for accessing city data')
    parser.add_argument('--db-uri', help='Database URI (e.g., sqlite:///cities.db or postgresql://user:pass@localhost/cities)')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    subparsers.required = True
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search for cities by name')
    search_parser.add_argument('query', help='Search query (city name prefix)')
    search_parser.add_argument('--limit', type=int, default=10, help='Maximum number of results (default: 10)')
    search_parser.add_argument('--country', help='Filter by country name')
    search_parser.set_defaults(func=search_command)
    
    # City by ID command
    city_parser = subparsers.add_parser('city', help='Get a city by ID')
    city_parser.add_argument('city_id', type=int, help='City ID')
    city_parser.set_defaults(func=city_command)
    
    # Coordinates command
    coords_parser = subparsers.add_parser('coordinates', help='Find cities near coordinates')
    coords_parser.add_argument('lat', type=float, help='Latitude')
    coords_parser.add_argument('lng', type=float, help='Longitude')
    coords_parser.add_argument('--radius', type=float, default=10, help='Search radius in kilometers (default: 10)')
    coords_parser.set_defaults(func=coordinates_command)
    
    # Countries command
    countries_parser = subparsers.add_parser('countries', help='List all countries')
    countries_parser.set_defaults(func=countries_command)
    
    # States command
    states_parser = subparsers.add_parser('states', help='List all states in a country')
    states_parser.add_argument('country', help='Country name')
    states_parser.set_defaults(func=states_command)
    
    # Cities in state command
    cities_state_parser = subparsers.add_parser('cities-in-state', help='List all cities in a state')
    cities_state_parser.add_argument('state', help='State name')
    cities_state_parser.add_argument('country', help='Country name')
    cities_state_parser.set_defaults(func=cities_in_state_command)
    
    # Import data command
    import_parser = subparsers.add_parser('import', help='Import city data from CSV')
    import_parser.add_argument('--csv-path', help='Path to CSV file (optional)')
    import_parser.add_argument('--batch-size', type=int, default=5000, help='Batch size for import (default: 5000)')
    import_parser.set_defaults(func=import_data_command)
    
    # Table info command
    info_parser = subparsers.add_parser('table-info', help='Get information about the city_data table')
    info_parser.set_defaults(func=table_info_command)
    
    # Server command
    server_parser = subparsers.add_parser('server', help='Start the GeoDash API server')
    server_parser.add_argument('--host', type=str, default='0.0.0.0', help='The host to bind to')
    server_parser.add_argument('--port', type=int, default=5000, help='The port to bind to')
    server_parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    server_parser.set_defaults(func=server_command)
    
    # Parse arguments and execute command
    args = parser.parse_args()
    return args.func(args)

if __name__ == '__main__':
    sys.exit(main()) 