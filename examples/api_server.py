#!/usr/bin/env python3
"""
Example showing how to run the GeoDash module as an API server.

This script starts a Flask server that provides REST API endpoints
for accessing the city data functionality.
"""
import argparse

# Import GeoDash components
from GeoDash import start_server
from GeoDash.utils.logging import get_logger, set_log_level

# Configure logging
set_log_level('info')
logger = get_logger(__name__, {"component": "api_example"})

def main():
    """Main entry point for the API server example."""
    parser = argparse.ArgumentParser(description='City Data API Server')
    parser.add_argument('--host', default='localhost', help='Host to listen on')
    parser.add_argument('--port', type=int, default=5001, help='Port to listen on')  # Changed from 5000 to 5001
    parser.add_argument('--db-uri', help='Database URI to use')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode')
    
    args = parser.parse_args()
    
    print("Starting the API server...")
    print(f"API will be available at http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop the server")
    
    # Start the server
    start_server(
        host=args.host,
        port=args.port,
        db_uri=args.db_uri,
        debug=args.debug
    )

if __name__ == '__main__':
    main() 