#!/usr/bin/env python3
"""
Entry point for the GeoDash API server.
This allows running the server directly with `python server.py`.
"""
import argparse
from typing import Optional

from GeoDash.api.server import start_server
from GeoDash.utils.logging import get_logger, set_log_level

# Get a logger for this module
logger = get_logger(__name__)

def main():
    """Entry point for the server."""
    parser = argparse.ArgumentParser(description='Start the GeoDash API server')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='The host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='The port to bind to')
    parser.add_argument('--db-uri', type=str, help='The database URI')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--log-level', choices=['debug', 'info', 'warning', 'error', 'critical'], 
                        default='info', help='Set the logging level')
    
    args = parser.parse_args()
    
    # Set the log level if specified
    if args.log_level:
        set_log_level(args.log_level)
    
    logger.info(f"Starting server on {args.host}:{args.port}")
    
    start_server(
        host=args.host,
        port=args.port,
        db_uri=args.db_uri,
        debug=args.debug
    )

if __name__ == "__main__":
    main() 