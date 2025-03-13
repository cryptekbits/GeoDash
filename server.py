#!/usr/bin/env python3
"""
Entry point for the CitiZen API server.
This allows running the server directly with `python server.py`.
"""
import argparse
import logging
from typing import Optional

from citizen.api.server import start_server

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Entry point for the server."""
    parser = argparse.ArgumentParser(description='Start the CitiZen API server')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='The host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='The port to bind to')
    parser.add_argument('--db-uri', type=str, help='The database URI')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    args = parser.parse_args()
    
    logger.info(f"Starting server on {args.host}:{args.port}")
    
    start_server(
        host=args.host,
        port=args.port,
        db_uri=args.db_uri,
        debug=args.debug
    )

if __name__ == '__main__':
    main() 