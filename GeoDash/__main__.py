#!/usr/bin/env python3
"""
Main entry point for the CitiZen package when run as a module.

This module provides the entry point for running the CitiZen package as a module
using `python -m citizen`. It delegates to the CLI's main function.

Example:
    $ python -m citizen search "New York"
    $ python -m citizen coordinates 40.7128 -74.0060
    $ python -m citizen server --host localhost --port 8080
"""

import sys
import logging

# Configure logging with proper format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for the CitiZen package."""
    try:
        from citizen.cli.commands import main as cli_main
        cli_main()
    except ImportError as e:
        logger.error(f"Failed to import CLI module: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error running CitiZen: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main() 