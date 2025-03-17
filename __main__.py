#!/usr/bin/env python3
"""
Main entry point for the GeoDash package when run as a program.
This module is executed when the package is run directly with:
    python -m GeoDash
"""

import sys
from GeoDash.utils.logging import get_logger, set_log_level

# Configure logging
set_log_level('info')
logger = get_logger(__name__, {"component": "main"})

def main():
    """Main entry point when run as a program."""
    try:
        from GeoDash.__main__ import main as geodash_main
        return geodash_main()
    except ImportError as e:
        logger.error(f"Failed to import GeoDash: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 