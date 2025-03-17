#!/usr/bin/env python3
"""
Main entry point for the GeoDash package when run as a module.

This module provides the entry point for running the GeoDash package as a module
using `python -m GeoDash`. It delegates to the CLI's main function.

Example:
    $ python -m GeoDash search "New York"
    $ python -m GeoDash coordinates 40.7128 -74.0060
    $ python -m GeoDash server --host localhost --port 8080
"""

import sys
from GeoDash.utils.logging import get_logger

# Get a logger for this module
logger = get_logger(__name__)

def main():
    """Main entry point for the GeoDash package."""
    try:
        from GeoDash.cli.commands import main as cli_main
        cli_main()
    except ImportError as e:
        from GeoDash.utils import log_error_with_github_info
        log_error_with_github_info(e, "Failed to import CLI module")
        sys.exit(1)
    except Exception as e:
        from GeoDash.utils import log_error_with_github_info
        log_error_with_github_info(e, "Error running GeoDash")
        sys.exit(1)

if __name__ == "__main__":
    main() 