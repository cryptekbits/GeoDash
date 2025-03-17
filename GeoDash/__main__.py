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
        from GeoDash.utils import log_error_with_github_info, handle_exception
        from GeoDash.exceptions import SystemError
        
        # Convert to a system error with user-friendly message
        error = handle_exception(
            e,
            logger=logger,
            error_class=SystemError,
            user_message="Failed to start GeoDash. The application may be incorrectly installed.",
            report_to_github=True
        )
        
        # Print user-friendly message to console
        print(f"Error: {error.user_message}")
        print("Technical details have been logged.")
        sys.exit(1)
    except Exception as e:
        from GeoDash.utils import log_error_with_github_info, handle_exception
        from GeoDash.exceptions import GeoDataError
        
        # Handle the exception with our utility function
        error = handle_exception(
            e,
            logger=logger,
            error_class=GeoDataError,
            user_message="An error occurred while running GeoDash.",
            report_to_github=True
        )
        
        # Print user-friendly message to console
        if hasattr(error, 'user_message'):
            print(f"Error: {error.user_message}")
        else:
            print(f"Error: An unexpected error occurred.")
        
        print("Technical details have been logged.")
        sys.exit(1)

if __name__ == "__main__":
    main() 