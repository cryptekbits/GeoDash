"""
Command-line interface module for the GeoDash package.

This module provides a command-line interface for interacting with the GeoDash
package functionality. It includes commands for searching cities, retrieving
city information by coordinates, and managing hierarchical data.

Key Components:
- main: Main entry point for the CLI
- Commands: Various commands for accessing city data
"""

from GeoDash.cli.commands import main

__all__ = ['main'] 