#!/usr/bin/env python3
"""
Main entry point for the GeoDash package.
This allows running the package directly with `python -m`.
"""
import os
import sys

# Add the parent directory to the path if not already there
# This ensures the GeoDash module can be found regardless of how it's run
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from GeoDash.cli.commands import main

if __name__ == '__main__':
    main() 