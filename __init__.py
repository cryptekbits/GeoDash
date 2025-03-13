"""
CitiZen - A Python package for managing city data with fast coordinate queries and autocomplete functionality.
"""
import os
import sys

# Add parent directory to Python path for relative imports
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from citizen.data.city_manager import CityData
from citizen.api.server import start_server

__version__ = '1.0.0'
__all__ = ['CityData', 'start_server'] 