"""
Data management module for the CitiZen package.

This module provides classes and functions for managing city data, including
database management, schema management, data import, and query repositories.
"""

from citizen.data.city_manager import CityData
from citizen.data.database import DatabaseManager
from citizen.data.schema import SchemaManager
from citizen.data.importer import CityDataImporter
from citizen.data.repositories import (
    BaseRepository,
    CityRepository,
    GeoRepository,
    RegionRepository
)

__all__ = [
    'CityData',
    'DatabaseManager',
    'SchemaManager',
    'CityDataImporter',
    'BaseRepository',
    'CityRepository',
    'GeoRepository',
    'RegionRepository'
] 