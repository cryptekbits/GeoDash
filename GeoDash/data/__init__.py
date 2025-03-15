"""
Data management module for the GeoDash package.

This module provides classes and functions for managing city data, including
database management, schema management, data import, and query repositories.
"""

from GeoDash.data.city_manager import CityData
from GeoDash.data.database import DatabaseManager
from GeoDash.data.schema import SchemaManager
from GeoDash.data.importer import CityDataImporter
from GeoDash.data.repositories import (
    BaseRepository,
    CityRepository,
    GeoRepository,
    RegionRepository,
    get_city_repository,
    get_geo_repository,
    get_region_repository
)

__all__ = [
    'CityData',
    'DatabaseManager',
    'SchemaManager',
    'CityDataImporter',
    'BaseRepository',
    'CityRepository',
    'GeoRepository',
    'RegionRepository',
    'get_city_repository',
    'get_geo_repository',
    'get_region_repository'
] 