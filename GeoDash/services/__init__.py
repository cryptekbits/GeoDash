"""
Service layer for the GeoDash package.

This module provides service functions that can be used by both the CLI and API layers,
avoiding code duplication and centralizing business logic.
"""

from GeoDash.services.city_service import CityService

__all__ = ['CityService'] 