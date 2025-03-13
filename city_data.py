"""
Compatibility file for backwards compatibility.
This file imports the CityData class from the new module structure.
"""

from citizen.data.city_manager import CityData

__all__ = ['CityData'] 