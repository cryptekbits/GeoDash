"""
City service module for the GeoDash package.

This module provides service methods for city data operations that can be used
by both the CLI and API layers.
"""

from typing import Dict, List, Any, Optional, Union

from GeoDash.data import CityData
from GeoDash.utils.logging import get_logger

# Get a logger for this module
logger = get_logger(__name__)

class CityService:
    """
    Service class for city data operations.
    
    This class wraps the CityData class to provide standardized service methods
    that can be used by both the CLI and API layers.
    """
    
    def __init__(self, db_uri: Optional[str] = None, persistent: bool = False):
        """
        Initialize the CityService.
        
        Args:
            db_uri: Database URI to connect to. If None, uses SQLite in the data directory.
            persistent: Whether to keep database connections open (for worker processes)
        """
        self.city_data = CityData(db_uri=db_uri, persistent=persistent)
        
    def search_cities(
        self, 
        query: str, 
        limit: int = 10, 
        country: Optional[str] = None, 
        user_lat: Optional[float] = None, 
        user_lng: Optional[float] = None, 
        user_country: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for cities by name with optional location-aware prioritization.
        
        Args:
            query: The search query (city name or prefix)
            limit: Maximum number of results to return
            country: Optional country filter (restricts results to this country)
            user_lat: User's latitude for location-aware prioritization
            user_lng: User's longitude for location-aware prioritization
            user_country: User's country for location-aware prioritization
            
        Returns:
            List of matching cities as dictionaries
        """
        logger.debug(f"Searching cities with query: {query}, limit: {limit}, country: {country}")
        return self.city_data.search_cities(
            query=query,
            limit=limit,
            country=country,
            user_lat=user_lat,
            user_lng=user_lng,
            user_country=user_country
        )
    
    def get_city(self, city_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a city by its ID.
        
        Args:
            city_id: The ID of the city to fetch
            
        Returns:
            City details as a dictionary or None if not found
        """
        logger.debug(f"Getting city with ID: {city_id}")
        return self.city_data.get_city(city_id=city_id)
    
    def get_cities_by_coordinates(
        self, 
        lat: float, 
        lng: float, 
        radius_km: float = 10
    ) -> List[Dict[str, Any]]:
        """
        Find cities within a given radius from the specified coordinates.
        
        Args:
            lat: Latitude of the center point
            lng: Longitude of the center point
            radius_km: Search radius in kilometers (default: 10)
            
        Returns:
            List of cities within the radius, ordered by distance
        """
        logger.debug(f"Finding cities near coordinates: ({lat}, {lng}) within {radius_km} km")
        return self.city_data.get_cities_by_coordinates(
            lat=lat,
            lng=lng,
            radius_km=radius_km
        )
    
    def get_countries(self) -> List[str]:
        """
        Get a list of all countries.
        
        Returns:
            List of country names, sorted alphabetically
        """
        logger.debug("Getting list of all countries")
        return self.city_data.get_countries()
    
    def get_states(self, country: str) -> List[str]:
        """
        Get a list of states in a country.
        
        Args:
            country: Country name
            
        Returns:
            List of state names, sorted alphabetically
        """
        logger.debug(f"Getting states in country: {country}")
        return self.city_data.get_states(country=country)
    
    def get_cities_in_state(self, state: str, country: str) -> List[Dict[str, Any]]:
        """
        Get a list of cities in a state.
        
        Args:
            state: State name
            country: Country name
            
        Returns:
            List of cities in the state, sorted by name
        """
        logger.debug(f"Getting cities in state: {state}, country: {country}")
        return self.city_data.get_cities_in_state(state=state, country=country)
    
    def get_table_info(self) -> Dict[str, Any]:
        """
        Get information about the city_data table.
        
        Returns:
            Dictionary with table information including columns and row count
        """
        logger.debug("Getting city_data table information")
        return self.city_data.get_table_info()
    
    def import_city_data(self, csv_path: Optional[str] = None, batch_size: int = 5000) -> bool:
        """
        Import city data from a CSV file.
        
        Args:
            csv_path: Path to the CSV file to import. If None, attempts to find a default file.
            batch_size: Number of records to import at once.
            
        Returns:
            True if the import was successful, False otherwise.
        """
        logger.debug(f"Importing city data from CSV: {csv_path if csv_path else 'default'}")
        return self.city_data.import_city_data(csv_path=csv_path, batch_size=batch_size)
    
    def close(self) -> None:
        """
        Close the database connection.
        
        This should be called when the service is no longer needed.
        """
        if hasattr(self, 'city_data'):
            self.city_data.close()
    
    def __enter__(self):
        """Context manager entry point."""
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit point."""
        self.close() 