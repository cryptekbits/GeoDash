"""
Repository module for the GeoDash package.

This module provides repository classes for accessing and querying city data
in the GeoDash database.
"""

import logging
import math
from typing import Dict, List, Any, Tuple, Optional
from functools import lru_cache

from GeoDash.data.database import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BaseRepository:
    """
    Base repository class for city data.
    
    This class provides common functionality for all city data repositories.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize the repository with a database manager.
        
        Args:
            db_manager: The database manager to use for database operations
        """
        self.db_manager = db_manager
        self.table_name = 'city_data'
    
    def _row_to_dict(self, row: Tuple, columns: List[str]) -> Dict[str, Any]:
        """
        Convert a database row to a dictionary using column names.
        
        Args:
            row: The database row tuple
            columns: List of column names
            
        Returns:
            Dictionary representation of the row
        """
        return dict(zip(columns, row))
    
    def _rows_to_dicts(self, rows: List[Tuple], columns: List[str]) -> List[Dict[str, Any]]:
        """
        Convert multiple database rows to dictionaries.
        
        Args:
            rows: List of database row tuples
            columns: List of column names
            
        Returns:
            List of dictionary representations of the rows
        """
        return [self._row_to_dict(row, columns) for row in rows]

class CityRepository(BaseRepository):
    """
    Repository for city lookup by ID and search operations.
    """
    
    @lru_cache(maxsize=1000)
    def get_by_id(self, city_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a city by its ID.
        
        Args:
            city_id: The ID of the city to fetch
            
        Returns:
            City details as a dictionary or None if not found
        """
        try:
            with self.db_manager.cursor() as cursor:
                if self.db_manager.db_type == 'sqlite':
                    cursor.execute("""
                        SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng, population
                        FROM city_data
                        WHERE id = ?
                    """, (city_id,))
                else:  # PostgreSQL
                    cursor.execute("""
                        SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng, population
                        FROM city_data
                        WHERE id = %s
                    """, (city_id,))
                
                row = cursor.fetchone()
                
                if row:
                    columns = ['id', 'name', 'ascii_name', 'country', 'country_code', 
                              'state', 'state_code', 'lat', 'lng', 'population']
                    return self._row_to_dict(row, columns)
                return None
                
        except Exception as e:
            logger.error(f"Error getting city by ID: {str(e)}")
            return None
    
    @lru_cache(maxsize=5000)
    def search(self, query: str, limit: int = 10, country: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Search for cities by name with autocomplete support.
        
        Args:
            query: The search query (city name prefix)
            limit: Maximum number of results to return (default: 10)
            country: Optional country filter
            
        Returns:
            List of matching cities as dictionaries with city details
        """
        if not query:
            return []
            
        try:
            query = query.strip().lower()
            
            with self.db_manager.cursor() as cursor:
                columns = ['id', 'name', 'ascii_name', 'country', 'country_code', 
                          'state', 'state_code', 'lat', 'lng', 'population']
                
                if self.db_manager.db_type == 'sqlite':
                    # Check if FTS5 is available and the city_search table exists
                    has_fts = False
                    try:
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='city_search'")
                        has_fts = bool(cursor.fetchone())
                    except Exception:
                        has_fts = False
                    
                    if has_fts:
                        # Use FTS5 for full-text search
                        # Build the search query
                        search_term = f"{query}*"  # Add wildcard for prefix matching
                        
                        if country:
                            # Use FTS with country filter
                            cursor.execute(f"""
                                SELECT c.id, c.name, c.ascii_name, c.country, c.country_code, 
                                       c.state, c.state_code, c.lat, c.lng, c.population
                                FROM city_data c
                                JOIN city_search s ON c.id = s.rowid
                                WHERE city_search MATCH ? AND LOWER(c.country) = ?
                                ORDER BY rank, c.population DESC NULLS LAST
                                LIMIT ?
                            """, (search_term, country.lower(), limit))
                        else:
                            # Use FTS without country filter
                            cursor.execute(f"""
                                SELECT c.id, c.name, c.ascii_name, c.country, c.country_code, 
                                       c.state, c.state_code, c.lat, c.lng, c.population
                                FROM city_data c
                                JOIN city_search s ON c.id = s.rowid
                                WHERE city_search MATCH ?
                                ORDER BY rank, c.population DESC NULLS LAST
                                LIMIT ?
                            """, (search_term, limit))
                    else:
                        # Fall back to LIKE queries if FTS is not available
                        if country:
                            cursor.execute("""
                                SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng, population
                                FROM city_data
                                WHERE (LOWER(ascii_name) LIKE ? OR LOWER(name) LIKE ?) AND LOWER(country) = ?
                                ORDER BY 
                                    CASE WHEN LOWER(ascii_name) LIKE ? THEN 1
                                         WHEN LOWER(name) LIKE ? THEN 1
                                         ELSE 2 END,
                                    population DESC NULLS LAST
                                LIMIT ?
                            """, (f"{query}%", f"{query}%", country.lower(), f"{query}%", f"{query}%", limit))
                        else:
                            cursor.execute("""
                                SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng, population
                                FROM city_data
                                WHERE LOWER(ascii_name) LIKE ? OR LOWER(name) LIKE ?
                                ORDER BY 
                                    CASE WHEN LOWER(ascii_name) LIKE ? THEN 1
                                         WHEN LOWER(name) LIKE ? THEN 1
                                         ELSE 2 END,
                                    population DESC NULLS LAST
                                LIMIT ?
                            """, (f"{query}%", f"{query}%", f"{query}%", f"{query}%", limit))
                else:  # PostgreSQL
                    # Check if the tsvector column exists
                    has_tsvector = False
                    try:
                        cursor.execute("""
                            SELECT column_name FROM information_schema.columns 
                            WHERE table_name = 'city_data' AND column_name = 'search_vector'
                        """)
                        has_tsvector = bool(cursor.fetchone())
                    except Exception:
                        has_tsvector = False
                    
                    if has_tsvector:
                        # Use the tsvector column for full-text search
                        # Create tsquery for the search term
                        # Convert query to tsquery format with prefix matching
                        tsquery = f"{query}:*"
                        
                        if country:
                            cursor.execute("""
                                SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng, population,
                                       ts_rank(search_vector, to_tsquery('english', %s)) as rank
                                FROM city_data
                                WHERE search_vector @@ to_tsquery('english', %s) AND LOWER(country) = %s
                                ORDER BY rank DESC, population DESC NULLS LAST
                                LIMIT %s
                            """, (tsquery, tsquery, country.lower(), limit))
                        else:
                            cursor.execute("""
                                SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng, population,
                                       ts_rank(search_vector, to_tsquery('english', %s)) as rank
                                FROM city_data
                                WHERE search_vector @@ to_tsquery('english', %s)
                                ORDER BY rank DESC, population DESC NULLS LAST
                                LIMIT %s
                            """, (tsquery, tsquery, limit))
                        
                        # Add rank to columns and then remove it from results
                        temp_columns = columns + ['rank']
                        rows = cursor.fetchall()
                        result = self._rows_to_dicts(rows, temp_columns)
                        
                        # Remove the rank field from the results
                        for city in result:
                            city.pop('rank', None)
                        
                        return result
                    else:
                        # Fall back to LIKE queries if tsvector is not available
                        if country:
                            cursor.execute("""
                                SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng, population
                                FROM city_data
                                WHERE (LOWER(ascii_name) LIKE %s OR LOWER(name) LIKE %s) AND LOWER(country) = %s
                                ORDER BY 
                                    CASE WHEN LOWER(ascii_name) LIKE %s THEN 1
                                         WHEN LOWER(name) LIKE %s THEN 1
                                         ELSE 2 END,
                                    population DESC NULLS LAST
                                LIMIT %s
                            """, (f"{query}%", f"{query}%", country.lower(), f"{query}%", f"{query}%", limit))
                        else:
                            cursor.execute("""
                                SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng, population
                                FROM city_data
                                WHERE LOWER(ascii_name) LIKE %s OR LOWER(name) LIKE %s
                                ORDER BY 
                                    CASE WHEN LOWER(ascii_name) LIKE %s THEN 1
                                         WHEN LOWER(name) LIKE %s THEN 1
                                         ELSE 2 END,
                                    population DESC NULLS LAST
                                LIMIT %s
                            """, (f"{query}%", f"{query}%", f"{query}%", f"{query}%", limit))
                
                rows = cursor.fetchall()
                return self._rows_to_dicts(rows, columns)
        
        except Exception as e:
            logger.error(f"Error searching cities: {str(e)}")
            return []

class GeoRepository(BaseRepository):
    """
    Repository for geographic queries such as finding cities by coordinates.
    """
    
    def find_by_coordinates(self, lat: float, lng: float, radius_km: float = 10) -> List[Dict[str, Any]]:
        """
        Find cities within a given radius from the specified coordinates.
        
        Args:
            lat: Latitude of the center point
            lng: Longitude of the center point
            radius_km: Search radius in kilometers (default: 10)
            
        Returns:
            List of cities within the radius, ordered by distance
        """
        try:
            # Validate inputs
            if not isinstance(lat, (int, float)) or not isinstance(lng, (int, float)):
                logger.error(f"Invalid coordinates: lat={lat}, lng={lng}")
                return []
                
            if not -90 <= lat <= 90 or not -180 <= lng <= 180:
                logger.error(f"Coordinates out of range: lat={lat}, lng={lng}")
                return []
                
            if radius_km <= 0:
                logger.error(f"Invalid radius: {radius_km}")
                return []
                
            # Try to use PostGIS for PostgreSQL
            if self.db_manager.db_type == 'postgresql':
                try:
                    with self.db_manager.cursor() as cursor:
                        # Check if PostGIS is available
                        cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'postgis'")
                        if cursor.fetchone():
                            # Use PostGIS for efficient spatial query
                            cursor.execute("""
                                SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng, population,
                                       ST_Distance(
                                           geom::geography,
                                           ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                                       ) as distance
                                FROM city_data
                                WHERE ST_DWithin(
                                    geom::geography,
                                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                                    %s * 1000
                                )
                                ORDER BY distance
                            """, (lng, lat, lng, lat, radius_km))
                            
                            columns = ['id', 'name', 'ascii_name', 'country', 'country_code', 
                                      'state', 'state_code', 'lat', 'lng', 'population', 'distance']
                            rows = cursor.fetchall()
                            cities = self._rows_to_dicts(rows, columns)
                            
                            # Convert distance to kilometers
                            for city in cities:
                                city['distance_km'] = city.pop('distance') / 1000
                                
                            return cities
                except Exception as e:
                    logger.warning(f"PostGIS spatial query failed, falling back to Haversine: {str(e)}")
            
            # Fall back to Haversine formula
            return self._find_by_haversine(lat, lng, radius_km)
            
        except Exception as e:
            logger.error(f"Error getting cities by coordinates: {str(e)}")
            return []
    
    def _find_by_haversine(self, lat: float, lng: float, radius_km: float) -> List[Dict[str, Any]]:
        """
        Find cities within a radius using the Haversine formula.
        
        Args:
            lat: Latitude of the center point
            lng: Longitude of the center point
            radius_km: Search radius in kilometers
            
        Returns:
            List of cities within the radius, ordered by distance
        """
        with self.db_manager.cursor() as cursor:
            # Get all cities from the database
            cursor.execute("""
                SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng, population
                FROM city_data
            """)
            
            columns = ['id', 'name', 'ascii_name', 'country', 'country_code', 
                      'state', 'state_code', 'lat', 'lng', 'population']
            rows = cursor.fetchall()
            all_cities = self._rows_to_dicts(rows, columns)
            
            # Calculate distances using Haversine formula
            cities_with_distance = []
            for city in all_cities:
                city_lat = city['lat']
                city_lng = city['lng']
                distance = self._haversine(lat, lng, city_lat, city_lng)
                
                if distance <= radius_km:
                    city['distance_km'] = distance
                    cities_with_distance.append(city)
            
            # Sort by distance
            return sorted(cities_with_distance, key=lambda x: x['distance_km'])
    
    def _haversine(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate the great-circle distance between two points on the Earth.
        
        Args:
            lat1: Latitude of first point in degrees
            lon1: Longitude of first point in degrees
            lat2: Latitude of second point in degrees
            lon2: Longitude of second point in degrees
            
        Returns:
            Distance in kilometers
        """
        # Convert decimal degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        r = 6371  # Radius of Earth in kilometers
        
        return c * r

class RegionRepository(BaseRepository):
    """
    Repository for hierarchical region queries (countries, states, cities in states).
    """
    
    @lru_cache(maxsize=1)
    def get_countries(self) -> List[str]:
        """
        Get a list of all countries.
        
        Returns:
            List of country names, sorted alphabetically
        """
        try:
            with self.db_manager.cursor() as cursor:
                cursor.execute("SELECT DISTINCT country FROM city_data ORDER BY country")
                
                return [row[0] for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting countries: {str(e)}")
            return []
    
    @lru_cache(maxsize=100)
    def get_states(self, country: str) -> List[str]:
        """
        Get a list of states in a country.
        
        Args:
            country: Country name
            
        Returns:
            List of state names, sorted alphabetically
        """
        try:
            with self.db_manager.cursor() as cursor:
                if self.db_manager.db_type == 'sqlite':
                    cursor.execute("""
                        SELECT DISTINCT state 
                        FROM city_data 
                        WHERE LOWER(country) = LOWER(?) AND state != ''
                        ORDER BY state
                    """, (country,))
                else:  # PostgreSQL
                    cursor.execute("""
                        SELECT DISTINCT state 
                        FROM city_data 
                        WHERE LOWER(country) = LOWER(%s) AND state != ''
                        ORDER BY state
                    """, (country,))
                
                return [row[0] for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting states for country {country}: {str(e)}")
            return []
    
    @lru_cache(maxsize=500)
    def get_cities_in_state(self, state: str, country: str) -> List[Dict[str, Any]]:
        """
        Get a list of cities in a state.
        
        Args:
            state: State name
            country: Country name
            
        Returns:
            List of cities in the state, sorted by population (descending)
        """
        try:
            with self.db_manager.cursor() as cursor:
                if self.db_manager.db_type == 'sqlite':
                    cursor.execute("""
                        SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng, population
                        FROM city_data
                        WHERE LOWER(state) = LOWER(?) AND LOWER(country) = LOWER(?)
                        ORDER BY population DESC NULLS LAST
                    """, (state, country))
                else:  # PostgreSQL
                    cursor.execute("""
                        SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng, population
                        FROM city_data
                        WHERE LOWER(state) = LOWER(%s) AND LOWER(country) = LOWER(%s)
                        ORDER BY population DESC NULLS LAST
                    """, (state, country))
                
                columns = ['id', 'name', 'ascii_name', 'country', 'country_code', 
                          'state', 'state_code', 'lat', 'lng', 'population']
                rows = cursor.fetchall()
                return self._rows_to_dicts(rows, columns)
                
        except Exception as e:
            logger.error(f"Error getting cities in state {state}, country {country}: {str(e)}")
            return [] 