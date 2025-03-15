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

# For fuzzy matching support (rapidfuzz is significantly faster than fuzzywuzzy)
try:
    from rapidfuzz import fuzz, process
    USING_RAPIDFUZZ = True
except ImportError:
    from fuzzywuzzy import fuzz, process
    USING_RAPIDFUZZ = False
    logging.warning("rapidfuzz not found, using slower fuzzywuzzy. Install rapidfuzz for better performance.")

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
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize the repository with a database manager and load city data into memory.
        
        Args:
            db_manager: The database manager to use for database operations
        """
        super().__init__(db_manager)
        
        # City name lookup structures
        self.city_index = {}  # Map of city_id to city data
        self.city_names = {}  # Map of lowercase city name to list of city IDs
        self.ascii_names = {} # Map of lowercase ASCII name to list of city IDs
        self.country_cities = {} # Map of country to list of city IDs
        
        # Load cities into memory for fast searching
        self._load_cities()
        
    def _load_cities(self):
        """Load all cities into memory for fast fuzzy searching."""
        logger.info("Loading all cities into memory for fast search...")
        try:
            with self.db_manager.cursor() as cursor:
                cursor.execute("""
                    SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng
                    FROM city_data
                """)
                
                columns = ['id', 'name', 'ascii_name', 'country', 'country_code', 
                          'state', 'state_code', 'lat', 'lng']
                
                for row in cursor.fetchall():
                    city = self._row_to_dict(row, columns)
                    city_id = city['id']
                    name_lower = city['name'].lower()
                    ascii_lower = city['ascii_name'].lower()
                    country = city['country'].lower()
                    
                    # Store city data by ID
                    self.city_index[city_id] = city
                    
                    # Add to name lookup
                    if name_lower not in self.city_names:
                        self.city_names[name_lower] = []
                    self.city_names[name_lower].append(city_id)
                    
                    # Add to ASCII name lookup
                    if ascii_lower not in self.ascii_names:
                        self.ascii_names[ascii_lower] = []
                    self.ascii_names[ascii_lower].append(city_id)
                    
                    # Add to country lookup
                    if country not in self.country_cities:
                        self.country_cities[country] = []
                    self.country_cities[country].append(city_id)
                
                logger.info(f"Loaded {len(self.city_index)} cities into memory")
        except Exception as e:
            logger.error(f"Error loading cities: {str(e)}", exc_info=True)
    
    @lru_cache(maxsize=1000)
    def get_by_id(self, city_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a city by its ID.
        
        Args:
            city_id: The ID of the city to fetch
            
        Returns:
            City details as a dictionary or None if not found
        """
        # First try to get from memory
        if city_id in self.city_index:
            return self.city_index[city_id].copy()
        
        # Fall back to database query if not in memory
        try:
            with self.db_manager.cursor() as cursor:
                if self.db_manager.db_type == 'sqlite':
                    cursor.execute("""
                        SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng
                        FROM city_data
                        WHERE id = ?
                    """, (city_id,))
                else:  # PostgreSQL
                    cursor.execute("""
                        SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng
                        FROM city_data
                        WHERE id = %s
                    """, (city_id,))
                
                row = cursor.fetchone()
                
                if row:
                    columns = ['id', 'name', 'ascii_name', 'country', 'country_code', 
                              'state', 'state_code', 'lat', 'lng']
                    return self._row_to_dict(row, columns)
                return None
                
        except Exception as e:
            logger.error(f"Error getting city by ID: {str(e)}")
            return None
    
    @lru_cache(maxsize=5000)
    def search(
        self, 
        query: str, 
        limit: int = 10, 
        country: Optional[str] = None,
        user_lat: Optional[float] = None,
        user_lng: Optional[float] = None,
        user_country: Optional[str] = None,
        fuzzy_threshold: int = 70
    ) -> List[Dict[str, Any]]:
        """
        Search for cities by name with autocomplete, fuzzy matching, and location-aware support.
        
        Args:
            query: The search query (city name prefix)
            limit: Maximum number of results to return (default: 10)
            country: Optional country filter (restricts results to this country)
            user_lat: User's latitude for location-aware prioritization
            user_lng: User's longitude for location-aware prioritization
            user_country: User's country for location-aware prioritization
            fuzzy_threshold: Minimum similarity score for fuzzy matching (0-100)
            
        Returns:
            List of matching cities as dictionaries with city details,
            prioritized by proximity to user's location when provided
        """
        if not query:
            return []
            
        try:
            query = query.strip().lower()
            logger.info(f"Searching for cities with query: '{query}', country: {country}, user_country: {user_country}")
            
            # Get city IDs to search (filtered by country if provided)
            city_ids_to_search = []
            if country:
                country_lower = country.lower()
                if country_lower in self.country_cities:
                    city_ids_to_search = self.country_cities[country_lower]
                else:
                    return []  # Country not found
            
            # If the query is an exact match for a city name, prioritize it
            exact_match_ids = []
            if query in self.city_names:
                exact_match_ids.extend(self.city_names[query])
            if query in self.ascii_names:
                exact_match_ids.extend(self.ascii_names[query])
            
            # Ensure we only have unique IDs
            exact_match_ids = list(set(exact_match_ids))
            
            # Filter by country if needed
            if country and exact_match_ids:
                country_lower = country.lower()
                exact_match_ids = [city_id for city_id in exact_match_ids 
                                 if self.city_index[city_id]['country'].lower() == country_lower]
            
            # Prefix matches (cities that start with the query)
            prefix_match_ids = []
            
            # Check each city name and ASCII name for prefix match
            for name, ids in self.city_names.items():
                if name.startswith(query) and (not country or any(self.city_index[city_id]['country'].lower() == country.lower() for city_id in ids)):
                    prefix_match_ids.extend(ids)
            
            for name, ids in self.ascii_names.items():
                if name.startswith(query) and (not country or any(self.city_index[city_id]['country'].lower() == country.lower() for city_id in ids)):
                    prefix_match_ids.extend(ids)
            
            # Deduplicate and remove exact matches
            prefix_match_ids = list(set([city_id for city_id in prefix_match_ids if city_id not in exact_match_ids]))
            
            # Apply fuzzy matching if needed
            fuzzy_match_results = []
            
            # Only do expensive fuzzy matching if we don't have enough exact/prefix matches
            if len(exact_match_ids) + len(prefix_match_ids) < limit * 2 and len(query) > 2:
                # Candidates to search - either country-filtered or all names
                search_names = []
                
                if country:
                    # Get all city names for the specified country
                    country_lower = country.lower()
                    for city_id in self.country_cities.get(country_lower, []):
                        city = self.city_index[city_id]
                        search_names.append((city['name'].lower(), city_id))
                        search_names.append((city['ascii_name'].lower(), city_id))
                else:
                    # Use all names (could be expensive for large datasets)
                    for name, ids in self.city_names.items():
                        for city_id in ids:
                            search_names.append((name, city_id))
                    
                    for name, ids in self.ascii_names.items():
                        for city_id in ids:
                            search_names.append((name, city_id))
                
                # Remove duplicates
                search_names = list(set(search_names))
                
                # Extract just the names for fuzzy matching
                names_only = [name for name, _ in search_names]
                
                # Perform fuzzy matching using process.extract
                fuzzy_matches = process.extract(
                    query, 
                    names_only, 
                    limit=min(100, len(names_only)), 
                    scorer=fuzz.token_set_ratio,
                    score_cutoff=fuzzy_threshold
                )
                
                # Convert fuzzy match results to city IDs with scores
                for matched_name, score, idx in fuzzy_matches:
                    city_id = search_names[idx][1]
                    # Skip cities already in exact or prefix matches
                    if city_id in exact_match_ids or city_id in prefix_match_ids:
                        continue
                    
                    city = self.city_index[city_id].copy()
                    city['fuzzy_score'] = score
                    fuzzy_match_results.append(city)
            
            # Combine results
            results = []
            
            # Add exact matches first
            for city_id in exact_match_ids:
                city = self.city_index[city_id].copy()
                city['match_type'] = 'exact'
                results.append(city)
            
            # Add prefix matches
            for city_id in prefix_match_ids:
                city = self.city_index[city_id].copy()
                city['match_type'] = 'prefix'
                results.append(city)
            
            # Add fuzzy matches
            for city in fuzzy_match_results:
                city['match_type'] = 'fuzzy'
                results.append(city)
            
            # Apply location-based prioritization
            if (user_lat is not None and user_lng is not None) or user_country is not None:
                # Flag to check if we need to sort by multiple criteria
                has_geo_sort = user_lat is not None and user_lng is not None
                has_country_sort = user_country is not None
                
                logger.info(f"Applying location prioritization to {len(results)} results")
                
                # Function to calculate city score based on location
                def city_score(city):
                    score = 0
                    
                    # Scoring by match type
                    if city.get('match_type') == 'exact':
                        score += 100000
                    elif city.get('match_type') == 'prefix':
                        score += 50000
                    
                    # Add fuzzy match score if present
                    if 'fuzzy_score' in city:
                        # Scale the fuzzy score and apply higher weight
                        fuzzy_value = city['fuzzy_score'] * 200
                        
                        # Extra boost for high fuzzy scores (> 80)
                        if city['fuzzy_score'] > 80:
                            fuzzy_value *= 1.5
                        
                        score += fuzzy_value
                    
                    # Country match
                    if has_country_sort and city['country'].lower() == user_country.lower():
                        score += 25000
                    
                    # Distance to user
                    if has_geo_sort:
                        # Calculate distance using Haversine formula
                        city_lat = city['lat']
                        city_lng = city['lng']
                        distance = self._haversine(user_lat, user_lng, city_lat, city_lng)
                        
                        # Convert distance to a score (closer = higher score)
                        distance_score = 50000 / (1 + (distance / 50))
                        score += distance_score
                        
                        # Store distance for reference
                        city['distance_km'] = distance
                    
                    return -score  # Negative for descending sort (higher score = better match)
                
                # Sort the results by the combined score
                results.sort(key=city_score)
            else:
                # Simple sorting by match type if no location info
                def simple_score(city):
                    if city.get('match_type') == 'exact':
                        base_score = 1000
                    elif city.get('match_type') == 'prefix':
                        base_score = 500
                    else:
                        base_score = 0
                    
                    # Add fuzzy score if present
                    fuzzy_score = city.get('fuzzy_score', 0)
                    return -(base_score + fuzzy_score)  # Negative for descending sort
                
                results.sort(key=simple_score)
            
            # Clean up before returning
            for city in results:
                city.pop('match_type', None)
                city.pop('fuzzy_score', None)
            
                return results[:limit]
        
        except Exception as e:
            logger.error(f"Error searching cities: {str(e)}", exc_info=True)
            return []

    def _haversine(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        
        Args:
            lat1: Latitude of point 1
            lon1: Longitude of point 1
            lat2: Latitude of point 2
            lon2: Longitude of point 2
            
        Returns:
            Distance in kilometers between the points
        """
        # Convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
        
        # Haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        r = 6371  # Radius of earth in kilometers
        
        return c * r

class GeoRepository(BaseRepository):
    """
    Repository for geographic queries such as finding cities by coordinates.
    """
    
    def find_by_coordinates(self, lat: float, lng: float, radius_km: float = 10) -> List[Dict[str, Any]]:
        """
        Find cities within a given radius of coordinates.
        
        Args:
            lat: Latitude of the center point
            lng: Longitude of the center point
            radius_km: Radius in kilometers
            
        Returns:
            List of cities within the radius, sorted by distance
        """
        # Validate inputs
        if not (-90 <= lat <= 90):
            raise ValueError(f"Latitude must be between -90 and 90, got {lat}")
        if not (-180 <= lng <= 180):
            raise ValueError(f"Longitude must be between -180 and 180, got {lng}")
        if radius_km <= 0:
            raise ValueError(f"Radius must be positive, got {radius_km}")
            
        # Try to use PostGIS if available for PostgreSQL
        if self.db_manager.db_type == 'postgresql':
            try:
                with self.db_manager.cursor() as cursor:
                    # Check if PostGIS is available and enabled
                    cursor.execute("SELECT PostGIS_version()")
                    postgis_version = cursor.fetchone()
                    
                    if postgis_version:
                        # Use PostGIS for optimal spatial search
                        cursor.execute("""
                            SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng,
                                   ST_Distance(
                                       ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                                       ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography
                                   ) as distance
                            FROM city_data
                            WHERE ST_DWithin(
                                ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography,
                                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                                %s
                            )
                            ORDER BY distance
                        """, (lng, lat, lng, lat, radius_km * 1000))
                        
                        rows = cursor.fetchall()
                        columns = ['id', 'name', 'ascii_name', 'country', 'country_code', 
                                  'state', 'state_code', 'lat', 'lng', 'distance']
                        return self._rows_to_dicts(rows, columns)
            except Exception as e:
                logger.warning(f"PostGIS query failed: {str(e)}. Falling back to Haversine.")
        
        # Fall back to Haversine formula for distance calculation
        # This works for any database type
        return self._find_by_haversine(lat, lng, radius_km)
    
    def _find_by_haversine(self, lat: float, lng: float, radius_km: float) -> List[Dict[str, Any]]:
        """
        Find cities within a radius using the Haversine formula.
        
        Args:
            lat: Latitude of the center point
            lng: Longitude of the center point
            radius_km: Radius in kilometers
            
        Returns:
            List of cities within the radius, sorted by distance
        """
        try:
            with self.db_manager.cursor() as cursor:
                # Get all city data
                cursor.execute("""
                    SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng
                    FROM city_data
                """)
                
                rows = cursor.fetchall()
                columns = ['id', 'name', 'ascii_name', 'country', 'country_code', 
                          'state', 'state_code', 'lat', 'lng']
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
        except Exception as e:
            logger.error(f"Error getting cities by coordinates: {str(e)}")
            return []
    
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
        Get all cities in a state.
        
        Args:
            state: State name
            country: Country name
            
        Returns:
            List of cities in the state, sorted by name
        """
        with self.db_manager.cursor() as cursor:
            # Enforce case-insensitive matching
            state_lower = state.lower()
            country_lower = country.lower()
            
            if self.db_manager.db_type == 'sqlite':
                cursor.execute("""
                    SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng
                    FROM city_data
                    WHERE LOWER(state) = ? AND LOWER(country) = ?
                    ORDER BY name
                """, (state_lower, country_lower))
            else:  # PostgreSQL
                cursor.execute("""
                    SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng
                    FROM city_data
                    WHERE LOWER(state) = %s AND LOWER(country) = %s
                    ORDER BY name
                """, (state_lower, country_lower))
            
            rows = cursor.fetchall()
            columns = ['id', 'name', 'ascii_name', 'country', 'country_code', 
                      'state', 'state_code', 'lat', 'lng']
            return self._rows_to_dicts(rows, columns) 