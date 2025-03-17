"""
Repository module for the GeoDash package.

This module provides repository classes for accessing and querying city data
in the GeoDash database.
"""

import logging
import math
from typing import Dict, List, Any, Tuple, Optional, Union, ClassVar, Type, TypeVar, Set
from functools import lru_cache
import time
import asyncio
import os
import threading
import pickle
import sys
from multiprocessing import shared_memory, Lock
import tempfile
import atexit

from GeoDash.data.database import DatabaseManager

# For fuzzy matching support (rapidfuzz is significantly faster than fuzzywuzzy)
try:
    from rapidfuzz import fuzz, process
    USING_RAPIDFUZZ = True
except ImportError:
    from fuzzywuzzy import fuzz, process
    USING_RAPIDFUZZ = False
    logging.warning("rapidfuzz not found, using slower fuzzywuzzy. Install rapidfuzz for better performance.")

# For trie implementation
try:
    import pygtrie as trie
    USING_TRIE = True
except ImportError:
    USING_TRIE = False
    logging.warning("pygtrie not found, using slower dictionary lookups. Install pygtrie for better performance.")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Shared memory initialization lock
_init_lock = Lock()

# Shared memory names
_CITY_REPO_SHM_NAME = "geodash_city_repo_init"
_GEO_REPO_SHM_NAME = "geodash_geo_repo_init"
_REGION_REPO_SHM_NAME = "geodash_region_repo_init"

# Shared memory for actual repository data
_CITY_REPO_DATA_SHM_NAME = "geodash_city_repo_data"
_GEO_REPO_DATA_SHM_NAME = "geodash_geo_repo_data" 
_REGION_REPO_DATA_SHM_NAME = "geodash_region_repo_data"

# Process-local singleton instances (only created once per process)
_city_repository_instance = None
_geo_repository_instance = None
_region_repository_instance = None

# Estimated memory sizes for repository data (adjust based on your data size)
# These values are conservative estimates, increase if needed
_CITY_REPO_SIZE_BYTES = 200 * 1024 * 1024  # 200MB for city repository
_GEO_REPO_SIZE_BYTES = 10 * 1024 * 1024    # 10MB for geo repository
_REGION_REPO_SIZE_BYTES = 5 * 1024 * 1024  # 5MB for region repository

# Shared memory reference counting
_shm_reference_counts = {}
_shm_ref_lock = Lock()

def _create_or_get_shared_flag(name):
    """Creates or gets a shared memory flag for repository initialization."""
    try:
        # Try to attach to existing shared memory
        shm = shared_memory.SharedMemory(name=name)
        logger.debug(f"Attached to existing shared memory flag: {name}")
        _increment_shm_ref_count(name)
        BaseRepository.register_shared_memory(name, shm)
    except FileNotFoundError:
        # Create new shared memory if it doesn't exist
        try:
            with _init_lock:
                try:
                    # Double-check to avoid race condition
                    shm = shared_memory.SharedMemory(name=name)
                    logger.debug(f"Attached to existing shared memory flag in lock: {name}")
                    _increment_shm_ref_count(name)
                    BaseRepository.register_shared_memory(name, shm)
                except FileNotFoundError:
                    # Create the shared memory block (1 byte for flag)
                    shm = shared_memory.SharedMemory(name=name, create=True, size=1)
                    # Initialize to 0 (not initialized)
                    shm.buf[0] = 0
                    logger.debug(f"Created new shared memory flag: {name}")
                    _increment_shm_ref_count(name)
                    BaseRepository.register_shared_memory(name, shm)
        except Exception as e:
            logger.error(f"Error creating shared memory flag {name}: {str(e)}")
            # Fallback to a non-shared approach
            return None
    return shm

def _create_or_get_shared_data(name, size_bytes):
    """Creates or gets a shared memory block for repository data."""
    try:
        # Try to attach to existing shared memory
        shm = shared_memory.SharedMemory(name=name)
        logger.debug(f"Attached to existing shared memory data block: {name}")
        _increment_shm_ref_count(name)
        BaseRepository.register_shared_memory(name, shm)
        return shm, False  # Return with flag indicating not newly created
    except FileNotFoundError:
        # Create new shared memory if it doesn't exist
        try:
            with _init_lock:
                try:
                    # Double-check to avoid race condition
                    shm = shared_memory.SharedMemory(name=name)
                    logger.debug(f"Attached to existing shared memory data block in lock: {name}")
                    _increment_shm_ref_count(name)
                    BaseRepository.register_shared_memory(name, shm)
                    return shm, False  # Not newly created
                except FileNotFoundError:
                    # Create the shared memory block with specified size
                    shm = shared_memory.SharedMemory(name=name, create=True, size=size_bytes)
                    logger.debug(f"Created new shared memory data block: {name} with size {size_bytes}")
                    _increment_shm_ref_count(name)
                    BaseRepository.register_shared_memory(name, shm)
                    return shm, True  # Newly created
        except Exception as e:
            logger.error(f"Error creating shared memory data block {name}: {str(e)}")
            # Fallback to a non-shared approach
            return None, False
    
def _increment_shm_ref_count(name):
    """Increment reference count for a shared memory block."""
    with _shm_ref_lock:
        if name in _shm_reference_counts:
            _shm_reference_counts[name] += 1
        else:
            _shm_reference_counts[name] = 1
        logger.debug(f"Incremented reference count for {name} to {_shm_reference_counts[name]}")

def _decrement_shm_ref_count(name):
    """Decrement reference count for a shared memory block."""
    with _shm_ref_lock:
        if name in _shm_reference_counts:
            _shm_reference_counts[name] -= 1
            count = _shm_reference_counts[name]
            logger.debug(f"Decremented reference count for {name} to {count}")
            return count
        return 0

def _serialize_to_shared_memory(obj, shm):
    """Serialize an object and store it in shared memory."""
    try:
        # Pickle the object to bytes
        pickled_data = pickle.dumps(obj)
        data_size = len(pickled_data)
        
        # Check if it fits in the shared memory
        if data_size > shm.size:
            logger.error(f"Object size ({data_size} bytes) exceeds shared memory size ({shm.size} bytes)")
            return False
        
        # Store the data size at the beginning (first 8 bytes) for easy retrieval
        size_bytes = data_size.to_bytes(8, byteorder='little')
        shm.buf[:8] = size_bytes
        
        # Store the pickled data after the size
        shm.buf[8:8+data_size] = pickled_data
        logger.debug(f"Serialized object to shared memory ({data_size} bytes)")
        return True
    except Exception as e:
        logger.error(f"Error serializing to shared memory: {str(e)}")
        return False

def _deserialize_from_shared_memory(shm):
    """Deserialize an object from shared memory."""
    try:
        # Read the data size from the first 8 bytes
        data_size = int.from_bytes(shm.buf[:8], byteorder='little')
        
        # Check if the size is valid
        if data_size <= 0 or data_size > shm.size - 8:
            logger.error(f"Invalid data size in shared memory: {data_size}")
            return None
        
        # Read the pickled data after the size
        pickled_data = bytes(shm.buf[8:8+data_size])
        
        # Unpickle the object
        obj = pickle.loads(pickled_data)
        logger.debug(f"Deserialized object from shared memory ({data_size} bytes)")
        return obj
    except Exception as e:
        logger.error(f"Error deserializing from shared memory: {str(e)}")
        return None

def get_city_repository(db_manager: DatabaseManager) -> 'CityRepository':
    """
    Get the global CityRepository singleton instance.
    
    This ensures the city data is only loaded once into memory when the
    server starts, not for each request.
    
    Args:
        db_manager: Database manager to use if repository needs to be created
        
    Returns:
        The global CityRepository instance
    """
    global _city_repository_instance
    
    # If we already have a process-local instance, return it
    if _city_repository_instance is not None:
        return _city_repository_instance
    
    # Check if we're in a Gunicorn worker
    worker_id = os.environ.get('GUNICORN_WORKER_ID')
    
    # Get or create the shared initialization flag - this helps workers coordinate
    # so they don't all try to load data simultaneously
    init_shm = _create_or_get_shared_flag(_CITY_REPO_SHM_NAME)
    
    if init_shm is None:
        # Fallback to old behavior if shared memory fails
        logger.warning("Shared memory failed, falling back to process-local singleton")
        logger.info("Creating process-local CityRepository instance")
        _city_repository_instance = CityRepository(db_manager)
        return _city_repository_instance
    
    with _init_lock:
        # Check if another process has already initialized the repository in this run
        if init_shm.buf[0] == 0:
            # We're the first to initialize
            if worker_id:
                logger.info(f"Worker {worker_id}: First worker to initialize CityRepository")
            else: 
                logger.info("Creating global CityRepository singleton instance (first process)")
            
            # Create the repository - this loads all cities from DB
            start_time = time.time()
            repo = CityRepository(db_manager)
            load_time = time.time() - start_time
            
            if worker_id:
                logger.info(f"Worker {worker_id}: Loaded cities in {load_time:.2f}s")
            else:
                logger.info(f"Cities loaded in {load_time:.2f}s")
            
            # Mark as initialized for other processes
            init_shm.buf[0] = 1
            
            # Store our instance
            _city_repository_instance = repo
            
        else:
            # Another process already initialized it, we load our own copy
            # because it's more reliable than trying to share complex data structures
            if worker_id:
                logger.info(f"Worker {worker_id}: Loading CityRepository data (worker {init_shm.buf[0]} was first)")
            else:
                logger.info("Loading CityRepository instance (another process was first)")
            
            # Create our own repository instance
            start_time = time.time()
            _city_repository_instance = CityRepository(db_manager)
            load_time = time.time() - start_time
            
            if worker_id:
                logger.info(f"Worker {worker_id}: Loaded cities in {load_time:.2f}s")
            else:
                logger.info(f"Cities loaded in {load_time:.2f}s")
    
    # Clean up initialization flag
    init_shm.close()
    
    return _city_repository_instance

def get_geo_repository(db_manager: DatabaseManager) -> 'GeoRepository':
    """
    Get the global GeoRepository singleton instance.
    
    Args:
        db_manager: Database manager to use if repository needs to be created
        
    Returns:
        The global GeoRepository instance
    """
    global _geo_repository_instance
    
    # If we already have a process-local instance, return it
    if _geo_repository_instance is not None:
        return _geo_repository_instance
    
    # For the GeoRepository, we don't need to share data as it's primarily 
    # a wrapper for database queries. Just use the flag for coordination.
    init_shm = _create_or_get_shared_flag(_GEO_REPO_SHM_NAME)
    
    if init_shm is None:
        # Fallback to old behavior if shared memory fails
        logger.warning("Shared memory failed, falling back to process-local singleton")
        logger.info("Creating process-local GeoRepository instance")
        _geo_repository_instance = GeoRepository(db_manager)
        return _geo_repository_instance
    
    with _init_lock:
        # Check if another process has already initialized the repository
        if init_shm.buf[0] == 0:
            # We're the first to initialize
            logger.info("Creating global GeoRepository singleton instance (first process)")
            _geo_repository_instance = GeoRepository(db_manager)
            # Mark as initialized for other processes
            init_shm.buf[0] = 1
        else:
            # Another process already initialized it, create our local instance
            logger.info("Creating process-local copy of GeoRepository singleton instance")
            _geo_repository_instance = GeoRepository(db_manager)
    
    # Clean up shared memory when no longer needed
    init_shm.close()
    
    return _geo_repository_instance

def get_region_repository(db_manager: DatabaseManager) -> 'RegionRepository':
    """
    Get the global RegionRepository singleton instance.
    
    Args:
        db_manager: Database manager to use if repository needs to be created
        
    Returns:
        The global RegionRepository instance
    """
    global _region_repository_instance
    
    # If we already have a process-local instance, return it
    if _region_repository_instance is not None:
        return _region_repository_instance
    
    # Region repository also primarily wraps database queries and doesn't store
    # large amounts of data, so we also just coordinate initialization.
    init_shm = _create_or_get_shared_flag(_REGION_REPO_SHM_NAME)
    
    if init_shm is None:
        # Fallback to old behavior if shared memory fails
        logger.warning("Shared memory failed, falling back to process-local singleton")
        logger.info("Creating process-local RegionRepository instance")
        _region_repository_instance = RegionRepository(db_manager)
        return _region_repository_instance
    
    with _init_lock:
        # Check if another process has already initialized the repository
        if init_shm.buf[0] == 0:
            # We're the first to initialize
            logger.info("Creating global RegionRepository singleton instance (first process)")
            _region_repository_instance = RegionRepository(db_manager)
            # Mark as initialized for other processes
            init_shm.buf[0] = 1
        else:
            # Another process already initialized it, create our local instance
            logger.info("Creating process-local copy of RegionRepository singleton instance")
            _region_repository_instance = RegionRepository(db_manager)
    
    # Clean up shared memory when no longer needed
    init_shm.close()
    
    return _region_repository_instance

class BaseRepository:
    """
    Base repository class for city data.
    
    This class provides common functionality for all city data repositories.
    """
    
    # Shared memory management for all repositories
    _shared_memory_handles: ClassVar[Dict[str, Any]] = {}
    
    @classmethod
    def close_shared_memory(cls, name: str) -> bool:
        """Close a shared memory block safely."""
        try:
            if name in cls._shared_memory_handles:
                shm = cls._shared_memory_handles[name]
                shm.close()
                del cls._shared_memory_handles[name]
                logger.debug(f"Closed shared memory: {name}")
                return True
        except Exception as e:
            logger.error(f"Error closing shared memory {name}: {str(e)}")
        return False
    
    @classmethod
    def register_shared_memory(cls, name: str, shm: Any) -> None:
        """Register a shared memory handle for cleanup."""
        cls._shared_memory_handles[name] = shm
        logger.debug(f"Registered shared memory handle: {name}")
    
    @classmethod
    def cleanup_shared_memory(cls) -> None:
        """Cleanup shared memory blocks, considering reference counting."""
        logger.info("Beginning shared memory cleanup process")
        
        try:
            # Only the parent process should fully unlink the shared memory
            is_parent = os.getpid() == os.getppid()
            
            # Process flag shared memory blocks
            for name in [_CITY_REPO_SHM_NAME, _GEO_REPO_SHM_NAME, _REGION_REPO_SHM_NAME]:
                try:
                    # Always close our handle
                    cls.close_shared_memory(name)
                    
                    # Decrement reference count
                    ref_count = _decrement_shm_ref_count(name)
                    
                    # If we're the parent process or the last reference, unlink
                    if is_parent or ref_count <= 0:
                        try:
                            shm = shared_memory.SharedMemory(name=name)
                            shm.close()
                            shm.unlink()
                            logger.info(f"Cleaned up shared memory flag: {name}")
                        except FileNotFoundError:
                            logger.debug(f"Shared memory flag already removed: {name}")
                        except Exception as e:
                            logger.warning(f"Error unlinking shared memory flag {name}: {str(e)}")
                    else:
                        logger.debug(f"Not unlinking {name}, ref count: {ref_count}")
                except Exception as e:
                    logger.warning(f"Error during cleanup of shared memory flag {name}: {str(e)}")
            
            # Process data shared memory blocks
            for name in [_CITY_REPO_DATA_SHM_NAME, _GEO_REPO_DATA_SHM_NAME, _REGION_REPO_DATA_SHM_NAME]:
                try:
                    # Always close our handle
                    cls.close_shared_memory(name)
                    
                    # Decrement reference count
                    ref_count = _decrement_shm_ref_count(name)
                    
                    # If we're the parent process or the last reference, unlink
                    if is_parent or ref_count <= 0:
                        try:
                            shm = shared_memory.SharedMemory(name=name)
                            shm.close()
                            shm.unlink()
                            logger.info(f"Cleaned up shared memory data: {name}")
                        except FileNotFoundError:
                            logger.debug(f"Shared memory data already removed: {name}")
                        except Exception as e:
                            logger.warning(f"Error unlinking shared memory data {name}: {str(e)}")
                    else:
                        logger.debug(f"Not unlinking {name}, ref count: {ref_count}")
                except Exception as e:
                    logger.warning(f"Error during cleanup of shared memory data {name}: {str(e)}")
                    
            logger.info("Shared memory cleanup completed")
        except Exception as e:
            logger.error(f"Error in shared memory cleanup: {str(e)}")
    
    def __init__(self, db_manager: DatabaseManager) -> None:
        """
        Initialize the repository with a database manager.
        
        Args:
            db_manager: The database manager to use for database operations
        """
        self.db_manager = db_manager
        self.table_name = 'city_data'
    
    def __del__(self) -> None:
        """Destructor to ensure shared memory is cleaned up when the repository is garbage collected."""
        try:
            # Attempt to clean up shared memory resources
            self.__class__.cleanup_shared_memory()
            logger.debug(f"Repository cleanup on garbage collection for {self.__class__.__name__}")
        except Exception as e:
            # Avoid errors during garbage collection
            logger.debug(f"Error during repository cleanup on garbage collection: {str(e)}")
    
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
    
    def __init__(self, db_manager: DatabaseManager, initialize: bool = True) -> None:
        """
        Initialize the repository with a database manager and load city data into memory.
        
        Args:
            db_manager: The database manager to use for database operations
            initialize: Whether to load cities into memory (default: True)
        """
        super().__init__(db_manager)
        
        # City name lookup structures
        self.city_index: Dict[int, Dict[str, Any]] = {}  # Map of city_id to city data
        self.city_names: Dict[str, List[int]] = {}  # Map of lowercase city name to list of city IDs
        self.ascii_names: Dict[str, List[int]] = {} # Map of lowercase ASCII name to list of city IDs
        self.country_cities: Dict[str, List[int]] = {} # Map of country to list of city IDs
        
        # Trie data structures for efficient prefix matching
        if USING_TRIE:
            self.name_trie = trie.CharTrie()
            self.ascii_trie = trie.CharTrie()
        
        # Load cities into memory for fast searching
        if initialize:
            start_time = time.time()
            self._load_cities()
            logger.info(f"Cities loaded in {time.time() - start_time:.2f} seconds")
        
    def _load_cities(self):
        """Load all cities into memory for fast search."""
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
                    
                    # Add to tries if available
                    if USING_TRIE:
                        if name_lower not in self.name_trie:
                            self.name_trie[name_lower] = []
                        self.name_trie[name_lower].append(city_id)
                        
                        if ascii_lower not in self.ascii_trie:
                            self.ascii_trie[ascii_lower] = []
                        self.ascii_trie[ascii_lower].append(city_id)
                
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
    
    def _get_prefix_matches(self, query: str, country: Optional[str] = None) -> List[int]:
        """
        Get city IDs that match the prefix using the appropriate data structure.
        
        Args:
            query: The prefix to match
            country: Optional country filter
            
        Returns:
            List of city IDs that match the prefix
        """
        if not query:
            return []
            
        prefix_match_ids = []
        query = query.lower()
        
        if USING_TRIE:
            # Use trie for efficient prefix matching
            try:
                # Get all items with the prefix
                name_matches = self.name_trie.items(prefix=query)
                for _, ids in name_matches:
                    prefix_match_ids.extend(ids)
                    
                ascii_matches = self.ascii_trie.items(prefix=query)
                for _, ids in ascii_matches:
                    prefix_match_ids.extend(ids)
            except KeyError:
                # No matches found in the trie
                pass
        else:
            # Fall back to dictionary lookup
            for name, ids in self.city_names.items():
                if name.startswith(query):
                    prefix_match_ids.extend(ids)
            
            for name, ids in self.ascii_names.items():
                if name.startswith(query):
                    prefix_match_ids.extend(ids)
        
        # Filter by country if needed
        if country:
            country_lower = country.lower()
            prefix_match_ids = [city_id for city_id in prefix_match_ids 
                             if self.city_index[city_id]['country'].lower() == country_lower]
                
        return list(set(prefix_match_ids))
    
    def _perform_postgresql_search(
        self, 
        query: str, 
        limit: int = 10, 
        country: Optional[str] = None,
        user_lat: Optional[float] = None,
        user_lng: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """
        Perform a PostgreSQL-specific full-text search using the tsvector column.
        
        Args:
            query: The search query 
            limit: Maximum number of results to return
            country: Optional country filter
            user_lat: User's latitude for location-aware prioritization
            user_lng: User's longitude for location-aware prioritization
            
        Returns:
            List of matching cities as dictionaries
        """
        try:
            # Convert the query to a tsquery format safely
            # Split the query into words
            query_words = query.strip().lower().split()
            
            # Build the SQL query with parameters
            sql = f"""
                SELECT id, name, ascii_name, country, country_code, state, state_code, lat, lng,
                       ts_rank(search_vector, to_tsquery('english', %s)) AS rank
                FROM {self.db_manager.city_table_name}
                WHERE search_vector @@ to_tsquery('english', %s)
            """
            
            # Create the tsquery parameter (words connected by &)
            tsquery_param = ' & '.join(query_words) if query_words else ""
            params = [tsquery_param, tsquery_param]
            
            # Add country filter if specified
            if country:
                sql += " AND lower(country) = %s"
                params.append(country.lower())
                
            # Add location-based ranking if coordinates provided
            if user_lat is not None and user_lng is not None:
                # Incorporate distance into ranking using the <-> operator
                sql += """
                    ORDER BY 
                        rank * 0.7 + 
                        (1.0 / (1.0 + (point(lng, lat) <-> point(%s, %s)))) * 0.3
                    DESC
                """
                params.extend([user_lng, user_lat])
            else:
                # Order by rank only
                sql += " ORDER BY rank DESC"
                
            # Add limit
            sql += " LIMIT %s"
            params.append(limit)
            
            # Execute the query
            with self.db_manager.cursor() as cursor:
                cursor.execute(sql, params)
                columns = [desc[0] for desc in cursor.description]
                results = self._rows_to_dicts(cursor.fetchall(), columns)
                
                # Clean up result data
                for city in results:
                    city.pop('rank', None)
                    
                return results
                
        except Exception as e:
            logger.error(f"PostgreSQL full-text search error: {str(e)}", exc_info=True)
            # Fall back to regular search on error
            return []

    @lru_cache(maxsize=5000)
    def search(
        self, 
        query: str, 
        limit: int = 10, 
        country: Optional[str] = None,
        user_lat: Optional[float] = None,
        user_lng: Optional[float] = None,
        user_country: Optional[str] = None,
        fuzzy_threshold: int = 70  # Increased from 70 to 85 for better performance
    ) -> List[Dict[str, Any]]:
        """
        Search for cities by name with optional location-aware prioritization.
        
        This method searches for cities by name, optionally filtered by country and
        prioritized by proximity to the user's location if provided.
        
        Args:
            query: City name to search for (can be partial)
            limit: Maximum number of results to return
            country: Optional country to filter results by
            user_lat: User's latitude for location-based prioritization
            user_lng: User's longitude for location-based prioritization
            user_country: User's country for country-biased results
            fuzzy_threshold: Threshold for fuzzy matching (0-100)
            
        Returns:
            List of matching cities, ordered by relevance and optionally proximity
        """
        if not query:
            return []
            
        try:
            query = query.strip().lower()
            start_time = time.time()
            logger.info(f"Searching for cities with query: '{query}', country: {country}, user_country: {user_country}")
            
            # Use PostgreSQL full-text search if available
            if self.db_manager.db_type == 'postgresql':
                postgresql_results = self._perform_postgresql_search(
                    query, limit, country, user_lat, user_lng
                )
                
                # If we got PostgreSQL results, use them
                if postgresql_results:
                    elapsed = time.time() - start_time
                    logger.info(f"PostgreSQL full-text search completed in {elapsed*1000:.1f}ms")
                    
                    # Apply additional location-based prioritization if user_country is provided
                    if user_country and not (user_lat and user_lng):
                        postgresql_results = self._apply_location_prioritization(
                            postgresql_results, None, None, user_country
                        )
                        
                    return postgresql_results[:limit]

            # Fall back to in-memory search if PostgreSQL search returns no results
            # or is not available
            
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
            
            # Get prefix matches using the optimized method
            prefix_match_ids = self._get_prefix_matches(query, country)
            
            # Remove exact matches from prefix matches
            prefix_match_ids = [city_id for city_id in prefix_match_ids if city_id not in exact_match_ids]
            
            # Early return if we have enough exact and prefix matches
            exact_and_prefix_count = len(exact_match_ids) + len(prefix_match_ids)
            
            # If we already have at least 5 matches or the query is very short, skip fuzzy matching
            skip_fuzzy = exact_and_prefix_count >= 5 or len(query) <= 2
            
            # Check if we can return early (avoid expensive fuzzy matching)
            if skip_fuzzy and exact_and_prefix_count > 0:
                # Create result objects
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
                
                # Apply location-based prioritization if needed
                results = self._apply_location_prioritization(
                    results, user_lat, user_lng, user_country
                )
                
                # Clean up before returning
                for city in results:
                    city.pop('match_type', None)
                    city.pop('fuzzy_score', None)
                
                elapsed = time.time() - start_time
                logger.info(f"Search completed in {elapsed*1000:.1f}ms (skipped fuzzy matching)")
                return results[:limit]
            
            # Apply fuzzy matching if needed
            fuzzy_match_results = []
            
            # Only do expensive fuzzy matching if we need to and the query is meaningful
            if not skip_fuzzy and len(query) > 2:
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
                    score_cutoff=fuzzy_threshold  # Using higher threshold (was 70)
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
            results = self._apply_location_prioritization(
                results, user_lat, user_lng, user_country
            )
            
            # Clean up before returning
            for city in results:
                city.pop('match_type', None)
                city.pop('fuzzy_score', None)
            
            elapsed = time.time() - start_time
            logger.info(f"Search completed in {elapsed*1000:.1f}ms with {len(fuzzy_match_results)} fuzzy matches")
            
            return results[:limit]
        
        except Exception as e:
            logger.error(f"Error searching cities: {str(e)}", exc_info=True)
            return []
            
    def _apply_location_prioritization(
        self, 
        results: List[Dict[str, Any]], 
        user_lat: Optional[float] = None,
        user_lng: Optional[float] = None,
        user_country: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Apply location-based prioritization to the results.
        
        Args:
            results: List of city dictionaries to prioritize
            user_lat: User's latitude
            user_lng: User's longitude
            user_country: User's country
            
        Returns:
            Sorted list of cities
        """
        if not results:
            return []
            
        # Check if we need location-based sorting
        if (user_lat is not None and user_lng is not None) or user_country is not None:
            # Flag to check if we need to sort by multiple criteria
            has_geo_sort = user_lat is not None and user_lng is not None
            has_country_sort = user_country is not None
            
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
        
        return results

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

    async def search_async(
        self, 
        query: str, 
        limit: int = 10, 
        country: Optional[str] = None,
        user_lat: Optional[float] = None,
        user_lng: Optional[float] = None,
        user_country: Optional[str] = None,
        fuzzy_threshold: int = 85,
        callback = None
    ):
        """
        Asynchronous search for cities with tiered response.
        
        This method returns results in multiple tiers as they become available:
        1. First, exact and prefix matches (very fast, typically < 5ms)
        2. Then fuzzy matches if needed (slower, typically < 100ms)
        
        Args:
            query: The search query (city name prefix)
            limit: Maximum number of results to return
            country: Optional country filter
            user_lat: User's latitude for location-aware prioritization
            user_lng: User's longitude for location-aware prioritization
            user_country: User's country for location-aware prioritization
            fuzzy_threshold: Minimum similarity score for fuzzy matching
            callback: Function to call with results as they become available
            
        Returns:
            Coroutine that returns the final list of results
        """
        if not query:
            return []
        
        try:
            query = query.strip().lower()
            start_time = time.time()
            logger.info(f"Starting async search with query: '{query}'")
            
            # Use PostgreSQL full-text search if available
            if self.db_manager.db_type == 'postgresql':
                # Run in a thread pool since it involves database access
                loop = asyncio.get_event_loop()
                postgresql_results = await loop.run_in_executor(None, 
                    lambda: self._perform_postgresql_search(
                        query, limit, country, user_lat, user_lng
                    )
                )
                
                # If we got PostgreSQL results, use them
                if postgresql_results:
                    elapsed = time.time() - start_time
                    logger.info(f"PostgreSQL full-text search completed in {elapsed*1000:.1f}ms")
                    
                    # Apply additional location-based prioritization if user_country is provided
                    if user_country and not (user_lat and user_lng):
                        postgresql_results = self._apply_location_prioritization(
                            postgresql_results, None, None, user_country
                        )
                    
                    # Call the callback with results if provided
                    if callback:
                        await callback(postgresql_results[:limit])
                        
                    return postgresql_results[:limit]
            
            # Fall back to in-memory search if PostgreSQL search returns no results
            # or is not available
            
            # Get exact and prefix matches first (fast)
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
            
            # Get prefix matches
            prefix_match_ids = self._get_prefix_matches(query, country)
            
            # Remove exact matches from prefix matches
            prefix_match_ids = [city_id for city_id in prefix_match_ids if city_id not in exact_match_ids]
            
            # Prepare initial results (exact + prefix matches)
            initial_results = []
            
            # Add exact matches first
            for city_id in exact_match_ids:
                city = self.city_index[city_id].copy()
                city['match_type'] = 'exact'
                initial_results.append(city)
            
            # Add prefix matches
            for city_id in prefix_match_ids:
                city = self.city_index[city_id].copy()
                city['match_type'] = 'prefix'
                initial_results.append(city)
            
            # Apply location prioritization to initial results
            initial_results = self._apply_location_prioritization(
                initial_results, user_lat, user_lng, user_country
            )
            
            # Clean up before returning
            for city in initial_results:
                city.pop('match_type', None)
                city.pop('fuzzy_score', None)
            
            initial_result_count = len(initial_results)
            elapsed = time.time() - start_time
            logger.info(f"Initial results ready in {elapsed*1000:.1f}ms: {initial_result_count} matches")
            
            # If we have enough initial results or query is very short, skip fuzzy matching
            if initial_result_count >= limit or len(query) <= 2:
                logger.info("Skipping fuzzy matching - enough initial results")
                return initial_results[:limit]
            
            # Start fuzzy matching in the background if we need more results
            if callback:
                asyncio.create_task(self._perform_fuzzy_matching(
                    query, country, exact_match_ids, prefix_match_ids, 
                    user_lat, user_lng, user_country, fuzzy_threshold, 
                    limit, callback, start_time
                ))
            
            # Return the initial results immediately
            return initial_results[:limit]
            
        except Exception as e:
            logger.error(f"Error in async search: {str(e)}", exc_info=True)
            return []
    
    async def _perform_fuzzy_matching(
        self, query, country, exact_match_ids, prefix_match_ids,
        user_lat, user_lng, user_country, fuzzy_threshold, 
        limit, callback, start_time
    ):
        """
        Perform fuzzy matching asynchronously and call the callback with updated results.
        """
        try:
            # Perform fuzzy matching
            fuzzy_match_results = []
            
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
            
            # Run the CPU-intensive fuzzy matching in a thread pool
            def run_fuzzy_matching():
                return process.extract(
                    query, 
                    names_only, 
                    limit=min(100, len(names_only)), 
                    scorer=fuzz.token_set_ratio,
                    score_cutoff=fuzzy_threshold
                )
            
            # Run the fuzzy matching in a thread pool to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            fuzzy_matches = await loop.run_in_executor(None, run_fuzzy_matching)
            
            # Convert fuzzy match results to city IDs with scores
            for matched_name, score, idx in fuzzy_matches:
                city_id = search_names[idx][1]
                # Skip cities already in exact or prefix matches
                if city_id in exact_match_ids or city_id in prefix_match_ids:
                    continue
                
                city = self.city_index[city_id].copy()
                city['fuzzy_score'] = score
                city['match_type'] = 'fuzzy'
                fuzzy_match_results.append(city)
            
            # Combine all results
            combined_results = []
            
            # Add exact matches first
            for city_id in exact_match_ids:
                city = self.city_index[city_id].copy()
                city['match_type'] = 'exact'
                combined_results.append(city)
            
            # Add prefix matches
            for city_id in prefix_match_ids:
                city = self.city_index[city_id].copy()
                city['match_type'] = 'prefix'
                combined_results.append(city)
            
            # Add fuzzy matches
            combined_results.extend(fuzzy_match_results)
            
            # Apply location prioritization
            combined_results = self._apply_location_prioritization(
                combined_results, user_lat, user_lng, user_country
            )
            
            # Clean up before returning
            for city in combined_results:
                city.pop('match_type', None)
                city.pop('fuzzy_score', None)
            
            elapsed = time.time() - start_time
            logger.info(f"Fuzzy matching completed in {elapsed*1000:.1f}ms: {len(fuzzy_match_results)} fuzzy matches")
            
            # Call the callback with the final results
            callback(combined_results[:limit])
            
        except Exception as e:
            logger.error(f"Error in fuzzy matching: {str(e)}", exc_info=True)
            # Call the callback with the original results in case of error
            callback([])

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
                # For SQLite with R*Tree index, use a more efficient approach
                if self.db_manager.db_type == 'sqlite':
                    # Check if the R*Tree index exists
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='city_rtree'")
                    rtree_exists = cursor.fetchone()
                    
                    if rtree_exists:
                        # Calculate bounding box for the given radius
                        # Approximate degrees per km (1 degree of latitude ≈ 111.32 km)
                        # For longitude, 1 degree ≈ 111.32 * cos(latitude) km
                        lat_radius = radius_km / 111.32
                        lng_radius = radius_km / (111.32 * abs(math.cos(math.radians(lat))))
                        
                        min_lat = lat - lat_radius
                        max_lat = lat + lat_radius
                        min_lng = lng - lng_radius
                        max_lng = lng + lng_radius
                        
                        # Use R*Tree to get cities within the bounding box
                        # R*Tree query checks if the bounding box of the city overlaps with our search box
                        cursor.execute("""
                            SELECT c.id, c.name, c.ascii_name, c.country, c.country_code, 
                                   c.state, c.state_code, c.lat, c.lng
                            FROM city_data c
                            INNER JOIN city_rtree r ON c.id = r.id
                            WHERE r.min_lat <= ? AND r.max_lat >= ? 
                               AND r.min_lng <= ? AND r.max_lng >= ?
                        """, (max_lat, min_lat, max_lng, min_lng))
                        
                        rows = cursor.fetchall()
                        columns = ['id', 'name', 'ascii_name', 'country', 'country_code', 
                                  'state', 'state_code', 'lat', 'lng']
                        
                        # Get the cities and filter by Haversine distance (more accurate than bounding box)
                        cities_with_distance = []
                        for city in self._rows_to_dicts(rows, columns):
                            city_lat = city['lat']
                            city_lng = city['lng']
                            distance = self._haversine(lat, lng, city_lat, city_lng)
                            
                            if distance <= radius_km:
                                city['distance_km'] = distance
                                cities_with_distance.append(city)
                        
                        # Sort by distance
                        return sorted(cities_with_distance, key=lambda x: x['distance_km'])
                
                # Fallback method for when R*Tree is not available or for other database types
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