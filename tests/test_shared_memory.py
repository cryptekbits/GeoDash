"""
Test script for shared memory handling in GeoDash repositories.

This script tests the reference counting and cleanup mechanisms for shared memory
blocks used by GeoDash repositories.
"""

import os
import sys
import time
import logging
import multiprocessing as mp
from multiprocessing import shared_memory
import atexit
import tempfile

# Add parent directory to path to import GeoDash
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from GeoDash.data.repositories import (
    get_city_repository, 
    get_geo_repository, 
    BaseRepository,
    _CITY_REPO_SHM_NAME,
    _GEO_REPO_SHM_NAME,
    _REGION_REPO_SHM_NAME,
    _CITY_REPO_DATA_SHM_NAME,
    _GEO_REPO_DATA_SHM_NAME,
    _REGION_REPO_DATA_SHM_NAME,
    _shm_reference_counts,
    _shm_ref_lock,
)
from GeoDash.data.database import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("shared_memory_test")

# Create a temporary database file for testing
db_file = os.path.join(tempfile.gettempdir(), 'geodash_test.db')
DB_URI = f"sqlite:///{db_file}"

def worker_process(worker_id):
    """Worker process that creates and uses repositories."""
    logger.info(f"Worker {worker_id} starting")
    
    # Create a database manager with the test database
    db_manager = DatabaseManager(DB_URI)
    
    # Get repositories
    logger.info(f"Worker {worker_id} getting city repository")
    try:
        city_repo = get_city_repository(db_manager)
        logger.info(f"Worker {worker_id} has city repository: {city_repo is not None}")
    except Exception as e:
        logger.error(f"Worker {worker_id} error getting city repository: {e}")
    
    logger.info(f"Worker {worker_id} getting geo repository")
    try:
        geo_repo = get_geo_repository(db_manager)
        logger.info(f"Worker {worker_id} has geo repository: {geo_repo is not None}")
    except Exception as e:
        logger.error(f"Worker {worker_id} error getting geo repository: {e}")
    
    # Log reference counts
    with _shm_ref_lock:
        logger.info(f"Worker {worker_id} reference counts: {_shm_reference_counts}")
    
    # Do some work with the repositories
    time.sleep(1)  # Simulate work
    
    # Log shared memory state
    logger.info(f"Worker {worker_id} shared memory handles: {len(BaseRepository._shared_memory_handles)}")
    
    # Log reference counts again
    with _shm_ref_lock:
        logger.info(f"Worker {worker_id} reference counts before exit: {_shm_reference_counts}")
    
    # Explicitly exit to test cleanup
    logger.info(f"Worker {worker_id} exiting")
    
def log_shared_memory_state():
    """Log the current state of shared memory blocks."""
    # Check if shared memory blocks exist
    existing_blocks = []
    missing_blocks = []
    
    for name in [
        _CITY_REPO_SHM_NAME, 
        _GEO_REPO_SHM_NAME, 
        _REGION_REPO_SHM_NAME,
        _CITY_REPO_DATA_SHM_NAME,
        _GEO_REPO_DATA_SHM_NAME,
        _REGION_REPO_DATA_SHM_NAME
    ]:
        try:
            shm = shared_memory.SharedMemory(name=name)
            existing_blocks.append(name)
            shm.close()
        except FileNotFoundError:
            missing_blocks.append(name)
    
    logger.info(f"Existing shared memory blocks: {existing_blocks}")
    logger.info(f"Missing shared memory blocks: {missing_blocks}")
    
    # Log reference counts
    with _shm_ref_lock:
        logger.info(f"Current reference counts: {_shm_reference_counts}")

def main():
    """Main test function."""
    logger.info("Starting shared memory test")
    
    # Delete existing test database if it exists
    if os.path.exists(db_file):
        os.remove(db_file)
        logger.info(f"Removed existing test database: {db_file}")
    
    # Create and start multiple worker processes
    processes = []
    for i in range(3):  # Create 3 worker processes
        p = mp.Process(target=worker_process, args=(i,))
        processes.append(p)
        p.start()
    
    # Wait for all processes to finish
    for p in processes:
        p.join()
    
    # Check shared memory state after worker processes
    logger.info("Checking shared memory state after worker processes")
    log_shared_memory_state()
    
    # Explicitly call cleanup for any remaining shared memory
    logger.info("Explicitly calling shared memory cleanup")
    BaseRepository.cleanup_shared_memory()
    
    # Check shared memory state after explicit cleanup
    logger.info("Checking shared memory state after explicit cleanup")
    log_shared_memory_state()
    
    # Clean up test database
    if os.path.exists(db_file):
        os.remove(db_file)
        logger.info(f"Removed test database: {db_file}")
    
    logger.info("Shared memory test completed")

if __name__ == "__main__":
    main() 