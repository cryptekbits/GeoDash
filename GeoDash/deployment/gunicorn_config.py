"""
Gunicorn configuration file for GeoDash API.
"""
import multiprocessing
import os
import logging
import time
import sys
import tempfile
import fcntl
import pickle
import json
from pathlib import Path

# Add the project root to the Python path
# Since this file is now in GeoDash/deployment, adjust the path to point to the project root
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# Configure logging
from GeoDash.utils.logging import get_logger, configure_logging

# Set up logging with proper configuration
configure_logging(
    level=os.environ.get('GEODASH_LOG_LEVEL', 'info'),
    use_json=os.environ.get('GEODASH_LOG_FORMAT', 'json').lower() == 'json',
    log_file=os.environ.get('GEODASH_LOG_FILE')
)

logger = get_logger("geodash.gunicorn", {"component": "gunicorn"})

# Bind to all interfaces on port 5000
bind = "0.0.0.0:32000"

# Number of worker processes
# A common formula is 2-4 x $(NUM_CORES)
workers = multiprocessing.cpu_count() * 2 + 1

# Use Gevent worker type for better concurrency
worker_class = "gevent"

# Maximum requests a worker will process before restarting
max_requests = 1000
max_requests_jitter = 50

# Timeout for worker processes (seconds)
timeout = 30

# Log level
loglevel = "info"

# Access log format
accesslog = "-"  # Log to stdout
errorlog = "-"   # Log to stderr

# Process name
proc_name = "geodash_api"

# Shared state path
_INIT_MARKER_PATH = os.path.join(tempfile.gettempdir(), "geodash_db_initialized.tmp")
_SHARED_DATA_PATH = os.path.join(tempfile.gettempdir(), "geodash_shared_data.tmp")

# Pre-initialization flag
_db_initialized = False

def _acquire_lock(lock_file):
    """Acquire an exclusive lock on the given file."""
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        return True
    except IOError:
        return False

def _release_lock(lock_file):
    """Release the lock on the given file."""
    try:
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        return True
    except IOError:
        return False

def on_starting(server):
    """Called just before the master process is initialized."""
    # Initialize database in the master process before forking any workers
    from GeoDash.data import CityData
    from GeoDash.data.database import DatabaseManager

    logger.info(f"Starting GeoDash API server with {workers} workers")
    logger.info("Master process: Pre-initializing city data before forking workers")
    
    # Use a file lock to ensure only one master process initializes
    with open(_INIT_MARKER_PATH, 'w+') as lock_file:
        if _acquire_lock(lock_file):
            try:
                start_time = time.time()
                
                # Create database connections
                db_path = os.path.join(project_root, 'GeoDash', 'data', 'cities.db')
                db_uri = f"sqlite:///{db_path}"
                
                logger.info(f"Master process: Initializing database at {db_uri}")
                
                # Initialize the data only once in the master process
                city_data = CityData(db_uri=db_uri)
                
                # Get count for logging
                table_info = city_data.get_table_info()
                record_count = table_info.get('row_count', 0)
                
                # Write marker file to indicate initialization is complete
                # Do NOT attempt to pickle and share the actual data - it's more reliable
                # to just let each worker efficiently load from DB themselves
                init_info = {
                    'timestamp': time.time(),
                    'record_count': record_count,
                    'initialization_time': time.time() - start_time,
                    'status': 'initialized'
                }
                
                logger.info(f"Master process: Database verified with {record_count} records in {time.time() - start_time:.2f}s")
                logger.info("Master process: Workers will load data efficiently on startup")
                
                # Close the connections in the master process
                city_data.close()
                
                # Write to the lock file after everything is finished
                lock_file.seek(0)
                lock_file.write(json.dumps(init_info))
                lock_file.truncate()
                
            except Exception as e:
                logger.error(f"Master process: Failed to pre-initialize database: {str(e)}")
                # Write error to the lock file
                lock_file.seek(0)
                lock_file.write(json.dumps({'error': str(e)}))
                lock_file.truncate()
            finally:
                _release_lock(lock_file)
        else:
            logger.info("Master process: Another process is initializing the database")

def post_fork(server, worker):
    """
    Post-fork initialization.
    This code runs after a worker has been forked.
    Set environment variables for worker identification.
    """
    worker_id = worker.pid
    os.environ['GUNICORN_WORKER_ID'] = str(worker_id)
    logger.info(f"Forked worker {worker_id}")
    
    # Check if we need to clean up any stale shared memory from previous runs
    # that might have crashed without proper cleanup
    if worker_id % workers == 0:  # Only do this for the first worker
        logger.info(f"Worker {worker_id}: Checking for stale shared memory resources")
        try:
            # List shared memory blocks and attempt cleanup of any stale ones
            import glob
            pattern = os.path.join(tempfile.gettempdir(), "wnsm_*")
            for path in glob.glob(pattern):
                if os.path.getmtime(path) < time.time() - 86400:  # Older than 1 day
                    try:
                        os.unlink(path)
                        logger.info(f"Worker {worker_id}: Cleaned up stale shared memory: {path}")
                    except Exception as e:
                        pass
        except Exception as e:
            logger.debug(f"Error checking for stale shared memory: {str(e)}")

# Lifecycle event handlers
def worker_int(worker):
    """Handle worker SIGINT or SIGQUIT events."""
    worker_id = worker.pid
    logger.info(f"Worker {worker_id} received INT or QUIT signal")

def pre_fork(server, worker):
    """Pre-fork initialization."""
    pass

def pre_exec(server):
    """Pre-execution handler."""
    logger.info("Forking GeoDash API master process")

def on_exit(server):
    """Called just before exiting."""
    logger.info("Shutting down GeoDash API server")
    
    # Clean up temporary files
    try:
        if os.path.exists(_INIT_MARKER_PATH):
            os.unlink(_INIT_MARKER_PATH)
        if os.path.exists(_SHARED_DATA_PATH):
            os.unlink(_SHARED_DATA_PATH)
    except Exception as e:
        logger.error(f"Failed to clean up temporary files: {str(e)}") 