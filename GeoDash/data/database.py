"""
Database management module for the GeoDash package.

This module provides the DatabaseManager class for managing database connections 
and operations for the GeoDash package. It supports both SQLite and PostgreSQL.
"""

import os
import sqlite3
import threading
from typing import Optional, Any, Dict, Tuple, List, Union, Iterator, TypeVar, Type, cast, ContextManager, Generator, Callable, Deque, Protocol, NoReturn, Generic, overload, Literal
from contextlib import contextmanager
from pathlib import Path
import time
import re
from collections import deque
from GeoDash.utils import log_error_with_github_info
from GeoDash.utils.logging import get_logger
from GeoDash.exceptions import (
    DatabaseError, ConnectionError, QueryError, 
    TransactionError, ConfigurationError
)

# Get a logger for this module
logger = get_logger(__name__)

T = TypeVar('T', bound='DatabaseManager')
CursorType = TypeVar('CursorType')

class ConnectionPool:
    """
    A connection pool that manages multiple database connections.
    
    This class maintains a pool of database connections that can be reused 
    across multiple database operations, reducing the overhead of creating
    new connections for each operation.
    """
    
    def __init__(self, db_uri: str, connection_timeout: int = 30, 
                 min_connections: int = 2, max_connections: int = 10,
                 max_idle_time: int = 300):
        """
        Initialize the connection pool.
        
        Args:
            db_uri: Database URI to connect to
            connection_timeout: Timeout in seconds for database connections
            min_connections: Minimum number of connections to keep in the pool
            max_connections: Maximum number of connections in the pool
            max_idle_time: Maximum time in seconds a connection can be idle before being checked
        """
        self.db_uri = db_uri
        self.db_type = self._get_db_type(db_uri)
        self.connection_timeout = connection_timeout
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.max_idle_time = max_idle_time
        
        # Pool state
        self._available_connections: Deque[Tuple[Any, float]] = deque()  # (connection, last_used_time)
        self._in_use_connections: Dict[Any, float] = {}  # connection -> checkout_time
        self._pool_lock = threading.RLock()
        self._connection_count = 0
        
        # Initialize the pool with minimum connections
        self._initialize_pool()
        
    def _initialize_pool(self) -> None:
        """Initialize the connection pool with minimum number of connections."""
        with self._pool_lock:
            for _ in range(self.min_connections):
                if self._connection_count < self.max_connections:
                    try:
                        conn = self._create_new_connection()
                        self._available_connections.append((conn, time.time()))
                        self._connection_count += 1
                    except Exception as e:
                        logger.error(f"Error initializing connection pool: {str(e)}")
    
    def _get_db_type(self, db_uri: str) -> str:
        """
        Get the database type from the URI.
        
        Args:
            db_uri: Database URI to parse
            
        Returns:
            Database type: 'sqlite' or 'postgresql'
        """
        if db_uri.startswith('sqlite:'):
            return 'sqlite'
        elif db_uri.startswith('postgresql:'):
            return 'postgresql'
        else:
            raise ConfigurationError(
                message=f"Technical error: Unsupported database URI: {db_uri}",
                user_message="The database configuration is invalid.",
                context={"db_uri": db_uri}
            )
    
    def _create_new_connection(self) -> Any:
        """
        Create a new database connection.
        
        Returns:
            A new database connection
        """
        if self.db_type == 'sqlite':
            # SQLite URI format: sqlite:///path/to/db.sqlite
            import sqlite3
            
            # Extract the path from the URI
            path = self.db_uri.replace('sqlite:///', '')
            
            logger.debug(f"Creating new SQLite connection at {path}")
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            
            # Connect to the database (will create if it doesn't exist)
            conn = sqlite3.connect(path, timeout=self.connection_timeout)
            
            # Enable foreign keys
            conn.execute('PRAGMA foreign_keys = ON')
            
            # Return row objects as dictionaries
            conn.row_factory = sqlite3.Row
            
            return conn
            
        elif self.db_type == 'postgresql':
            # PostgreSQL URI format: postgresql://user:pass@host:port/dbname
            import psycopg2
            from psycopg2.extras import RealDictCursor
            
            logger.debug(f"Creating new PostgreSQL connection")
            
            # Extract connection parameters from the URI
            match = re.match(r'postgresql://(?:(\w+)(?::([^@]+))?@)?([^:/]+)(?::(\d+))?/(\w+)', self.db_uri)
            
            if not match:
                raise ConfigurationError(f"Invalid PostgreSQL URI: {self.db_uri}")
                
            user, password, host, port, dbname = match.groups()
            
            # Set default values if not specified
            if not port:
                port = 5432
                
            # Connect to the database with timeout
            conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port,
                connect_timeout=self.connection_timeout
            )
            
            return conn
            
        else:
            raise ConfigurationError(f"Unsupported database type: {self.db_type}")
    
    def _check_connection(self, connection: Any) -> bool:
        """
        Check if a connection is still alive.
        
        Args:
            connection: The database connection to check
            
        Returns:
            True if connection is valid, False otherwise
        """
        try:
            # Test connection with simple query
            if self.db_type == 'sqlite':
                connection.execute("SELECT 1").fetchone()
            else:  # PostgreSQL
                # Use server ping for PostgreSQL
                if hasattr(connection, 'closed') and not connection.closed:
                    old_isolation_level = connection.isolation_level
                    connection.isolation_level = 0  # autocommit mode
                    connection.cursor().execute("SELECT 1")
                    connection.isolation_level = old_isolation_level
                else:
                    # Connection is closed, need a new one
                    return False
            
            return True
        except Exception:
            return False
    
    def get_connection(self) -> Any:
        """
        Get a connection from the pool or create a new one if needed.
        
        Returns:
            A database connection
        """
        with self._pool_lock:
            # First, try to get a connection from the pool
            while self._available_connections:
                connection, last_used_time = self._available_connections.popleft()
                
                # Check if connection is still valid
                current_time = time.time()
                if current_time - last_used_time > self.max_idle_time:
                    # Connection has been idle for too long, check if still valid
                    if self._check_connection(connection):
                        # Connection is still good
                        self._in_use_connections[connection] = current_time
                        return connection
                    else:
                        # Connection is no longer valid, close and remove it
                        try:
                            connection.close()
                        except:
                            pass
                        self._connection_count -= 1
                else:
                    # Connection should be fresh enough
                    self._in_use_connections[connection] = current_time
                    return connection
            
            # If we get here, we need to create a new connection
            if self._connection_count < self.max_connections:
                # We can create a new connection
                connection = self._create_new_connection()
                self._in_use_connections[connection] = time.time()
                self._connection_count += 1
                return connection
            
            # If we get here, we've reached max connections and need to wait
            # In a more sophisticated implementation, we might block and wait
            # for a connection to become available. For now, we'll just error.
            raise ConnectionError("Connection pool exhausted. Try again later.")
    
    def return_connection(self, connection: Any) -> None:
        """
        Return a connection to the pool.
        
        Args:
            connection: The database connection to return
        """
        with self._pool_lock:
            if connection in self._in_use_connections:
                # Remove from in-use set
                del self._in_use_connections[connection]
                
                # Check if connection is still valid
                if self._check_connection(connection):
                    # Add back to available queue
                    self._available_connections.append((connection, time.time()))
                else:
                    # Connection is no longer valid, close it
                    try:
                        connection.close()
                    except:
                        pass
                    self._connection_count -= 1
            else:
                # This connection wasn't checked out from this pool
                logger.warning("Returning a connection that wasn't checked out from this pool")
                try:
                    connection.close()
                except:
                    pass
    
    def close_all(self) -> None:
        """Close all connections in the pool."""
        with self._pool_lock:
            # Close available connections
            while self._available_connections:
                connection, _ = self._available_connections.popleft()
                try:
                    connection.close()
                except:
                    pass
            
            # Close in-use connections
            for connection in list(self._in_use_connections.keys()):
                try:
                    connection.close()
                except:
                    pass
                del self._in_use_connections[connection]
            
            # Reset counts
            self._connection_count = 0

class DatabaseManager:
    """
    Manager for database connections and operations.
    
    This class provides methods for managing connections to various database types
    and performing common database operations like executing queries and managing
    tables.
    """
    
    def __init__(self, db_uri: str, persistent: bool = False, connection_timeout: int = 30, 
                 min_pool_size: int = 2, max_pool_size: int = 10) -> None:
        """
        Initialize a DatabaseManager instance.
        
        Args:
            db_uri: Database URI to connect to
            persistent: Whether to keep the connection open (for worker processes)
            connection_timeout: Timeout in seconds for database connections
            min_pool_size: Minimum number of connections in the pool
            max_pool_size: Maximum number of connections in the pool
        """
        self.db_uri = db_uri
        self.db_type = self._get_db_type(db_uri)
        self.persistent = persistent
        self.connection_timeout = connection_timeout
        self.last_connection_time = 0
        self.max_idle_time = 300  # 5 minutes default idle timeout
        
        # Only try to import config if we're not already importing it (to avoid circular import)
        try:
            from GeoDash.config.manager import get_config
            self.config = get_config()
            
            # Get pool settings from config if not explicitly provided
            pool_enabled = self.config.is_pooling_enabled()
            
            if pool_enabled:
                # Get pool settings from config
                pool_settings = self.config.get_pool_settings()
                min_pool_size = min_pool_size or pool_settings.get('min_size', 2)
                max_pool_size = max_pool_size or pool_settings.get('max_size', 10)
                connection_timeout = connection_timeout or pool_settings.get('timeout', 30)
                self.max_idle_time = pool_settings.get('idle_timeout', 300)
        except ImportError:
            # Config module not available, use defaults
            pool_enabled = True
            
        # Set SQLite pragmas based on advanced feature flag
        self.use_advanced_features = True
        try:
            if hasattr(self, 'config'):
                self.use_advanced_features = self.config.is_feature_enabled('enable_advanced_db')
        except:
            pass
        
        self.connection = None
        self.connection_pool = None
        
        # Only use connection pooling for non-persistent connections to PostgreSQL
        if self.db_type == 'postgresql' and not persistent and pool_enabled:
            self.connection_pool = ConnectionPool(
                db_uri, 
                connection_timeout=connection_timeout,
                min_connections=min_pool_size,
                max_connections=max_pool_size
            )
            
        logger.debug(f"Initialized DatabaseManager for {self.db_type} with URI: {db_uri}")
    
    def __del__(self) -> None:
        """Cleanup during garbage collection - attempt to close connections."""
        try:
            # Check if this instance has been properly initialized
            if hasattr(self, 'connection') or hasattr(self, 'connection_pool'):
                self.close()
        except Exception:
            # Ignore errors during garbage collection
            pass
    
    def __enter__(self: T) -> T:
        """Enter the context manager, return self for use in 'with' statements."""
        return self
    
    def __exit__(self, exc_type: Optional[Type[BaseException]], 
                exc_val: Optional[BaseException], 
                exc_tb: Optional[Any]) -> None:
        """Exit the context manager, close the connection if not persistent."""
        if not self.persistent:
            self.close()
        return False  # Don't suppress exceptions
    
    def _get_db_type(self, db_uri: str) -> str:
        """
        Get the database type from the URI.
        
        Args:
            db_uri: Database URI to parse
            
        Returns:
            Database type: 'sqlite' or 'postgresql'
            
        Raises:
            ConfigurationError: If the database URI is not supported
        """
        if db_uri.startswith('sqlite:'):
            return 'sqlite'
        elif db_uri.startswith('postgresql:'):
            return 'postgresql'
        else:
            raise ConfigurationError(
                message=f"Technical error: Unsupported database URI: {db_uri}",
                user_message="The database configuration is invalid.",
                context={"db_uri": db_uri}
            )
    
    def _check_connection(self):
        """
        Check if the connection is still alive and reconnect if needed.
        
        Returns:
            True if connection is valid, False if reconnection failed
        """
        if not self.connection:
            return False
            
        # Skip check if connection is recent
        current_time = time.time()
        if current_time - self.last_connection_time < self.max_idle_time:
            return True
            
        try:
            # Test connection with simple query
            if self.db_type == 'sqlite':
                self.connection.execute("SELECT 1").fetchone()
            else:  # PostgreSQL
                # Use server ping for PostgreSQL
                if hasattr(self.connection, 'closed') and not self.connection.closed:
                    old_isolation_level = self.connection.isolation_level
                    self.connection.isolation_level = 0  # autocommit mode
                    self.connection.cursor().execute("SELECT 1")
                    self.connection.isolation_level = old_isolation_level
                else:
                    # Connection is closed, need to reconnect
                    raise ConnectionError("Connection is closed")
            
            # Update last connection time
            self.last_connection_time = current_time
            return True
        except Exception as e:
            logger.warning(f"Connection stale, attempting to reconnect: {str(e)}")
            try:
                # Try to close the old connection
                try:
                    self.connection.close()
                except:
                    pass
                    
                # Get a new connection
                self.connection = self._get_connection()
                self.last_connection_time = time.time()
                logger.info("Successfully reconnected to database")
                return True
            except Exception as reconnect_error:
                logger.error(f"Failed to reconnect: {str(reconnect_error)}")
                return False
    
    def _get_connection(self):
        """
        Get a database connection based on the configured database type.
        
        For persistent connections, reuses the existing connection if available.
        For non-persistent connections, creates a new connection.
        
        Returns:
            Database connection object
        """
        # If we're using a connection pool, get a connection from it
        if self.connection_pool is not None:
            try:
                return self.connection_pool.get_connection()
            except Exception as e:
                raise ConnectionError(
                    message=f"Failed to get connection from pool: {str(e)}",
                    user_message="The database is currently unavailable.",
                    context={"db_uri": self.db_uri}
                )
                
        # For persistent connections, reuse the existing connection if available
        if self.persistent and self.connection is not None:
            try:
                # Check if connection is still alive
                self._check_connection()
                return self.connection
            except Exception:
                # If connection check fails, create a new one
                try:
                    self.connection = None
                except:
                    # Ignore any error while closing
                    pass
        
        # Create a new connection
        if self.db_type == 'sqlite':
            # Extract the path from the URI
            path = self.db_uri.replace('sqlite:///', '')
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            
            # Connect to the database
            import sqlite3
            connection = sqlite3.connect(path, timeout=self.connection_timeout)
            
            # Enable foreign keys, always safe to enable
            connection.execute('PRAGMA foreign_keys = ON')
            
            # Apply additional pragmas if advanced features are enabled
            if self.use_advanced_features:
                # These pragmas optimize for performance but may marginally reduce durability
                connection.execute('PRAGMA journal_mode = WAL')  # Write-Ahead Logging for better concurrency
                connection.execute('PRAGMA synchronous = NORMAL')  # Slightly better performance
                connection.execute('PRAGMA cache_size = -2000')  # 2MB page cache
                connection.execute('PRAGMA temp_store = MEMORY')  # Store temp tables in memory
            
            # Return row objects as dictionaries
            connection.row_factory = sqlite3.Row
            
        elif self.db_type == 'postgresql':
            # Connect to PostgreSQL
            try:
                import psycopg2
                from psycopg2.extras import RealDictCursor
                
                # Extract connection parameters from the URI
                match = re.match(r'postgresql://(?:(\w+)(?::([^@]+))?@)?([^:/]+)(?::(\d+))?/(\w+)', self.db_uri)
                
                if not match:
                    raise ConfigurationError(
                        message=f"Invalid PostgreSQL URI: {self.db_uri}",
                        user_message="The database configuration is invalid.",
                        context={"db_uri": self.db_uri}
                    )
                    
                user, password, host, port, dbname = match.groups()
                
                # Set default values
                port = port or 5432
                
                # Additional connection parameters for performance
                conn_params = {}
                if self.use_advanced_features:
                    conn_params.update({
                        'application_name': 'GeoDash',
                        'client_encoding': 'utf8',
                        'options': '-c statement_timeout=30000'  # 30 seconds timeout
                    })
                
                # Connect to the database
                connection = psycopg2.connect(
                    dbname=dbname,
                    user=user,
                    password=password,
                    host=host,
                    port=port,
                    connect_timeout=self.connection_timeout,
                    **conn_params
                )
                
                # Set cursor factory for returning dictionaries
                connection.cursor_factory = RealDictCursor
                
            except ImportError:
                raise ConfigurationError(
                    message="PostgreSQL support requires psycopg2 package",
                    user_message="Database connection failed because the required library is not installed.",
                    hint="Install psycopg2 with: pip install psycopg2-binary"
                )
            except Exception as e:
                raise ConnectionError(
                    message=f"Failed to connect to PostgreSQL: {str(e)}",
                    user_message="Could not connect to the PostgreSQL database.",
                    context={"db_uri": self.db_uri}
                )
        else:
            raise ConfigurationError(
                message=f"Technical error: Unsupported database type: {self.db_type}",
                user_message="The database configuration is invalid."
            )
            
        # Store connection for reuse if persistent
        if self.persistent:
            self.connection = connection
            
        return connection
    
    def cursor(self) -> Union['DatabaseCursor', 'PersistentCursor', 'PooledCursor']:
        """
        Get a database cursor for executing SQL statements.
        
        For persistent connections, returns a cursor that shares the persistent connection.
        For non-persistent connections, returns a cursor that will close when the context
        manager exits.
        
        Returns:
            Database cursor appropriate for the connection type
        """
        try:
            # If using a connection pool, return a pooled cursor
            if self.connection_pool is not None:
                connection = self.connection_pool.get_connection()
                return PooledCursor(connection, self.connection_pool)
                
            # If using a persistent connection, return a persistent cursor
            if self.persistent:
                if not self.connection or not self._check_connection():
                    # Create a new connection if necessary
                    self.connection = self._get_connection()
                
                # Create a cursor from the persistent connection
                cursor = self.connection.cursor()
                return PersistentCursor(cursor, self.connection)
                
            # Otherwise, return a regular DatabaseCursor
            return DatabaseCursor(self)
        except Exception as e:
            # Log the error and re-raise
            logger.error(f"Error creating cursor: {str(e)}")
            raise ConnectionError(
                message=f"Failed to create database cursor: {str(e)}",
                user_message="Could not connect to the database.",
                context={"db_uri": self.db_uri}
            )
    
    def close(self) -> None:
        """
        Close the database connection and release resources.
        
        For persistent connections, this is a no-op unless force=True.
        For connection pools, this closes all connections in the pool.
        """
        try:
            # Close pooled connections if they exist
            if self.connection_pool is not None:
                logger.debug(f"Closing connection pool")
                self.connection_pool.close_all()
                self.connection_pool = None
            
            # Close persistent connection if it exists
            if self.connection is not None:
                logger.debug(f"Closing persistent connection")
                try:
                    self.connection.close()
                except Exception as e:
                    logger.warning(f"Error closing connection: {str(e)}")
                self.connection = None
                
            logger.debug("Database connections closed")
        except Exception as e:
            logger.error(f"Error closing database connections: {str(e)}")
            # Don't re-raise, as close() is often called during cleanup
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if the table exists, False otherwise
        """
        with self.cursor() as cursor:
            try:
                if self.db_type == 'sqlite':
                    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                else:  # PostgreSQL
                    cursor.execute(f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=?)", (table_name,))
                
                result = cursor.fetchone()
                return bool(result and result[0])
            except Exception as e:
                logger.error(f"Error checking if table exists: {str(e)}")
                return False
    
    def create_table(self, table_name: str, schema: str) -> None:
        """
        Create a table in the database.
        
        Args:
            table_name: Name of the table to create
            schema: SQL schema definition for the table
            
        Raises:
            DatabaseError: If there's an error creating the table
        """
        with self.cursor() as cursor:
            try:
                cursor.execute(schema)
                logger.info(f"Created table: {table_name}")
            except Exception as e:
                logger.error(f"Error creating table {table_name}: {str(e)}")
                raise DatabaseError(f"Error creating table {table_name}: {str(e)}") from e
    
    def create_index(self, index_name: str, table_name: str, columns: List[str], unique: bool = False) -> None:
        """
        Create an index on a table.
        
        Args:
            index_name: Name of the index to create
            table_name: Name of the table to create the index on
            columns: Columns to include in the index
            unique: Whether the index should enforce uniqueness
            
        Raises:
            DatabaseError: If there's an error creating the index
        """
        unique_str = "UNIQUE" if unique else ""
        columns_str = ", ".join(columns)
        
        with self.cursor() as cursor:
            try:
                cursor.execute(f"CREATE {unique_str} INDEX IF NOT EXISTS {index_name} ON {table_name}({columns_str})")
                logger.info(f"Created index: {index_name} on {table_name}({columns_str})")
            except Exception as e:
                logger.error(f"Error creating index {index_name}: {str(e)}")
                raise DatabaseError(f"Error creating index {index_name}: {str(e)}") from e
    
    def execute(self, query: str, params: Tuple = ()) -> List[Tuple[Any, ...]]:
        """
        Execute a SQL query.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            Query results
            
        Raises:
            QueryError: If there's an error executing the query
        """
        with self.cursor() as cursor:
            try:
                cursor.execute(query, params)
                return cursor.fetchall()
            except Exception as e:
                logger.error(f"Error executing query: {str(e)}")
                raise QueryError(f"Error executing query: {str(e)}") from e
    
    def execute_many(self, query: str, params_list: List[Tuple]) -> None:
        """
        Execute a SQL query with multiple parameter sets.
        
        Args:
            query: SQL query to execute
            params_list: List of query parameter tuples
            
        Raises:
            QueryError: If there's an error executing the batch query
        """
        with self.cursor() as cursor:
            try:
                cursor.executemany(query, params_list)
            except Exception as e:
                logger.error(f"Error executing batch query: {str(e)}")
                raise QueryError(f"Error executing batch query: {str(e)}") from e
    
    def has_rtree_support(self) -> bool:
        """
        Check if the SQLite database supports R*Tree extension.
        
        Returns:
            True if R*Tree is supported, False otherwise
        """
        if self.db_type != 'sqlite':
            return False
            
        try:
            with self.cursor() as cursor:
                cursor.execute("PRAGMA compile_options")
                compile_options = cursor.fetchall()
                return any('RTREE' in option[0].upper() for option in compile_options)
        except Exception as e:
            logger.warning(f"Error checking R*Tree support: {str(e)}")
            return False

class DatabaseCursor:
    """
    Context manager for database cursor operations for non-persistent connections.
    
    This class creates a new connection for each cursor operation and closes it
    when the operation is completed, ensuring proper cleanup of resources.
    """
    
    def __init__(self, db_manager: 'DatabaseManager'):
        """
        Initialize the cursor context manager.
        
        Args:
            db_manager: Database manager to use for the connection
        """
        self.db_manager = db_manager
        self.connection = None
        self.cursor = None
        
    def __enter__(self) -> Any:
        """
        Enter the context manager and get a cursor.
        
        Returns:
            Database cursor
            
        Raises:
            ConnectionError: If there's an error creating the cursor
        """
        try:
            self.connection = self.db_manager._get_connection()
            self.cursor = self.connection.cursor()
            return self.cursor
        except Exception as e:
            # Ensure connection is closed if cursor creation fails
            if self.connection:
                try:
                    self.connection.close()
                except:
                    pass
            logger.error(f"Error creating cursor: {str(e)}")
            raise ConnectionError(f"Failed to create database cursor: {str(e)}")
        
    def __exit__(self, exc_type: Optional[Type[BaseException]], 
                 exc_val: Optional[BaseException], 
                 exc_tb: Optional[Any]) -> None:
        """
        Exit the context manager and clean up resources.
        
        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Traceback if an exception was raised
        """
        try:
            if exc_type is None:
                # No exception occurred, commit the transaction
                if self.connection:
                    self.connection.commit()
            else:
                # An exception occurred, rollback the transaction
                if self.connection:
                    self.connection.rollback()
        except Exception as e:
            error_msg = f"Error committing/rolling back transaction: {str(e)}"
            logger.error(error_msg)
            raise TransactionError(error_msg) from e
        finally:
            # Always close cursor and connection in finally block
            try:
                if self.cursor:
                    self.cursor.close()
            except Exception as e:
                logger.error(f"Error closing cursor: {str(e)}")
                
            try:
                if self.connection:
                    self.connection.close()
            except Exception as e:
                logger.error(f"Error closing connection: {str(e)}")

class PooledCursor:
    """
    Context manager for database cursor operations with pooled connections.
    
    This class wraps a cursor from a connection pool, handling the commit/rollback
    and returning the connection to the pool when the operation is completed.
    """
    
    def __init__(self, connection: Any, connection_pool: 'ConnectionPool'):
        """
        Initialize the pooled cursor context manager.
        
        Args:
            connection: The database connection from the pool
            connection_pool: The connection pool to return the connection to
        """
        self.connection = connection
        self.connection_pool = connection_pool
        self.cursor = None
        
    def __enter__(self) -> Any:
        """
        Enter the context manager and get a cursor.
        
        Returns:
            Database cursor
            
        Raises:
            ConnectionError: If there's an error creating the cursor
        """
        try:
            self.cursor = self.connection.cursor()
            return self.cursor
        except Exception as e:
            # Return connection to pool and re-raise the exception
            self.connection_pool.return_connection(self.connection)
            logger.error(f"Error creating cursor: {str(e)}")
            raise ConnectionError(f"Failed to create database cursor: {str(e)}")
        
    def __exit__(self, exc_type: Optional[Type[BaseException]], 
                 exc_val: Optional[BaseException], 
                 exc_tb: Optional[Any]) -> None:
        """
        Exit the context manager, handle transaction, and return connection to pool.
        
        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Traceback if an exception was raised
        """
        try:
            if exc_type is None:
                # No exception occurred, commit the transaction
                self.connection.commit()
            else:
                # An exception occurred, rollback the transaction
                self.connection.rollback()
        except Exception as e:
            error_msg = f"Error committing/rolling back transaction: {str(e)}"
            logger.error(error_msg)
            raise TransactionError(error_msg) from e
        finally:
            # Always close cursor and return connection to pool in finally block
            try:
                if self.cursor:
                    self.cursor.close()
            except Exception as e:
                logger.error(f"Error closing cursor: {str(e)}")
                
            # Return connection to the pool
            self.connection_pool.return_connection(self.connection)

class PersistentCursor:
    """
    Context manager for database cursor operations with persistent connections.
    
    This class wraps a cursor from a persistent connection, handling the commit/rollback
    but not closing the connection when the operation is completed.
    """
    
    def __init__(self, cursor: Any, connection: Any):
        """
        Initialize the persistent cursor context manager.
        
        Args:
            cursor: The database cursor
            connection: The persistent database connection
        """
        self.cursor = cursor
        self.connection = connection
        
    def __enter__(self) -> Any:
        """
        Enter the context manager and return the cursor.
        
        Returns:
            Database cursor
        """
        return self.cursor
        
    def __exit__(self, exc_type: Optional[Type[BaseException]], 
                 exc_val: Optional[BaseException], 
                 exc_tb: Optional[Any]) -> None:
        """
        Exit the context manager and commit/rollback the transaction.
        
        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Traceback if an exception was raised
        """
        try:
            if exc_type is None:
                # No exception occurred, commit the transaction
                self.connection.commit()
            else:
                # An exception occurred, rollback the transaction
                self.connection.rollback()
        except Exception as e:
            error_msg = f"Error committing/rolling back transaction: {str(e)}"
            logger.error(error_msg)
            raise TransactionError(error_msg) from e
        finally:
            # Always close the cursor in finally block
            try:
                if self.cursor:
                    self.cursor.close()
            except Exception as e:
                logger.error(f"Error closing cursor: {str(e)}") 

def create_db_manager_from_config() -> DatabaseManager:
    """
    Creates a DatabaseManager using the configuration settings.
    
    This function reads the database configuration from the ConfigManager
    and creates a properly configured DatabaseManager instance.
    
    Returns:
        Configured DatabaseManager instance
    """
    from GeoDash.config.manager import get_config
    
    # Get the configuration manager
    config = get_config()
    
    # Get database URI from config
    db_uri = config.get_database_uri()
    
    # Check if pooling is enabled
    use_pooling = config.is_pooling_enabled()
    
    if use_pooling:
        # Get pool settings
        pool_settings = config.get_pool_settings()
        
        # Create the database manager with pooling
        db_manager = DatabaseManager(
            db_uri=db_uri,
            persistent=False,  # Not using persistent connections when pooling
            connection_timeout=pool_settings["timeout"],
            min_pool_size=pool_settings["min_size"],
            max_pool_size=pool_settings["max_size"]
        )
    else:
        # Create the database manager without pooling
        db_manager = DatabaseManager(
            db_uri=db_uri,
            persistent=True  # Use persistent connections when not pooling
        )
    
    # Configure database-specific options
    
    # For SQLite databases
    if db_uri.startswith("sqlite:"):
        # Check if R-Tree support is enabled
        rtree_enabled = config.get("database.sqlite.rtree", True)
        
        # Check if FTS (Full-Text Search) is enabled
        fts_enabled = config.get("database.sqlite.fts", True)
        
        # Configure SQLite-specific settings here if needed
        if rtree_enabled and db_manager.has_rtree_support():
            logger.debug("R-Tree spatial indexing is enabled")
        else:
            logger.warning("R-Tree spatial indexing is disabled or not supported")
        
        # More SQLite-specific configuration can be added here
    
    # For PostgreSQL databases
    elif db_uri.startswith("postgresql:"):
        # Check if PostGIS extension should be enabled
        postgis_enabled = config.get("database.postgresql.postgis", True)
        
        if postgis_enabled:
            logger.debug("PostGIS extension will be used if available")
            # PostGIS initialization logic could be added here
    
    logger.info(f"Database manager created with URI type: {db_uri.split(':')[0]}")
    return db_manager 