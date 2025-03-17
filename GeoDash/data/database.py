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
    Database manager for the GeoDash package.
    
    This class provides a wrapper around SQLite and PostgreSQL database connections
    for the GeoDash package.
    """
    
    def __init__(self, db_uri: str, persistent: bool = False, connection_timeout: int = 30, 
                 min_pool_size: int = 2, max_pool_size: int = 10) -> None:
        """
        Initialize the database manager with a database URI.
        
        Args:
            db_uri: Database URI to connect to
            persistent: Whether to keep the connection open (True) or close after use (False)
            connection_timeout: Timeout in seconds for database connections
            min_pool_size: Minimum number of connections to keep in the pool (if using connection pooling)
            max_pool_size: Maximum number of connections in the pool (if using connection pooling)
        """
        self.db_uri = db_uri
        self.db_type = self._get_db_type(db_uri)
        self.persistent = persistent
        self.connection = None
        self._connection_lock = threading.Lock()
        self.connection_timeout = connection_timeout
        self.last_connection_time = 0
        # Max idle time (seconds) before checking connection
        self.max_idle_time = 300  # 5 minutes
        
        # Connection pooling properties
        self.use_pooling = not persistent
        self.connection_pool = None
        
        if self.use_pooling:
            # Initialize connection pool
            self.connection_pool = ConnectionPool(
                db_uri=db_uri,
                connection_timeout=connection_timeout,
                min_connections=min_pool_size,
                max_connections=max_pool_size,
                max_idle_time=self.max_idle_time
            )
            logger.info(f"Initialized connection pool with {min_pool_size}-{max_pool_size} connections")
        elif self.persistent:
            # Immediately establish a persistent connection
            self.connection = self._get_connection()
            self.last_connection_time = time.time()
            logger.info(f"Established persistent {self.db_type} connection")
    
    def __del__(self) -> None:
        """Destructor to ensure connection is closed when object is garbage collected."""
        self.close()
    
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
        Get a database connection.
        
        Returns:
            Database connection
        """
        if self.persistent and self.connection is not None:
            # Check connection health for persistent connections
            if self._check_connection():
                return self.connection
            
        if self.use_pooling and self.connection_pool:
            # Get connection from the pool
            return self.connection_pool.get_connection()
            
        if self.db_type == 'sqlite':
            # SQLite URI format: sqlite:///path/to/db.sqlite
            import sqlite3
            
            # Extract the path from the URI
            path = self.db_uri.replace('sqlite:///', '')
            
            logger.info(f"Connecting to SQLite database at {path}")
            
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
            
            logger.info(f"Connecting to PostgreSQL database")
            
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
    
    def cursor(self) -> Union['DatabaseCursor', 'PersistentCursor', 'PooledCursor']:
        """
        Get a cursor for executing database operations.
        
        This method returns a cursor as a context manager, which will be
        automatically closed when exiting the context.
        
        Returns:
            Database cursor context manager
        
        Raises:
            ConnectionError: If there's an error getting a connection or cursor
        """
        if self.use_pooling and self.connection_pool:
            try:
                # Get a connection from the pool and create a PooledCursor
                connection = self.connection_pool.get_connection()
                return PooledCursor(connection, self.connection_pool)
            except Exception as e:
                logger.error(f"Error getting pooled cursor: {str(e)}")
                raise ConnectionError(f"Failed to get database cursor from pool: {str(e)}")
                
        with self._connection_lock:
            if self.persistent:
                try:
                    if not self._check_connection():
                        self.connection = self._get_connection()
                        self.last_connection_time = time.time()
                    
                    # Create a PersistentCursor that wraps the cursor but doesn't close the connection
                    return PersistentCursor(self.connection.cursor(), self.connection)
                except Exception as e:
                    logger.error(f"Error getting persistent cursor: {str(e)}")
                    try:
                        # Try to reconnect
                        self.connection = self._get_connection()
                        self.last_connection_time = time.time()
                        return PersistentCursor(self.connection.cursor(), self.connection)
                    except Exception as reconnect_error:
                        logger.error(f"Failed to reconnect for cursor: {str(reconnect_error)}")
                        raise ConnectionError(f"Failed to get database cursor: {str(reconnect_error)}")
            
            # For non-persistent connections, use the DatabaseCursor context manager
            return DatabaseCursor(self)
    
    def close(self) -> None:
        """Close the database connection(s)."""
        with self._connection_lock:
            if self.connection_pool:
                # Close all connections in the pool
                self.connection_pool.close_all()
                self.connection_pool = None
                logger.debug("Connection pool closed")
            
            if self.connection:
                try:
                    self.connection.close()
                    logger.debug("Database connection closed")
                except Exception as e:
                    logger.error(f"Error closing database connection: {str(e)}")
                finally:
                    self.connection = None
    
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