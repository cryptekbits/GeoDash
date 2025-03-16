"""
Database management module for the GeoDash package.

This module provides the DatabaseManager class for managing database connections 
and operations for the GeoDash package. It supports both SQLite and PostgreSQL.
"""

import os
import sqlite3
import logging
import threading
from typing import Optional, Any, Dict, Tuple, List
from contextlib import contextmanager
from pathlib import Path
import time
from GeoDash.utils import log_error_with_github_info
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Database manager for the GeoDash package.
    
    This class provides a wrapper around SQLite and PostgreSQL database connections
    for the GeoDash package.
    """
    
    def __init__(self, db_uri: str, persistent: bool = False):
        """
        Initialize the database manager with a database URI.
        
        Args:
            db_uri: Database URI to connect to
            persistent: Whether to keep the connection open (True) or close after use (False)
        """
        self.db_uri = db_uri
        self.db_type = self._get_db_type(db_uri)
        self.persistent = persistent
        self.connection = None
        self._connection_lock = threading.Lock()
        
        if self.persistent:
            # Immediately establish a persistent connection
            self.connection = self._get_connection()
            logger.info(f"Established persistent {self.db_type} connection")
    
    def __del__(self):
        """Destructor to ensure connection is closed when object is garbage collected."""
        self.close()
    
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
            raise ValueError(f"Unsupported database URI: {db_uri}")
    
    def _get_connection(self):
        """
        Get a database connection.
        
        Returns:
            Database connection
        """
        if self.persistent and self.connection is not None:
            # Return the existing persistent connection
            return self.connection
            
        if self.db_type == 'sqlite':
            # SQLite URI format: sqlite:///path/to/db.sqlite
            import sqlite3
            
            # Extract the path from the URI
            path = self.db_uri.replace('sqlite:///', '')
            
            logger.info(f"Connecting to SQLite database at {path}")
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            
            # Connect to the database (will create if it doesn't exist)
            conn = sqlite3.connect(path)
            
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
                raise ValueError(f"Invalid PostgreSQL URI: {self.db_uri}")
                
            user, password, host, port, dbname = match.groups()
            
            # Set default values if not specified
            if not port:
                port = 5432
                
            # Connect to the database
            conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
            
            return conn
            
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
    def cursor(self):
        """
        Get a database cursor as a context manager.
        
        Returns:
            Database cursor context manager
        """
        if self.persistent:
            with self._connection_lock:
                if self.connection is None or (hasattr(self.connection, 'closed') and self.connection.closed):
                    self.connection = self._get_connection()
                
                # Create a PersistentCursor that wraps the cursor but doesn't close the connection
                return PersistentCursor(self.connection.cursor(), self.connection)
        
        # For non-persistent connections, use the DatabaseCursor context manager
        return DatabaseCursor(self)
    
    def close(self):
        """Close the database connection if it exists."""
        if self.connection and not self.persistent:
            try:
                self.connection.close()
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
            if self.db_type == 'sqlite':
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            else:  # PostgreSQL
                cursor.execute(f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=?)", (table_name,))
            
            result = cursor.fetchone()
            return bool(result and result[0])
    
    def create_table(self, table_name: str, schema: str):
        """
        Create a table in the database.
        
        Args:
            table_name: Name of the table to create
            schema: SQL schema definition for the table
        """
        with self.cursor() as cursor:
            cursor.execute(schema)
            logger.info(f"Created table: {table_name}")
    
    def create_index(self, index_name: str, table_name: str, columns: List[str], unique: bool = False):
        """
        Create an index on a table.
        
        Args:
            index_name: Name of the index to create
            table_name: Name of the table to create the index on
            columns: Columns to include in the index
            unique: Whether the index should enforce uniqueness
        """
        unique_str = "UNIQUE" if unique else ""
        columns_str = ", ".join(columns)
        
        with self.cursor() as cursor:
            cursor.execute(f"CREATE {unique_str} INDEX IF NOT EXISTS {index_name} ON {table_name}({columns_str})")
            logger.info(f"Created index: {index_name} on {table_name}({columns_str})")
    
    def execute(self, query: str, params: Tuple = ()):
        """
        Execute a SQL query.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            Query results
        """
        with self.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    
    def execute_many(self, query: str, params_list: List[Tuple]):
        """
        Execute a SQL query with multiple parameter sets.
        
        Args:
            query: SQL query to execute
            params_list: List of query parameter tuples
        """
        with self.cursor() as cursor:
            cursor.executemany(query, params_list)
    
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
        
    def __enter__(self):
        """Enter the context manager and get a cursor."""
        self.connection = self.db_manager._get_connection()
        self.cursor = self.connection.cursor()
        return self.cursor
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager and clean up resources."""
        try:
            if exc_type is None:
                # No exception occurred, commit the transaction
                self.connection.commit()
            else:
                # An exception occurred, rollback the transaction
                self.connection.rollback()
        except Exception as e:
            logger.error(f"Error committing/rolling back transaction: {str(e)}")
        finally:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()

class PersistentCursor:
    """
    Context manager for database cursor operations with persistent connections.
    
    This class wraps a cursor from a persistent connection, handling the commit/rollback
    but not closing the connection when the operation is completed.
    """
    
    def __init__(self, cursor, connection):
        """
        Initialize the persistent cursor context manager.
        
        Args:
            cursor: The database cursor
            connection: The persistent database connection
        """
        self.cursor = cursor
        self.connection = connection
        
    def __enter__(self):
        """Enter the context manager and return the cursor."""
        return self.cursor
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager and commit/rollback the transaction."""
        try:
            if exc_type is None:
                # No exception occurred, commit the transaction
                self.connection.commit()
            else:
                # An exception occurred, rollback the transaction
                self.connection.rollback()
        except Exception as e:
            logger.error(f"Error committing/rolling back transaction: {str(e)}")
        finally:
            # Close only the cursor, not the connection
            if self.cursor:
                self.cursor.close() 