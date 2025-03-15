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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    A class to manage database connections and schema for GeoDash.
    
    This class handles the connections to either SQLite or PostgreSQL databases,
    creates schemas, and provides utilities for database operations.
    
    Attributes:
        db_uri (Optional[str]): Database URI connection string
        conn: Database connection object
        db_type (str): Type of database ('sqlite' or 'postgresql')
        lock (threading.Lock): Lock for thread-safe database operations
    """
    
    def __init__(self, db_uri: Optional[str] = None):
        """
        Initialize the DatabaseManager with a connection to the specified database.
        
        Args:
            db_uri: Database URI. If None, a SQLite database will be created in the GeoDash module.
                    For SQLite: 'sqlite:///path/to/db.sqlite'
                    For PostgreSQL: 'postgresql://user:password@localhost:5432/dbname'
                    
        Raises:
            ValueError: If an unsupported database URI is provided
            ImportError: If psycopg2 is not installed when using PostgreSQL
        """
        self.db_uri = db_uri
        self.conn = None
        self.db_type = ''
        self.lock = threading.Lock()
        
        # Connect to the database
        self._connect_to_db()
        
    def _connect_to_db(self):
        """
        Connect to the specified database or create a new SQLite database.
        
        This method handles connection to either SQLite or PostgreSQL databases
        based on the db_uri parameter provided during initialization.
        
        Raises:
            ValueError: If an unsupported database URI is provided
            ImportError: If psycopg2 is not installed when using PostgreSQL
        """
        try:
            if self.db_uri is None:
                # Create a SQLite database in the GeoDash module
                db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'cities.sqlite')
                os.makedirs(os.path.dirname(db_path), exist_ok=True)  # Ensure the data directory exists
                logger.info(f"No DB URI provided. Creating SQLite database at {db_path}")
                self.conn = sqlite3.connect(db_path, check_same_thread=False)
                self.db_type = 'sqlite'
            elif self.db_uri.startswith('sqlite:///'):
                # Connect to the specified SQLite database
                db_path = self.db_uri.replace('sqlite:///', '')
                logger.info(f"Connecting to SQLite database at {db_path}")
                self.conn = sqlite3.connect(db_path, check_same_thread=False)
                self.db_type = 'sqlite'
            elif self.db_uri.startswith('postgresql://'):
                # Connect to the specified PostgreSQL database
                try:
                    import psycopg2
                    logger.info(f"Connecting to PostgreSQL database at {self.db_uri}")
                    self.conn = psycopg2.connect(self.db_uri.replace('postgresql://', ''))
                    self.db_type = 'postgresql'
                except ImportError:
                    log_error_with_github_info(ImportError("psycopg2 is not installed"), "Database connection error")
                    raise
            else:
                error = ValueError(f"Unsupported database URI: {self.db_uri}")
                log_error_with_github_info(error, "Database connection error")
                raise error
            
            # Enable foreign keys for SQLite
            if self.db_type == 'sqlite':
                self.conn.execute("PRAGMA foreign_keys = ON")
                # Enable WAL mode for better concurrent performance
                self.conn.execute("PRAGMA journal_mode = WAL")
                self.conn.execute("PRAGMA synchronous = NORMAL")
        except Exception as e:
            log_error_with_github_info(e, "Failed to connect to database")
            raise
            
    @contextmanager
    def cursor(self):
        """
        Context manager for database cursor operations.
        
        This method ensures proper acquisition and release of the database lock,
        and handles cursor creation and cleanup.
        
        Yields:
            Database cursor object
            
        Example:
            with db_manager.cursor() as cursor:
                cursor.execute("SELECT * FROM city_data")
        """
        with self.lock:
            cursor = self.conn.cursor()
            try:
                yield cursor
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                log_error_with_github_info(e, "Database operation failed")
                raise
            finally:
                cursor.close()
    
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
    
    def close(self):
        """
        Close the database connection.
        
        This method should be called when the DatabaseManager instance is no longer needed
        to properly release database resources.
        """
        if self.conn:
            try:
                self.conn.close()
                logger.info("Closed database connection")
            except Exception as e:
                logger.error(f"Error closing database connection: {str(e)}")
    
    def __del__(self):
        """Destructor to ensure the database connection is closed."""
        self.close()
        
    def __enter__(self):
        """Support for 'with' statement."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Support for 'with' statement."""
        self.close() 