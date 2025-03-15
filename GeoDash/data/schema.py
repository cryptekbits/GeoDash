"""
Schema management module for the GeoDash package.

This module provides schema definitions and management for the GeoDash database.
"""

import logging
from typing import List, Dict, Any
from GeoDash.data.database import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SchemaManager:
    """
    A class to manage the database schema for GeoDash.
    
    This class handles the creation and management of tables and indexes
    for the GeoDash database.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize the SchemaManager with a database manager.
        
        Args:
            db_manager: The database manager to use for schema operations
        """
        self.db_manager = db_manager
        self.city_table_name = 'city_data'
    
    def ensure_schema_exists(self):
        """
        Ensure that the database schema exists, creating it if necessary.
        """
        if not self.db_manager.table_exists(self.city_table_name):
            logger.info(f"Table {self.city_table_name} does not exist. Creating schema.")
            self.create_schema()
        else:
            logger.info(f"Table {self.city_table_name} already exists.")
    
    def create_schema(self):
        """
        Create the database schema, including tables and indexes.
        """
        self._create_city_table()
        self._create_city_indexes()
        logger.info("Schema creation complete.")
    
    def _create_city_table(self):
        """
        Create the city_data table in the database.
        """
        if self.db_manager.db_type == 'sqlite':
            # SQLite table creation
            schema = '''
            CREATE TABLE city_data (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                ascii_name TEXT NOT NULL,
                country TEXT NOT NULL,
                country_code CHAR(2) NOT NULL,
                state TEXT,
                state_code TEXT,
                lat REAL NOT NULL,
                lng REAL NOT NULL,
                population INTEGER
            )
            '''
        else:  # PostgreSQL
            # PostgreSQL table creation
            schema = '''
            CREATE TABLE city_data (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                ascii_name TEXT NOT NULL,
                country TEXT NOT NULL,
                country_code CHAR(2) NOT NULL,
                state TEXT,
                state_code TEXT,
                lat DOUBLE PRECISION NOT NULL,
                lng DOUBLE PRECISION NOT NULL,
                population INTEGER
            )
            '''
        
        self.db_manager.create_table(self.city_table_name, schema)
        logger.info("Created city_data table")
    
    def _create_city_indexes(self):
        """
        Create indexes on the city_data table for better query performance.
        """
        # Create basic indexes for all database types
        indexes = [
            {'name': 'idx_city_name', 'columns': ['ascii_name']},
            {'name': 'idx_city_country', 'columns': ['country']},
            {'name': 'idx_city_state', 'columns': ['state']},
            {'name': 'idx_city_coords', 'columns': ['lat', 'lng']},
            {'name': 'idx_city_population', 'columns': ['population']}
        ]
        
        for index in indexes:
            self.db_manager.create_index(
                index_name=index['name'],
                table_name=self.city_table_name,
                columns=index['columns']
            )
        
        # Add PostGIS spatial index for PostgreSQL if available
        if self.db_manager.db_type == 'postgresql':
            try:
                with self.db_manager.cursor() as cursor:
                    # Try to add PostGIS extension and spatial index
                    cursor.execute('CREATE EXTENSION IF NOT EXISTS postgis')
                    cursor.execute(f"SELECT AddGeometryColumn('{self.city_table_name}', 'geom', 4326, 'POINT', 2)")
                    cursor.execute(f"CREATE INDEX idx_city_geom ON {self.city_table_name} USING GIST(geom)")
                    logger.info("Created PostGIS spatial index")
            except Exception as e:
                logger.warning(f"Could not create PostGIS spatial index: {str(e)}")
    
    def get_table_info(self) -> Dict[str, Any]:
        """
        Get information about the city_data table.
        
        Returns:
            Dictionary with table information
        """
        if not self.db_manager.table_exists(self.city_table_name):
            return {'exists': False}
        
        with self.db_manager.cursor() as cursor:
            if self.db_manager.db_type == 'sqlite':
                cursor.execute(f"PRAGMA table_info({self.city_table_name})")
                columns = [{'name': row[1], 'type': row[2]} for row in cursor.fetchall()]
                
                cursor.execute(f"SELECT COUNT(*) FROM {self.city_table_name}")
                row_count = cursor.fetchone()[0]
                
                return {
                    'exists': True,
                    'columns': columns,
                    'row_count': row_count
                }
            else:  # PostgreSQL
                cursor.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{self.city_table_name}'
                """)
                columns = [{'name': row[0], 'type': row[1]} for row in cursor.fetchall()]
                
                cursor.execute(f"SELECT COUNT(*) FROM {self.city_table_name}")
                row_count = cursor.fetchone()[0]
                
                return {
                    'exists': True,
                    'columns': columns,
                    'row_count': row_count
                } 