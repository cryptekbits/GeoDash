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
        self._create_search_optimizations()
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
                state_id INTEGER,
                state_code TEXT,
                state TEXT,
                country_id INTEGER,
                country_code CHAR(2) NOT NULL,
                country TEXT NOT NULL,
                lat REAL NOT NULL,
                lng REAL NOT NULL,
                wiki_data_id TEXT
            )
            '''
        else:  # PostgreSQL
            # PostgreSQL table creation
            schema = '''
            CREATE TABLE city_data (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                ascii_name TEXT NOT NULL,
                state_id INTEGER,
                state_code TEXT,
                state TEXT,
                country_id INTEGER,
                country_code CHAR(2) NOT NULL,
                country TEXT NOT NULL,
                lat DOUBLE PRECISION NOT NULL,
                lng DOUBLE PRECISION NOT NULL,
                wiki_data_id TEXT
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
            {'name': 'idx_city_coords', 'columns': ['lat', 'lng']}
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
                
    def _create_search_optimizations(self):
        """
        Create full-text search optimizations for each database type.
        For SQLite: Create an FTS5 virtual table
        For PostgreSQL: Add tsvector column with GIN index
        """
        if self.db_manager.db_type == 'sqlite':
            # Create an FTS5 virtual table for SQLite
            try:
                # Check if FTS5 is available
                has_fts5 = False
                with self.db_manager.cursor() as cursor:
                    cursor.execute("SELECT sqlite_compileoption_used('ENABLE_FTS5')")
                    has_fts5 = bool(cursor.fetchone()[0])
                
                if has_fts5:
                    # Create FTS5 virtual table
                    with self.db_manager.cursor() as cursor:
                        cursor.execute("""
                        CREATE VIRTUAL TABLE IF NOT EXISTS city_search USING fts5(
                            id UNINDEXED,
                            name, 
                            ascii_name,
                            country,
                            state,
                            content='city_data',
                            content_rowid='id'
                        )
                        """)
                        
                        # Populate the FTS5 table with existing data
                        cursor.execute("""
                        INSERT INTO city_search(rowid, name, ascii_name, country, state)
                        SELECT id, name, ascii_name, country, state FROM city_data
                        """)
                        
                        # Create triggers to keep the FTS table in sync with the main table
                        cursor.execute("""
                        CREATE TRIGGER IF NOT EXISTS city_data_ai AFTER INSERT ON city_data BEGIN
                            INSERT INTO city_search(rowid, name, ascii_name, country, state)
                            VALUES (new.id, new.name, new.ascii_name, new.country, new.state);
                        END;
                        """)
                        
                        cursor.execute("""
                        CREATE TRIGGER IF NOT EXISTS city_data_ad AFTER DELETE ON city_data BEGIN
                            INSERT INTO city_search(city_search, rowid, name, ascii_name, country, state)
                            VALUES ('delete', old.id, old.name, old.ascii_name, old.country, old.state);
                        END;
                        """)
                        
                        cursor.execute("""
                        CREATE TRIGGER IF NOT EXISTS city_data_au AFTER UPDATE ON city_data BEGIN
                            INSERT INTO city_search(city_search, rowid, name, ascii_name, country, state)
                            VALUES ('delete', old.id, old.name, old.ascii_name, old.country, old.state);
                            INSERT INTO city_search(rowid, name, ascii_name, country, state)
                            VALUES (new.id, new.name, new.ascii_name, new.country, new.state);
                        END;
                        """)
                    
                    logger.info("Created FTS5 virtual table for city search")
                else:
                    logger.warning("FTS5 is not available in this SQLite build. Falling back to LIKE queries.")
            except Exception as e:
                logger.warning(f"Could not create FTS5 virtual table: {str(e)}")
                
        elif self.db_manager.db_type == 'postgresql':
            # Add tsvector column and GIN index for PostgreSQL
            try:
                with self.db_manager.cursor() as cursor:
                    # Check if the tsvector column already exists
                    cursor.execute(f"""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = '{self.city_table_name}' AND column_name = 'search_vector'
                    """)
                    if not cursor.fetchone():
                        # Add the tsvector column
                        cursor.execute(f"""
                        ALTER TABLE {self.city_table_name} 
                        ADD COLUMN search_vector tsvector
                        """)
                        
                        # Populate the tsvector column
                        cursor.execute(f"""
                        UPDATE {self.city_table_name} 
                        SET search_vector = to_tsvector('english', 
                            coalesce(name,'') || ' ' || 
                            coalesce(ascii_name,'') || ' ' || 
                            coalesce(country,'') || ' ' || 
                            coalesce(state,'')
                        )
                        """)
                        
                        # Create GIN index on the tsvector column
                        cursor.execute(f"""
                        CREATE INDEX idx_city_search ON {self.city_table_name} 
                        USING GIN(search_vector)
                        """)
                        
                        # Create trigger to keep the tsvector column up to date
                        cursor.execute(f"""
                        CREATE OR REPLACE FUNCTION city_search_trigger() RETURNS trigger AS $$
                        BEGIN
                            NEW.search_vector = to_tsvector('english', 
                                coalesce(NEW.name,'') || ' ' || 
                                coalesce(NEW.ascii_name,'') || ' ' || 
                                coalesce(NEW.country,'') || ' ' || 
                                coalesce(NEW.state,'')
                            );
                            RETURN NEW;
                        END
                        $$ LANGUAGE plpgsql;
                        """)
                        
                        cursor.execute(f"""
                        CREATE TRIGGER tsvector_update_trigger BEFORE INSERT OR UPDATE
                        ON {self.city_table_name} FOR EACH ROW
                        EXECUTE FUNCTION city_search_trigger();
                        """)
                        
                        logger.info("Created tsvector column and GIN index for PostgreSQL full-text search")
                    else:
                        logger.info("tsvector column already exists")
            except Exception as e:
                logger.warning(f"Could not create tsvector column and GIN index: {str(e)}")
    
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