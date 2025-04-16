"""
Schema management module for the GeoDash package.

This module provides schema definitions and management for the GeoDash database.
"""

from typing import List, Dict, Any, Optional, Tuple, Set, Union, cast
from GeoDash.data.database import DatabaseManager
from GeoDash.utils.logging import get_logger
from GeoDash.config.manager import get_config

# Get a logger for this module
logger = get_logger(__name__)

class SchemaManager:
    """
    A class to manage the database schema for GeoDash.
    
    This class handles the creation and management of tables and indexes
    for the GeoDash database.
    """
    
    def __init__(self, db_manager: DatabaseManager, config = None) -> None:
        """
        Initialize the SchemaManager with a database manager.
        
        Args:
            db_manager: The database manager to use for schema operations
            config: Configuration manager instance. If None, gets global instance.
        """
        self.db_manager = db_manager
        self.city_table_name = 'city_data'
        
        # Get config instance if not provided
        if config is None:
            config = get_config()
            
        self.config = config
    
    def ensure_schema_exists(self) -> None:
        """
        Ensure that the database schema exists, creating it if necessary.
        """
        if not self.db_manager.table_exists(self.city_table_name):
            logger.info(f"Table {self.city_table_name} does not exist. Creating schema.")
            self.create_schema()
            
            # Log information about R*Tree support for new databases
            if self.db_manager.db_type == 'sqlite':
                # Only check for R*Tree if the feature is enabled
                rtree_enabled = self.config.get("database.sqlite.rtree", True)
                if rtree_enabled:
                    has_rtree = self.db_manager.has_rtree_support()
                    if has_rtree:
                        logger.info("SQLite database initialized with R*Tree spatial index support.")
                        # Check if the R*Tree index was created
                        with self.db_manager.cursor() as cursor:
                            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='city_rtree'")
                            rtree_exists = cursor.fetchone()
                            if rtree_exists:
                                logger.info("R*Tree spatial index is ready for use.")
                            else:
                                logger.warning("R*Tree spatial index was not created during initialization.")
                    else:
                        logger.warning("SQLite database initialized without R*Tree support. Spatial queries will be slower.")
                else:
                    logger.info("R*Tree spatial indexing is disabled in configuration")
        else:
            logger.info(f"Table {self.city_table_name} already exists.")
            # Only ensure R*Tree is populated if the feature is enabled
            if self.db_manager.db_type == 'sqlite' and self.config.get("database.sqlite.rtree", True):
                self._ensure_rtree_populated()
    
    def _ensure_rtree_populated(self) -> None:
        """
        Ensure that the R*Tree index contains all city records.
        This is especially important if the R*Tree index was added after data was loaded.
        """
        if self.db_manager.db_type == 'sqlite':
            # Only proceed if R*Tree is enabled in config
            if not self.config.get("database.sqlite.rtree", True):
                logger.info("R*Tree is disabled in configuration. Skipping index population.")
                return
                
            try:
                # First check if R*Tree is supported in this SQLite build
                rtree_supported = self.db_manager.has_rtree_support()
                if not rtree_supported:
                    logger.warning("R*Tree is not supported in this SQLite build. Spatial queries will use the slower Haversine method.")
                    return
                
                with self.db_manager.cursor() as cursor:
                    # Check if R*Tree table exists
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='city_rtree'")
                    rtree_exists = cursor.fetchone()
                    
                    # Check if we have city data
                    cursor.execute("SELECT COUNT(*) FROM city_data")
                    city_count = cursor.fetchone()[0]
                    
                    if not rtree_exists and city_count > 0:
                        logger.info(f"Creating R*Tree spatial index for {city_count} existing cities")
                        
                        # Create the R*Tree table
                        cursor.execute(f'''
                        CREATE VIRTUAL TABLE IF NOT EXISTS city_rtree USING rtree(
                            id,             -- Integer primary key
                            min_lat, max_lat,  -- Latitude range
                            min_lng, max_lng   -- Longitude range
                        )
                        ''')
                        
                        # Create triggers to keep the R*Tree index updated
                        cursor.execute(f'''
                        CREATE TRIGGER IF NOT EXISTS city_rtree_insert AFTER INSERT ON {self.city_table_name}
                        BEGIN
                            INSERT INTO city_rtree VALUES (new.id, new.lat, new.lat, new.lng, new.lng);
                        END;
                        ''')
                        
                        cursor.execute(f'''
                        CREATE TRIGGER IF NOT EXISTS city_rtree_update AFTER UPDATE ON {self.city_table_name}
                        BEGIN
                            UPDATE city_rtree SET 
                                min_lat = new.lat, max_lat = new.lat,
                                min_lng = new.lng, max_lng = new.lng
                            WHERE id = new.id;
                        END;
                        ''')
                        
                        cursor.execute(f'''
                        CREATE TRIGGER IF NOT EXISTS city_rtree_delete AFTER DELETE ON {self.city_table_name}
                        BEGIN
                            DELETE FROM city_rtree WHERE id = old.id;
                        END;
                        ''')
                        
                        # Populate with all city data
                        cursor.execute(f'''
                        INSERT INTO city_rtree
                        SELECT id, lat, lat, lng, lng FROM {self.city_table_name}
                        ''')
                        
                        logger.info(f"Successfully created and populated R*Tree index for {city_count} cities")
                        return
                    
                    elif rtree_exists:
                        # Check for missing records in R*Tree
                        cursor.execute(f"""
                        SELECT COUNT(*) FROM {self.city_table_name} c 
                        LEFT JOIN city_rtree r ON c.id = r.id 
                        WHERE r.id IS NULL
                        """)
                        missing_count = cursor.fetchone()[0]
                        
                        if missing_count > 0:
                            logger.info(f"Found {missing_count} city records not in the R*Tree index. Adding them now.")
                            
                            # Add missing records to R*Tree
                            cursor.execute(f"""
                            INSERT INTO city_rtree
                            SELECT c.id, c.lat, c.lat, c.lng, c.lng
                            FROM {self.city_table_name} c
                            LEFT JOIN city_rtree r ON c.id = r.id
                            WHERE r.id IS NULL
                            """)
                            
                            logger.info("R*Tree index has been updated with all city records")
                        else:
                            logger.info("R*Tree spatial index is up-to-date")
                    
            except Exception as e:
                logger.warning(f"Error ensuring R*Tree index is populated: {str(e)}")
                import traceback
                logger.debug(traceback.format_exc())
    
    def create_schema(self) -> None:
        """
        Create the database schema, including tables and indexes.
        """
        self._create_city_table()
        self._create_city_indexes()
        self._create_search_optimizations()
        logger.info("Schema creation complete.")
    
    def _create_city_table(self) -> None:
        """
        Create the city_data table in the database.
        """
        if self.db_manager.db_type == 'sqlite':
            # SQLite table creation
            schema = '''
            CREATE TABLE city_data (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                ascii_name TEXT,
                state_id INTEGER,
                state_code TEXT,
                state_name TEXT,
                state TEXT,
                country_id INTEGER,
                country_code CHAR(2) NOT NULL,
                country_name TEXT,
                country TEXT,
                lat REAL NOT NULL,
                lng REAL NOT NULL,
                wikidata_id TEXT,
                population INTEGER,
                timezone TEXT
            )
            '''
        else:  # PostgreSQL
            # PostgreSQL table creation
            schema = '''
            CREATE TABLE city_data (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                ascii_name TEXT,
                state_id INTEGER,
                state_code TEXT,
                state_name TEXT,
                state TEXT,
                country_id INTEGER,
                country_code CHAR(2) NOT NULL,
                country_name TEXT,
                country TEXT,
                lat DOUBLE PRECISION NOT NULL,
                lng DOUBLE PRECISION NOT NULL,
                wikidata_id TEXT,
                population INTEGER,
                timezone TEXT
            )
            '''
        
        self.db_manager.create_table(self.city_table_name, schema)
        logger.info("Created city_data table")
    
    def _create_city_indexes(self) -> None:
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
        
        # Add SQLite R*Tree spatial index if using SQLite
        if self.db_manager.db_type == 'sqlite':
            # Only create R*Tree if enabled in config
            rtree_enabled = self.config.get("database.sqlite.rtree", True)
            
            if rtree_enabled:
                try:
                    # Use the method to check R*Tree support
                    rtree_supported = self.db_manager.has_rtree_support()
                    
                    if rtree_supported:
                        logger.info("Creating SQLite R*Tree spatial index for efficient spatial queries")
                        
                        with self.db_manager.cursor() as cursor:
                            # Create a virtual table using R*Tree for spatial indexing
                            cursor.execute(f'''
                            CREATE VIRTUAL TABLE IF NOT EXISTS city_rtree USING rtree(
                                id,             -- Integer primary key
                                min_lat, max_lat,  -- Latitude range
                                min_lng, max_lng   -- Longitude range
                            )
                            ''')
                            
                            # Create triggers to keep the R*Tree index updated
                            cursor.execute(f'''
                            CREATE TRIGGER IF NOT EXISTS city_rtree_insert AFTER INSERT ON {self.city_table_name}
                            BEGIN
                                INSERT INTO city_rtree VALUES (new.id, new.lat, new.lat, new.lng, new.lng);
                            END;
                            ''')
                            
                            cursor.execute(f'''
                            CREATE TRIGGER IF NOT EXISTS city_rtree_update AFTER UPDATE ON {self.city_table_name}
                            BEGIN
                                UPDATE city_rtree SET 
                                    min_lat = new.lat, max_lat = new.lat,
                                    min_lng = new.lng, max_lng = new.lng
                                WHERE id = new.id;
                            END;
                            ''')
                            
                            cursor.execute(f'''
                            CREATE TRIGGER IF NOT EXISTS city_rtree_delete AFTER DELETE ON {self.city_table_name}
                            BEGIN
                                DELETE FROM city_rtree WHERE id = old.id;
                            END;
                            ''')
                            
                            logger.info("Created R*Tree spatial index tables and triggers")
                    else:
                        logger.warning("R*Tree not supported in this SQLite build. Using slower spatial query methods.")
                except Exception as e:
                    logger.warning(f"Error creating R*Tree index: {str(e)}")
            else:
                logger.info("R*Tree spatial indexing is disabled in configuration")
        
        # Add PostGIS index if using PostgreSQL and enabled in config
        elif self.db_manager.db_type == 'postgresql':
            postgis_enabled = self.config.get("database.postgresql.postgis", True)
            
            if postgis_enabled and self.config.is_feature_enabled('enable_advanced_db'):
                try:
                    with self.db_manager.cursor() as cursor:
                        # Check if PostGIS extension is available
                        cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'postgis'")
                        has_postgis = cursor.fetchone()
                        
                        if not has_postgis:
                            # Try to create the extension
                            try:
                                cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis")
                                logger.info("Created PostGIS extension for advanced spatial queries")
                            except Exception as ext_error:
                                logger.warning(f"Could not create PostGIS extension: {str(ext_error)}")
                                logger.warning("PostGIS spatial index will not be created")
                                return
                        
                        # Create a GiST index for fast spatial queries
                        cursor.execute(f'''
                        CREATE INDEX IF NOT EXISTS idx_city_geography ON {self.city_table_name}
                        USING gist (ST_SetSRID(ST_MakePoint(lng, lat), 4326))
                        ''')
                        
                        logger.info("Created PostgreSQL spatial index using PostGIS")
                except Exception as e:
                    logger.warning(f"Error creating PostGIS index: {str(e)}")
            else:
                logger.info("PostGIS spatial indexing is disabled in configuration or advanced features are disabled")
    
    def _create_search_optimizations(self) -> None:
        """
        Create search optimizations for city data queries.
        """
        # Create full-text search indexes if enabled
        if self.db_manager.db_type == 'sqlite':
            # Check if FTS is enabled in config
            fts_enabled = self.config.get("database.sqlite.fts", True)
            
            if fts_enabled and self.config.is_feature_enabled('enable_advanced_db'):
                try:
                    with self.db_manager.cursor() as cursor:
                        # Create FTS5 virtual table for better text search
                        cursor.execute('''
                        CREATE VIRTUAL TABLE IF NOT EXISTS city_fts USING fts5(
                            name, ascii_name, state, country,
                            content='city_data',
                            content_rowid='id'
                        )
                        ''')
                        
                        # Create triggers to keep FTS index updated
                        cursor.execute(f'''
                        CREATE TRIGGER IF NOT EXISTS city_fts_insert AFTER INSERT ON {self.city_table_name}
                        BEGIN
                            INSERT INTO city_fts(rowid, name, ascii_name, state, country)
                            VALUES (new.id, new.name, new.ascii_name, new.state, new.country);
                        END;
                        ''')
                        
                        cursor.execute(f'''
                        CREATE TRIGGER IF NOT EXISTS city_fts_update AFTER UPDATE ON {self.city_table_name}
                        BEGIN
                            UPDATE city_fts SET
                                name = new.name,
                                ascii_name = new.ascii_name,
                                state = new.state,
                                country = new.country
                            WHERE rowid = new.id;
                        END;
                        ''')
                        
                        cursor.execute(f'''
                        CREATE TRIGGER IF NOT EXISTS city_fts_delete AFTER DELETE ON {self.city_table_name}
                        BEGIN
                            DELETE FROM city_fts WHERE rowid = old.id;
                        END;
                        ''')
                        
                        logger.info("Created SQLite FTS5 index for improved text search")
                except Exception as e:
                    logger.warning(f"Error creating FTS index: {str(e)}")
                    # Try FTS4 as fallback
                    try:
                        with self.db_manager.cursor() as cursor:
                            # Create FTS4 virtual table instead
                            cursor.execute('''
                            CREATE VIRTUAL TABLE IF NOT EXISTS city_fts USING fts4(
                                name, ascii_name, state, country,
                                content='city_data',
                                content_rowid='id'
                            )
                            ''')
                            
                            # Create triggers to keep FTS index updated
                            cursor.execute(f'''
                            CREATE TRIGGER IF NOT EXISTS city_fts_insert AFTER INSERT ON {self.city_table_name}
                            BEGIN
                                INSERT INTO city_fts(docid, name, ascii_name, state, country)
                                VALUES (new.id, new.name, new.ascii_name, new.state, new.country);
                            END;
                            ''')
                            
                            cursor.execute(f'''
                            CREATE TRIGGER IF NOT EXISTS city_fts_update AFTER UPDATE ON {self.city_table_name}
                            BEGIN
                                UPDATE city_fts SET
                                    name = new.name,
                                    ascii_name = new.ascii_name,
                                    state = new.state,
                                    country = new.country
                                WHERE docid = new.id;
                            END;
                            ''')
                            
                            cursor.execute(f'''
                            CREATE TRIGGER IF NOT EXISTS city_fts_delete AFTER DELETE ON {self.city_table_name}
                            BEGIN
                                DELETE FROM city_fts WHERE docid = old.id;
                            END;
                            ''')
                            
                            logger.info("Created SQLite FTS4 index as fallback for improved text search")
                    except Exception as e2:
                        logger.warning(f"Failed to create FTS4 fallback index: {str(e2)}")
            else:
                logger.info("Full-text search indexing is disabled in configuration or advanced features are disabled")
        
        # For PostgreSQL, create tsvector columns and GIN index if enabled
        elif self.db_manager.db_type == 'postgresql' and self.config.is_feature_enabled('enable_advanced_db'):
            try:
                with self.db_manager.cursor() as cursor:
                    # Add tsvector columns if they don't exist
                    cursor.execute(f'''
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_name = '{self.city_table_name}' AND column_name = 'search_vector'
                        ) THEN
                            ALTER TABLE {self.city_table_name} ADD COLUMN search_vector tsvector;
                            
                            -- Populate search vector
                            UPDATE {self.city_table_name} SET search_vector = 
                                setweight(to_tsvector('english', coalesce(name, '')), 'A') ||
                                setweight(to_tsvector('english', coalesce(ascii_name, '')), 'A') ||
                                setweight(to_tsvector('english', coalesce(state, '')), 'B') ||
                                setweight(to_tsvector('english', coalesce(country, '')), 'C');
                                
                            -- Create GIN index
                            CREATE INDEX IF NOT EXISTS idx_city_search_vector ON {self.city_table_name} USING GIN(search_vector);
                            
                            -- Create trigger for updates
                            CREATE OR REPLACE FUNCTION city_search_vector_update() RETURNS trigger AS $$
                            BEGIN
                                NEW.search_vector := 
                                    setweight(to_tsvector('english', coalesce(NEW.name, '')), 'A') ||
                                    setweight(to_tsvector('english', coalesce(NEW.ascii_name, '')), 'A') ||
                                    setweight(to_tsvector('english', coalesce(NEW.state, '')), 'B') ||
                                    setweight(to_tsvector('english', coalesce(NEW.country, '')), 'C');
                                RETURN NEW;
                            END
                            $$ LANGUAGE plpgsql;
                            
                            CREATE TRIGGER city_search_update
                            BEFORE INSERT OR UPDATE ON {self.city_table_name}
                            FOR EACH ROW EXECUTE FUNCTION city_search_vector_update();
                        END IF;
                    END $$;
                    ''')
                    
                    logger.info("Created PostgreSQL full-text search index with tsvector and GIN")
            except Exception as e:
                logger.warning(f"Error creating PostgreSQL search optimizations: {str(e)}")
    
    def get_table_info(self) -> Dict[str, Any]:
        """
        Get information about the city_data table.
        
        Returns:
            Dictionary with table information including columns and row count
        """
        info = {
            'table_name': self.city_table_name,
            'columns': [],
            'row_count': 0,
            'indexes': [],
            'database_type': self.db_manager.db_type
        }
        
        # Get column information
        with self.db_manager.cursor() as cursor:
            if self.db_manager.db_type == 'sqlite':
                # Get columns for SQLite
                cursor.execute(f"PRAGMA table_info({self.city_table_name})")
                for row in cursor.fetchall():
                    col_info = {
                        'name': row[1],
                        'type': row[2],
                        'notnull': bool(row[3]),
                        'pk': bool(row[5])
                    }
                    info['columns'].append(col_info)
                
                # Get row count for SQLite
                cursor.execute(f"SELECT COUNT(*) FROM {self.city_table_name}")
                info['row_count'] = cursor.fetchone()[0]
                
                # Get indexes for SQLite
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='{self.city_table_name}'")
                for row in cursor.fetchall():
                    info['indexes'].append(row[0])
                
            elif self.db_manager.db_type == 'postgresql':
                # Get columns for PostgreSQL
                cursor.execute(f"""
                SELECT column_name, data_type, is_nullable, 
                       CASE WHEN column_name IN (SELECT a.attname
                                              FROM pg_index i
                                              JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                                              WHERE i.indrelid = '{self.city_table_name}'::regclass AND i.indisprimary)
                            THEN TRUE ELSE FALSE END as is_pk
                FROM information_schema.columns
                WHERE table_name = '{self.city_table_name}'
                """)
                for row in cursor.fetchall():
                    col_info = {
                        'name': row[0],
                        'type': row[1],
                        'notnull': row[2] == 'NO',
                        'pk': row[3]
                    }
                    info['columns'].append(col_info)
                
                # Get row count for PostgreSQL
                cursor.execute(f"SELECT COUNT(*) FROM {self.city_table_name}")
                info['row_count'] = cursor.fetchone()[0]
                
                # Get indexes for PostgreSQL
                cursor.execute(f"""
                SELECT indexname
                FROM pg_indexes
                WHERE tablename = '{self.city_table_name}'
                """)
                for row in cursor.fetchall():
                    info['indexes'].append(row[0])
        
        return info 