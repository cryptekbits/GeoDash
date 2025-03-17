"""
Data importer module for the GeoDash package.

This module provides functionality for importing city data from various sources
into the GeoDash database.
"""

import os
import time
import pandas as pd
import urllib.request
import sys
from typing import Dict, List, Any, Optional, Tuple, Union, Set, Iterator, TextIO, cast
from pathlib import Path

from GeoDash.data.database import DatabaseManager
from GeoDash.exceptions import DataImportError, DataNotFoundError, ValidationError
from GeoDash.utils.logging import get_logger

# Get a logger for this module
logger = get_logger(__name__)

def get_data_directory() -> str:
    """
    Get the directory where GeoDash data is stored.
    
    This function checks the following locations in order:
    1. GEODASH_DATA_DIR environment variable
    2. ~/.geodash/data directory
    3. A directory within the package
    
    Returns:
        Path to the data directory (will be created if it doesn't exist)
    """
    # 1. Check environment variable first
    if 'GEODASH_DATA_DIR' in os.environ:
        data_dir = os.environ['GEODASH_DATA_DIR']
        os.makedirs(data_dir, exist_ok=True)
        return data_dir
    
    # 2. Check ~/.geodash/data
    home_data_dir = os.path.join(os.path.expanduser('~'), '.geodash', 'data')
    if os.path.isdir(home_data_dir):
        return home_data_dir
        
    # 3. Fall back to package directory
    # Get the base directory where the module is installed
    if hasattr(sys, 'frozen'):
        # For PyInstaller
        base_dir = os.path.dirname(sys.executable)
    else:
        try:
            # For regular Python
            base_dir = os.path.dirname(os.path.abspath(sys.modules['GeoDash'].__file__))
        except (KeyError, AttributeError):
            # Module not found, use directory of this file
            base_dir = os.path.dirname(os.path.abspath(__file__))
    
    package_data_dir = os.path.join(base_dir, 'data')
    os.makedirs(package_data_dir, exist_ok=True)
    return package_data_dir

def download_city_data(force: bool = False, url: Optional[str] = None) -> str:
    """
    Download city data from the internet and save it to the data directory.
    
    Args:
        force: If True, force download even if the file already exists
        url: URL to download from. If None, uses a default URL.
        
    Returns:
        Path to the downloaded file
        
    Raises:
        DataImportError: If the download fails
    """
    # Get the data directory
    data_dir = get_data_directory()
    
    logger.info(f"Using data directory: {data_dir}")
    
    csv_path = os.path.join(data_dir, 'cities.csv')
    
    # Check if file already exists and force is False
    if os.path.exists(csv_path) and not force:
        logger.info(f"Cities data already exists at {csv_path}")
        return csv_path
    
    # Download the file only if it doesn't exist or force=True
    if url is None:
        url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/csv/cities.csv"
    
    try:
        logger.info(f"Downloading cities.csv from {url} to {csv_path}...")
        urllib.request.urlretrieve(url, csv_path)
        logger.info("Download complete!")
        return csv_path
    except Exception as e:
        logger.error(f"Failed to download cities.csv: {e}")
        raise DataImportError(f"Failed to download cities.csv: {e}")

class CityDataImporter:
    """
    A class to import city data from various sources into the GeoDash database.
    """
    
    def __init__(self, db_manager: DatabaseManager) -> None:
        """
        Initialize the CityDataImporter.
        
        Args:
            db_manager: The database manager to use for import operations
        """
        self.db_manager = db_manager
    
    def import_from_csv(self, csv_path: Optional[str] = None, batch_size: int = 1000, download_if_missing: bool = True) -> int:
        """
        Import city data from a CSV file.
        
        Args:
            csv_path: Path to the CSV file to import.
                If None, attempts to find a default file.
            batch_size: Number of records to import at once.
            download_if_missing: Whether to download the CSV file if not found.
            
        Returns:
            Number of cities imported.
            
        Raises:
            DataImportError: If the import fails
            DataNotFoundError: If the CSV file is not found and download_if_missing is False
        """
        start_time = time.time()
        
        if csv_path is None:
            # Try to find the CSV file
            csv_path = self._find_csv_file()
            
            # If still not found and download_if_missing is True, download it
            if csv_path is None and download_if_missing:
                logger.info("CSV file not found. Downloading...")
                csv_path = download_city_data(force=True)
            
            # If still not found, raise an error
            if csv_path is None:
                raise DataNotFoundError("City data CSV file not found and download not allowed.")
        
        # Ensure the CSV file exists
        if not os.path.exists(csv_path):
            if download_if_missing:
                logger.warning(f"CSV file not found at {csv_path}. Downloading...")
                csv_path = download_city_data(force=True)
            else:
                raise DataNotFoundError(f"City data CSV file not found at {csv_path}")
        
        logger.info(f"Importing city data from {csv_path}")
        
        # Read the CSV file
        try:
            # Use pandas to read the CSV file
            logger.info("Reading CSV file...")
            
            # Load in chunks to save memory for large files
            chunk_reader = pd.read_csv(
                csv_path, 
                chunksize=batch_size,
                encoding='utf-8',
                low_memory=False
            )
            
            total_imported = 0
            for i, chunk in enumerate(chunk_reader):
                # Clean up the dataframe
                df = self._standardize_columns(chunk)
                
                # Import the chunk
                n_imported = self._import_dataframe(df, batch_size)
                total_imported += n_imported
                
                logger.info(f"Imported chunk {i+1} with {n_imported} cities. Total: {total_imported}")
            
            elapsed = time.time() - start_time
            logger.info(f"Successfully imported {total_imported} cities in {elapsed:.2f} seconds")
            
            # Update geometry column if using PostGIS
            if self.db_manager.db_type == 'postgresql':
                self._update_postgis_geometry()
            
            return total_imported
            
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Failed to import city data after {elapsed:.2f} seconds: {str(e)}")
            raise DataImportError(f"Failed to import city data: {str(e)}")
    
    def _find_csv_file(self) -> Optional[str]:
        """
        Find a city data CSV file in common locations.
        
        Returns:
            Path to the CSV file if found, None otherwise
        """
        # Check in the data directory
        data_dir = get_data_directory()
        csv_path = os.path.join(data_dir, 'cities.csv')
        if os.path.exists(csv_path):
            return csv_path
        
        # Check in the current directory
        csv_path = os.path.join(os.getcwd(), 'cities.csv')
        if os.path.exists(csv_path):
            return csv_path
        
        # Check in the package directory
        if hasattr(sys, 'frozen'):
            # For PyInstaller
            base_dir = os.path.dirname(sys.executable)
        else:
            try:
                # For regular Python
                base_dir = os.path.dirname(os.path.abspath(sys.modules['GeoDash'].__file__))
            except (KeyError, AttributeError):
                # Module not found, use directory of this file
                base_dir = os.path.dirname(os.path.abspath(__file__))
        
        csv_path = os.path.join(base_dir, 'cities.csv')
        if os.path.exists(csv_path):
            return csv_path
        
        return None
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize DataFrame columns to match the database schema.
        
        Args:
            df: DataFrame to standardize
            
        Returns:
            Standardized DataFrame
        """
        # Convert column names to lowercase
        df.columns = [col.lower() for col in df.columns]
        
        # Handle different column naming conventions in CSV files
        column_map = {
            # Original dataset naming
            'id': 'id',
            'name': 'name',
            'state_id': 'state_id',
            'state_code': 'state_code',
            'state_name': 'state_name',
            'country_id': 'country_id',
            'country_code': 'country_code',
            'country_name': 'country_name',
            'latitude': 'lat',
            'lat': 'lat',
            'longitude': 'lng',
            'lng': 'lng',
            'wikiDataId': 'wikidata_id',
            'wikidata_id': 'wikidata_id',
            'population': 'population',
            'timezone': 'timezone',
            'city_id': 'id',
            'city_name': 'name',
            'timezone_id': 'timezone',
            'iso2': 'country_code'
        }
        
        # Rename columns based on the mapping
        for src, dest in column_map.items():
            if src in df.columns and dest not in df.columns:
                df[dest] = df[src]
                
        # Ensure required columns exist
        required_columns = ['id', 'name', 'country_code', 'lat', 'lng']
        for col in required_columns:
            if col not in df.columns:
                logger.warning(f"Required column {col} missing from CSV")
                # Add empty column
                df[col] = None
                
        # Ensure all columns have the right type
        if 'id' in df.columns:
            df['id'] = pd.to_numeric(df['id'], errors='coerce')
        if 'lat' in df.columns:
            df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        if 'lng' in df.columns:
            df['lng'] = pd.to_numeric(df['lng'], errors='coerce')
        if 'population' in df.columns:
            df['population'] = pd.to_numeric(df['population'], errors='coerce')
            
        # Filter out invalid coordinates
        df = df[(df['lat'].notna()) & (df['lng'].notna())]
        df = df[(df['lat'] >= -90) & (df['lat'] <= 90)]
        df = df[(df['lng'] >= -180) & (df['lng'] <= 180)]
        
        # Filter out missing names
        df = df[df['name'].notna()]
        
        # Filter out missing country codes
        df = df[df['country_code'].notna()]
        
        return df
    
    def _import_dataframe(self, df: pd.DataFrame, batch_size: int) -> int:
        """
        Import a DataFrame into the database.
        
        Args:
            df: DataFrame to import
            batch_size: Number of records to import at once
            
        Returns:
            Number of records imported
        """
        # Convert DataFrame to list of dictionaries
        cities = df.to_dict('records')
        
        # Split into batches
        total_imported = 0
        for i in range(0, len(cities), batch_size):
            batch = cities[i:i+batch_size]
            
            # Filter invalid cities
            valid_batch = self._filter_valid_cities(batch)
            
            # Import the batch
            n_imported = self._import_batch(valid_batch)
            total_imported += n_imported
            
        return total_imported
    
    def _import_batch(self, batch: List[Dict[str, Any]]) -> int:
        """
        Import a batch of cities into the database.
        
        Args:
            batch: List of city dictionaries to import
            
        Returns:
            Number of cities imported
        """
        if not batch:
            return 0
            
        # Use the appropriate import method based on database type
        if self.db_manager.db_type == 'sqlite':
            return self._import_batch_sqlite(batch)
        elif self.db_manager.db_type == 'postgresql':
            return self._import_batch_postgresql(batch)
        else:
            logger.error(f"Unsupported database type: {self.db_manager.db_type}")
            return 0
    
    def _filter_valid_cities(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter out invalid cities from a batch.
        
        Args:
            batch: List of city dictionaries to filter
            
        Returns:
            List of valid city dictionaries
        """
        valid_cities = []
        for city in batch:
            # Check for required fields
            if not city.get('name') or not city.get('country_code'):
                continue
                
            # Check for valid coordinates
            try:
                lat = float(city.get('lat', 0))
                lng = float(city.get('lng', 0))
                
                if lat < -90 or lat > 90 or lng < -180 or lng > 180:
                    continue
            except (ValueError, TypeError):
                continue
                
            # Ensure ID is an integer
            try:
                if 'id' in city and city['id'] is not None:
                    city['id'] = int(city['id'])
            except (ValueError, TypeError):
                city['id'] = None
                
            # All checks passed, add to valid cities
            valid_cities.append(city)
            
        return valid_cities
    
    def _import_batch_sqlite(self, cities: List[Dict[str, Any]]) -> int:
        """
        Import a batch of cities into a SQLite database.
        
        Args:
            cities: List of city dictionaries to import
            
        Returns:
            Number of cities imported
        """
        if not cities:
            return 0
            
        # Prepare the SQL query
        columns = ['id', 'name', 'state_id', 'state_code', 'state_name', 
                   'country_id', 'country_code', 'country_name', 
                   'lat', 'lng', 'wikidata_id', 'population', 'timezone']
                   
        # Filter out None values for required fields
        placeholders = ", ".join(["?"] * len(columns))
        
        sql = f"""
        INSERT OR REPLACE INTO city_data ({", ".join(columns)})
        VALUES ({placeholders})
        """
        
        # Prepare the values
        values = []
        for city in cities:
            row = []
            for col in columns:
                row.append(city.get(col))
            values.append(tuple(row))
        
        # Execute the query
        try:
            with self.db_manager.cursor() as cursor:
                cursor.executemany(sql, values)
            return len(cities)
        except Exception as e:
            logger.error(f"Error importing batch to SQLite: {str(e)}")
            return 0
    
    def _import_batch_postgresql(self, cities: List[Dict[str, Any]]) -> int:
        """
        Import a batch of cities into a PostgreSQL database.
        
        Args:
            cities: List of city dictionaries to import
            
        Returns:
            Number of cities imported
        """
        if not cities:
            return 0
            
        # Prepare the SQL query
        columns = ['id', 'name', 'state_id', 'state_code', 'state_name', 
                   'country_id', 'country_code', 'country_name', 
                   'lat', 'lng', 'wikidata_id', 'population', 'timezone']
        
        # Create placeholders for values
        value_placeholders = []
        for i, _ in enumerate(cities):
            placeholders = []
            for j, _ in enumerate(columns):
                placeholders.append(f"${i*len(columns) + j + 1}")
            value_placeholders.append(f"({', '.join(placeholders)})")
        
        sql = f"""
        INSERT INTO city_data ({", ".join(columns)})
        VALUES {", ".join(value_placeholders)}
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            state_id = EXCLUDED.state_id,
            state_code = EXCLUDED.state_code,
            state_name = EXCLUDED.state_name,
            country_id = EXCLUDED.country_id,
            country_code = EXCLUDED.country_code,
            country_name = EXCLUDED.country_name,
            lat = EXCLUDED.lat,
            lng = EXCLUDED.lng,
            wikidata_id = EXCLUDED.wikidata_id,
            population = EXCLUDED.population,
            timezone = EXCLUDED.timezone
        """
        
        # Prepare values
        values = []
        for city in cities:
            for col in columns:
                values.append(city.get(col))
        
        # Execute the query
        try:
            with self.db_manager.cursor() as cursor:
                cursor.execute(sql, values)
            return len(cities)
        except Exception as e:
            logger.error(f"Error importing batch to PostgreSQL: {str(e)}")
            return 0

    def _update_postgis_geometry(self) -> None:
        """
        Update the geometry column in the PostgreSQL database.
        For PostGIS database only.
        """
        try:
            if self.db_manager.db_type != 'postgresql':
                return
                
            # Check if PostGIS is installed
            with self.db_manager.cursor() as cursor:
                cursor.execute("SELECT PostGIS_Version()")
                result = cursor.fetchone()
                if not result:
                    logger.warning("PostGIS not installed. Skipping geometry update.")
                    return
                    
                # Update the geometry column
                cursor.execute("""
                UPDATE city_data
                SET geom = ST_SetSRID(ST_MakePoint(lng, lat), 4326)
                WHERE geom IS NULL
                """)
                
            logger.info("Updated PostGIS geometry column")
        except Exception as e:
            logger.error(f"Error updating PostGIS geometry column: {str(e)}")

def clean_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Clean a single row of data.
    
    Args:
        row: Dictionary representing a row of data
        
    Returns:
        Cleaned row dictionary
    """
    # Current code removes rows with missing values but doesn't handle country_code specifically
    return row 