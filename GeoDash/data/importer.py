"""
Data importer module for the GeoDash package.

This module provides functionality for importing city data from various sources
into the GeoDash database.
"""

import os
import logging
import time
import pandas as pd
import urllib.request
import sys
from typing import Dict, List, Any, Optional, Tuple, Union, Set, Iterator, TextIO
from pathlib import Path

from GeoDash.data.database import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
        Exception: If the download fails
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
        raise Exception(f"Failed to download cities.csv: {e}")

class CityDataImporter:
    """
    A class to import city data into the GeoDash database.
    
    This class handles the importing of city data from CSV files and other sources
    into the GeoDash database.
    """
    
    def __init__(self, db_manager: DatabaseManager) -> None:
        """
        Initialize the CityDataImporter with a database manager.
        
        Args:
            db_manager: The database manager to use for data import
        """
        self.db_manager = db_manager
        self.table_name = 'city_data'
    
    def import_from_csv(self, csv_path: Optional[str] = None, batch_size: int = 1000, download_if_missing: bool = True) -> int:
        """
        Import city data from a CSV file.
        
        Args:
            csv_path: Path to the CSV file. If None, tries to find it in standard locations.
            batch_size: Number of records to insert in each batch.
            download_if_missing: If True, tries to download the CSV file if not found.
            
        Returns:
            Number of records imported.
            
        Raises:
            FileNotFoundError: If the CSV file is not found and cannot be downloaded.
        """
        # Find the CSV file if not provided
        if csv_path is None:
            try:
                csv_path = self._find_csv_file()
                logger.info(f"Found local city data file at {csv_path}")
            except FileNotFoundError:
                if download_if_missing:
                    logger.info("Local city data file not found. Attempting to download...")
                    try:
                        csv_path = download_city_data()
                    except Exception as e:
                        raise FileNotFoundError(f"City data CSV file not found and download failed: {str(e)}")
                else:
                    raise FileNotFoundError("City data CSV file not found and download_if_missing is False")
        
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"City data CSV file not found at {csv_path}")
        
        logger.info(f"Importing city data from {csv_path}")
        
        # Check if table already has data
        with self.db_manager.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            count = cursor.fetchone()[0]
            
            if count > 0:
                logger.info(f"Table {self.table_name} already contains {count} records. Clearing table before import.")
                cursor.execute(f"DELETE FROM {self.table_name}")
        
        # Read the CSV file with pandas
        start_time = time.time()
        try:
            # Use keep_default_na=False to prevent "NA" from being interpreted as NaN
            df = pd.read_csv(csv_path, encoding='utf-8', keep_default_na=False, na_values=[''])
        except UnicodeDecodeError:
            # Try with different encodings if UTF-8 fails
            logger.warning("UTF-8 encoding failed, trying with ISO-8859-1")
            df = pd.read_csv(csv_path, encoding='ISO-8859-1', keep_default_na=False, na_values=[''])
        
        # Check for required columns
        required_columns = ['id', 'name', 'country_name', 'latitude', 'longitude']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"CSV file is missing required columns: {', '.join(missing_columns)}")
        
        # Remove rows with missing required values
        original_count = len(df)
        df = df.dropna(subset=['name', 'country_name', 'latitude', 'longitude'])
        if len(df) < original_count:
            logger.warning(f"Removed {original_count - len(df)} rows with missing required values")
        
        # Standardize column names
        df = self._standardize_columns(df)
        
        # Process and import the data in batches
        total_imported = self._import_dataframe(df, batch_size)
        
        end_time = time.time()
        logger.info(f"Imported {total_imported} cities in {end_time - start_time:.2f} seconds")
        
        # Update PostGIS geometry if PostgreSQL
        if self.db_manager.db_type == 'postgresql':
            self._update_postgis_geometry()
        
        return total_imported
    
    def _find_csv_file(self) -> str:
        """
        Find the city data CSV file in standard locations.
        
        Returns:
            Path to the CSV file.
            
        Raises:
            FileNotFoundError: If the CSV file is not found.
        """
        # Use the central get_data_directory function
        data_dir = get_data_directory()
        csv_path = os.path.join(data_dir, 'cities.csv')
        
        if os.path.isfile(csv_path):
            logger.info(f"Found city data at: {csv_path}")
            return csv_path
            
        # If not found in the primary location, check a few more standard locations
        possible_paths = [
            # Direct module path
            os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cities.csv'),
            # Up one level
            os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'cities.csv'),
        ]
        
        # Search for the file in possible locations
        for path in possible_paths:
            if os.path.isfile(path):
                logger.info(f"Found city data at: {path}")
                return path
        
        raise FileNotFoundError("City data CSV file not found in any standard locations")
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names in the dataframe.
        
        Args:
            df: The dataframe to standardize.
            
        Returns:
            Standardized dataframe.
        """
        # Define column mapping based on the actual CSV structure
        column_mapping = {
            'id': 'id',
            'name': 'name',
            'state_id': 'state_id', 
            'state_code': 'state_code',
            'state_name': 'state',
            'country_id': 'country_id',
            'country_code': 'country_code',
            'country_name': 'country',
            'latitude': 'lat',
            'longitude': 'lng',
            'wikiDataId': 'wiki_data_id'
        }
        
        # Rename columns that exist in the dataframe
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
        
        # Ensure required fields for schema
        if 'ascii_name' not in df.columns:
            df['ascii_name'] = df['name'].str.encode('ascii', errors='ignore').str.decode('ascii')
        
        # Fill missing values appropriately but maintain NULL values where they should be NULL
        # Only fill with empty strings for fields that allow NULL in the database schema
        df['state'] = df.get('state', pd.Series([None] * len(df)))
        df['state_code'] = df.get('state_code', pd.Series([None] * len(df)))
        df['country_id'] = df.get('country_id', pd.Series([None] * len(df)))
        df['state_id'] = df.get('state_id', pd.Series([None] * len(df)))
        df['wiki_data_id'] = df.get('wiki_data_id', pd.Series([None] * len(df)))
        
        # Handle missing country_code values by generating from country names
        if 'country_code' not in df.columns or df['country_code'].isna().any():
            # If country_code column doesn't exist, create it
            if 'country_code' not in df.columns:
                df['country_code'] = None
                
            # Find rows with missing country codes
            missing_code_mask = df['country_code'].isna()
            missing_count = missing_code_mask.sum()
            
            if missing_count > 0:
                logger.info(f"Generating country codes for {missing_count} cities with missing values")
                
                # Generate country code from country name (first 2 letters)
                for idx, row in df[missing_code_mask].iterrows():
                    if pd.notna(row['country']):
                        # Convert country name to a simple 2-letter code (not ISO standard, but better than NULL)
                        country_name = row['country'].strip()
                        # Use first two characters of country name, uppercase
                        country_code = country_name[:2].upper() if len(country_name) >= 2 else 'XX'
                        df.at[idx, 'country_code'] = country_code
                        logger.debug(f"Generated country code {country_code} for {country_name}")
                    else:
                        # If country is also missing, use placeholder
                        df.at[idx, 'country_code'] = 'XX'
                        logger.warning(f"Using placeholder country code XX for city with ID {row.get('id', 'unknown')}")
        
        return df
    
    def _import_dataframe(self, df: pd.DataFrame, batch_size: int) -> int:
        """
        Import a dataframe into the database.
        
        Args:
            df: The dataframe to import.
            batch_size: Number of records to insert in each batch.
            
        Returns:
            Number of records imported.
        """
        # Convert to records for batch insertion
        cities = df.to_dict(orient='records')
        total_imported = 0
        
        # Process in batches
        for i in range(0, len(cities), batch_size):
            batch = cities[i:i+batch_size]
            imported_count = self._import_batch(batch)
            total_imported += imported_count
            logger.info(f"Imported {min(i+batch_size, len(cities))}/{len(cities)} cities")
        
        return total_imported
    
    def _import_batch(self, batch: List[Dict[str, Any]]) -> int:
        """
        Import a batch of city records.
        
        Args:
            batch: List of city records to import.
            
        Returns:
            Number of records successfully imported.
        """
        # Filter out invalid records first
        valid_cities = self._filter_valid_cities(batch)
        
        if not valid_cities:
            return 0
            
        if self.db_manager.db_type == 'sqlite':
            return self._import_batch_sqlite(valid_cities)
        else:  # PostgreSQL
            return self._import_batch_postgresql(valid_cities)
    
    def _filter_valid_cities(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter out invalid city records.
        
        Args:
            batch: List of city records to filter.
            
        Returns:
            List of valid city records.
        """
        valid_cities = []
        for city in batch:
            # Skip records with missing essential data
            if not city.get('name'):
                logger.warning(f"Skipping city with ID {city.get('id')} due to missing name")
                continue
            
            # Skip records with missing country_code to avoid NULL constraint failure
            if not city.get('country_code'):
                logger.warning(f"Skipping city with ID {city.get('id')} due to missing country_code")
                continue
                
            valid_cities.append(city)
        
        return valid_cities
    
    def _import_batch_sqlite(self, cities: List[Dict[str, Any]]) -> int:
        """
        Import a batch of city records to SQLite.
        
        Args:
            cities: List of valid city records to import.
            
        Returns:
            Number of records successfully imported.
        """
        if not cities:
            return 0
            
        placeholders = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        query = f"""INSERT INTO {self.table_name} 
                 (id, name, ascii_name, state_id, state_code, state, country_id, country_code, country, lat, lng, wiki_data_id) 
                 VALUES {placeholders}"""
        
        imported_count = 0
        with self.db_manager.cursor() as cursor:
            for city in cities:
                try:
                    cursor.execute(query, (
                        city.get('id'),
                        city.get('name'),
                        city.get('ascii_name') or city.get('name'),
                        city.get('state_id'),
                        city.get('state_code'),
                        city.get('state'),
                        city.get('country_id'),
                        city.get('country_code'),
                        city.get('country'),
                        city.get('lat') or 0.0,
                        city.get('lng') or 0.0,
                        city.get('wiki_data_id')
                    ))
                    imported_count += 1
                except Exception as e:
                    logger.error(f"Error importing city {city.get('id')}: {str(e)}")
                    
        return imported_count
    
    def _import_batch_postgresql(self, cities: List[Dict[str, Any]]) -> int:
        """
        Import a batch of city records to PostgreSQL.
        
        Args:
            cities: List of valid city records to import.
            
        Returns:
            Number of records successfully imported.
        """
        if not cities:
            return 0
            
        imported_count = 0
        with self.db_manager.cursor() as cursor:
            for city in cities:
                try:
                    params = (
                        city.get('id'),
                        city.get('name'),
                        city.get('ascii_name') or city.get('name'),
                        city.get('state_id'),
                        city.get('state_code'),
                        city.get('state'),
                        city.get('country_id'),
                        city.get('country_code'),
                        city.get('country'),
                        city.get('lat') or 0.0,
                        city.get('lng') or 0.0,
                        city.get('wiki_data_id')
                    )
                    cursor.execute(f"""INSERT INTO {self.table_name} 
                                    (id, name, ascii_name, state_id, state_code, state, country_id, country_code, country, lat, lng, wiki_data_id) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", params)
                    imported_count += 1
                except Exception as e:
                    logger.error(f"Error importing city {city.get('id')}: {str(e)}")
                    
        return imported_count
    
    def _update_postgis_geometry(self):
        """
        Update PostGIS geometry column based on lat/lng values.
        """
        try:
            with self.db_manager.cursor() as cursor:
                # Check if PostGIS and geometry column exist
                cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'postgis'")
                if not cursor.fetchone():
                    return  # PostGIS not available
                
                cursor.execute(f"SELECT 1 FROM information_schema.columns WHERE table_name = '{self.table_name}' AND column_name = 'geom'")
                if not cursor.fetchone():
                    return  # Geometry column not available
                
                # Update geometry from lat/lng
                cursor.execute(f"UPDATE {self.table_name} SET geom = ST_SetSRID(ST_MakePoint(lng, lat), 4326) WHERE geom IS NULL")
                logger.info("Updated PostGIS geometry data")
        except Exception as e:
            logger.warning(f"Failed to update PostGIS geometry: {str(e)}")

def clean_row(row):
    # Current code removes rows with missing values but doesn't handle country_code specifically
    should_remove = any(row[field] in MISSING_VALUES for field in REQUIRED_FIELDS)
    return None if should_remove else row

# Proposed fix: Add country_code validation
REQUIRED_FIELDS = ['ascii_name', 'country', 'country_code', 'lat', 'lng']  # Ensure country_code is required 