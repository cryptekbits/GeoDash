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
from typing import Dict, List, Any, Optional
from pathlib import Path

from GeoDash.data.database import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def download_city_data(force: bool = False) -> str:
    """
    Download the cities.csv file from the remote source.
    
    Args:
        force: If True, download even if the file already exists.
        
    Returns:
        Path to the downloaded CSV file.
        
    Raises:
        Exception: If download fails.
    """
    # Check multiple potential locations for the data directory
    possible_paths = [
        # Direct path in the current module
        os.path.dirname(os.path.abspath(__file__)),
        # Path relative to the package (installed mode)
        os.path.join(os.path.dirname(os.path.abspath(__file__))),
        # Path relative to the project root
        os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'GeoDash', 'data')
    ]
    
    # Get the base directory where the module is installed
    if hasattr(sys, 'frozen'):
        # For PyInstaller
        base_dir = os.path.dirname(sys.executable)
    else:
        # For regular Python
        base_dir = os.path.dirname(os.path.abspath(sys.modules['GeoDash'].__file__))
    
    possible_paths.append(os.path.join(base_dir, 'data'))
    
    # Find the first valid directory
    data_dir = None
    for path in possible_paths:
        if os.path.isdir(path):
            data_dir = path
            break
    
    # If no valid directory found, use the first option and create it
    if data_dir is None:
        data_dir = possible_paths[0]
        
    os.makedirs(data_dir, exist_ok=True)
    logger.info(f"Using data directory: {data_dir}")
    
    csv_path = os.path.join(data_dir, 'cities.csv')
    
    # Check if file already exists and force is False
    if os.path.exists(csv_path) and not force:
        logger.info(f"Cities data already exists at {csv_path}")
        return csv_path
    
    # Download the file
    csv_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/csv/cities.csv"
    
    try:
        logger.info(f"Downloading cities.csv from {csv_url} to {csv_path}...")
        urllib.request.urlretrieve(csv_url, csv_path)
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
    
    def __init__(self, db_manager: DatabaseManager):
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
            except FileNotFoundError:
                if download_if_missing:
                    logger.info("Attempting to download city data...")
                    try:
                        csv_path = download_city_data()
                    except Exception as e:
                        raise FileNotFoundError(f"City data CSV file not found and download failed: {str(e)}")
                else:
                    raise
        
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"City data CSV file not found at {csv_path}")
        
        logger.info(f"Importing city data from {csv_path}")
        
        # Read the CSV file with pandas
        start_time = time.time()
        df = pd.read_csv(csv_path, encoding='utf-8')
        
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
        # Check multiple potential locations for the data directory
        # Similar to what's used in download_city_data
        possible_paths = [
            # Direct module path
            os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cities.csv'),
            # Up one level
            os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'cities.csv'),
            # Up two levels
            os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data', 'cities.csv'),
            # Relative to the package in installed mode
            os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'cities.csv'),
        ]
        
        # Get the base directory where the module is installed
        if hasattr(sys, 'frozen'):
            # For PyInstaller
            base_dir = os.path.dirname(sys.executable)
        else:
            try:
                # For regular Python
                base_dir = os.path.dirname(os.path.abspath(sys.modules['GeoDash'].__file__))
                possible_paths.append(os.path.join(base_dir, 'data', 'cities.csv'))
            except (KeyError, AttributeError):
                # Module not found, skip this path
                pass
        
        # Try site-packages location for pip-installed package
        try:
            import site
            site_packages = site.getsitepackages()
            for site_path in site_packages:
                possible_paths.append(os.path.join(site_path, 'GeoDash', 'data', 'cities.csv'))
        except (ImportError, AttributeError):
            pass
        
        # Search for the file in possible locations
        for path in possible_paths:
            if os.path.isfile(path):
                logger.info(f"Found city data at: {path}")
                return path
        
        # If we get here, the file wasn't found
        logger.warning("City data file not found in any standard location")
        logger.info(f"Searched in: {possible_paths}")
        raise FileNotFoundError("City data file (cities.csv) not found in any standard location")
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names in the dataframe.
        
        Args:
            df: The dataframe to standardize.
            
        Returns:
            Standardized dataframe.
        """
        # Define column mapping
        column_mapping = {
            'city_id': 'id',
            'city_name': 'name',
            'city_ascii': 'ascii_name',
            'state_name': 'state',
            'state_code': 'state_code',
            'country_name': 'country',
            'iso2': 'country_code',
            'latitude': 'lat',
            'longitude': 'lng',
        }
        
        # Rename columns that exist in the dataframe
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
        
        # Fill missing values appropriately
        df['state'] = df.get('state', pd.Series([''] * len(df))).fillna('')
        df['state_code'] = df.get('state_code', pd.Series([''] * len(df))).fillna('')
        df['population'] = df.get('population', pd.Series([0] * len(df))).fillna(0)
        
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
            self._import_batch(batch)
            total_imported += len(batch)
            logger.info(f"Imported {min(i+batch_size, len(cities))}/{len(cities)} cities")
        
        return total_imported
    
    def _import_batch(self, batch: List[Dict[str, Any]]):
        """
        Import a batch of city records.
        
        Args:
            batch: List of city records to import.
        """
        if self.db_manager.db_type == 'sqlite':
            # SQLite batch insertion
            placeholders = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            query = f"INSERT INTO {self.table_name} (id, name, ascii_name, country, country_code, state, state_code, lat, lng, population) VALUES {placeholders}"
            
            with self.db_manager.cursor() as cursor:
                for city in batch:
                    cursor.execute(query, (
                        city.get('id', None),
                        city.get('name', ''),
                        city.get('ascii_name', ''),
                        city.get('country', ''),
                        city.get('country_code', ''),
                        city.get('state', ''),
                        city.get('state_code', ''),
                        city.get('lat', 0.0),
                        city.get('lng', 0.0),
                        city.get('population', 0)
                    ))
        else:  # PostgreSQL
            # PostgreSQL batch insertion
            with self.db_manager.cursor() as cursor:
                params_list = []
                for city in batch:
                    params = (
                        city.get('id', None),
                        city.get('name', ''),
                        city.get('ascii_name', ''),
                        city.get('country', ''),
                        city.get('country_code', ''),
                        city.get('state', ''),
                        city.get('state_code', ''),
                        city.get('lat', 0.0),
                        city.get('lng', 0.0),
                        city.get('population', 0)
                    )
                    params_list.append(params)
                
                args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", params).decode('utf-8') 
                                    for params in params_list)
                
                cursor.execute(f"INSERT INTO {self.table_name} (id, name, ascii_name, country, country_code, state, state_code, lat, lng, population) VALUES {args_str}")
    
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