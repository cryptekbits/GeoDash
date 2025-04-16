"""
Configuration-related commands for the GeoDash CLI.

This module provides commands for interacting with the GeoDash configuration system,
including viewing, initializing, and validating configurations.
"""

import os
import yaml
import json
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List

import click

from GeoDash.config import get_config, validate_config
from GeoDash.config.defaults import DEFAULT_CONFIG
from GeoDash.utils.logging import get_logger

# Get logger for this module
logger = get_logger(__name__)

def config_show(format_type: str = 'yaml', section: Optional[str] = None) -> int:
    """
    Display the current active configuration.
    
    Args:
        format_type: Output format (yaml or json)
        section: Optional section to display (e.g., 'database', 'logging')
        
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        config = get_config()
        
        # Get the entire config or just a section
        if section:
            config_data = config.get(section)
            if config_data is None:
                click.echo(f"Error: Section '{section}' not found in configuration")
                return 1
        else:
            config_data = config.get_all()
        
        # Output in the requested format
        if format_type.lower() == 'json':
            click.echo(json.dumps(config_data, indent=2))
        else:  # Default to YAML
            click.echo(yaml.dump(config_data, default_flow_style=False))
            
        return 0
        
    except Exception as e:
        logger.error(f"Error displaying configuration: {str(e)}")
        return 1

def config_init(output_path: Optional[str] = None) -> int:
    """
    Create a template configuration file with explanatory comments.
    
    Args:
        output_path: Path where to create the template file
        
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        # Set default output path if not provided
        if not output_path:
            output_path = os.path.join(os.getcwd(), 'geodash.yml')
        
        output_path = Path(output_path)
        
        # Check if file already exists
        if output_path.exists():
            click.confirm(f"File {output_path} already exists. Overwrite?", abort=True)
        
        # Ensure directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create template with comments
        template = _create_config_template()
        
        # Write to file
        with open(output_path, 'w') as f:
            f.write(template)
            
        click.echo(f"Configuration template created at: {output_path}")
        return 0
        
    except Exception as e:
        logger.error(f"Error creating configuration template: {str(e)}")
        return 1

def config_validate(config_path: str) -> int:
    """
    Validate a configuration file.
    
    Args:
        config_path: Path to the configuration file to validate
        
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        # Convert to Path object
        path = Path(config_path)
        
        # Check if file exists
        if not path.exists():
            click.echo(f"Error: Configuration file not found: {path}")
            return 1
            
        # Determine file format from extension
        if path.suffix.lower() in ('.yaml', '.yml'):
            with open(path, 'r') as f:
                config_data = yaml.safe_load(f)
        elif path.suffix.lower() == '.json':
            with open(path, 'r') as f:
                config_data = json.load(f)
        else:
            click.echo(f"Error: Unsupported file format: {path.suffix}")
            return 1
        
        # Validate the configuration
        errors = validate_config(config_data)
        
        if not errors:
            click.echo(f"Configuration file is valid: {path}")
            return 0
        else:
            click.echo(f"Configuration validation errors:")
            for section, section_errors in errors.items():
                for error in section_errors:
                    click.echo(f"  - {section}: {error}")
            return 1
            
    except Exception as e:
        logger.error(f"Error validating configuration: {str(e)}")
        return 1

def _create_config_template() -> str:
    """
    Create a template configuration file with explanatory comments.
    
    Returns:
        YAML string with the template configuration
    """
    template = """# GeoDash Configuration File
# This is a template configuration file for GeoDash with explanatory comments.
# You can customize this file to fit your needs or use the defaults.

# Database Configuration
database:
  # Database type: 'sqlite' or 'postgresql'
  type: sqlite
  
  # SQLite Configuration (used when type is 'sqlite')
  sqlite:
    # Path to SQLite database file (null for in-memory database)
    path: null
    # Enable R-Tree extension for spatial queries
    rtree: true
    # Enable FTS extension for full-text search
    fts: true
  
  # PostgreSQL Configuration (used when type is 'postgresql')
  postgresql:
    host: localhost
    port: 5432
    database: geodash
    # Authentication (null for system authentication)
    user: null
    password: null
    # Enable PostGIS extension for spatial queries
    postgis: true
  
  # Connection Pool Configuration
  pool:
    enabled: true
    min_size: 2
    max_size: 10
    timeout: 30  # seconds

# Search Configuration
search:
  # Fuzzy Search Configuration
  fuzzy:
    # Similarity threshold (0-100)
    threshold: 70
    enabled: true
  
  # Location-Aware Search Configuration
  location_aware:
    enabled: true
    # Weight of distance in search results (0.0-1.0)
    distance_weight: 0.3
    # Boost for exact country matches
    country_boost: 25000
  
  # Search Cache Configuration
  cache:
    enabled: true
    # Maximum number of cached results
    size: 5000
    # Time-to-live in seconds
    ttl: 3600
  
  # Search Results Limits
  limits:
    # Default number of results
    default: 10
    # Maximum number of results allowed
    max: 100

# Logging Configuration
logging:
  # Logging level (debug, info, warning, error, critical)
  level: info
  # Log format (json, text)
  format: json
  # Log file path (null for console only)
  file: null

# Feature Flags
features:
  enable_fuzzy_search: true
  enable_location_aware: true
  enable_memory_caching: true
  enable_shared_memory: true
  enable_advanced_db: true
  auto_fetch_data: true

# Data Configuration
data:
  # Data file location (null for default)
  location: null
  # Countries to include (comma-separated list or 'ALL')
  countries: ALL
  # URL for downloading city data
  download_url: https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/csv/cities.csv
  # Batch size for database operations
  batch_size: 5000

# GeoDash Mode (simple, advanced)
mode: advanced
"""
    return template 