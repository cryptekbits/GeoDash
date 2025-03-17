"""
Centralized logging configuration for the GeoDash package.

This module provides functions for configuring logging across the GeoDash package.
It allows for centralized control of logging behavior and exposes functions for
users to modify logging behavior (e.g., changing log levels).
"""

import logging
import os
from typing import Optional, Dict, Any, Union

# Default logging format
DEFAULT_LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Environment variable that can be used to set log level
LOG_LEVEL_ENV_VAR = 'GEODASH_LOG_LEVEL'

# Mapping of string log levels to logging module constants
LOG_LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
}

# Track if logging has been configured
_logging_configured = False

def configure_logging(level: Optional[Union[int, str]] = None, 
                      format_str: Optional[str] = None) -> None:
    """
    Configure logging for the GeoDash package.
    
    This function sets up logging with standard formatting for all GeoDash
    loggers. It should be called early in the application lifecycle, 
    typically during import of the main package.
    
    Args:
        level: Log level to use (default: INFO or value from GEODASH_LOG_LEVEL env var)
        format_str: Log format string (default: predefined format)
    """
    global _logging_configured
    
    # If already configured, don't configure again unless specifically overriding
    if _logging_configured and level is None and format_str is None:
        return
    
    # Determine log level
    if level is None:
        # Check for environment variable
        env_level = os.environ.get(LOG_LEVEL_ENV_VAR)
        if env_level:
            level = env_level.lower()
        else:
            level = logging.INFO
    
    # Convert string level to logging constant if needed
    if isinstance(level, str):
        level = LOG_LEVELS.get(level.lower(), logging.INFO)
    
    # Use default format if none provided
    if format_str is None:
        format_str = DEFAULT_LOG_FORMAT
    
    # Configure the root logger
    logging.basicConfig(
        level=level,
        format=format_str,
        force=True  # Ensure configuration is applied even if logging was previously configured
    )
    
    # Mark as configured
    _logging_configured = True
    
    # Log the configuration at debug level
    root_logger = logging.getLogger()
    root_logger.debug(f"Logging configured with level: {logging.getLevelName(level)}")

def set_log_level(level: Union[int, str]) -> None:
    """
    Set the log level for GeoDash loggers.
    
    This function allows users to change the logging level at runtime.
    
    Args:
        level: Log level to set. Can be a string ('debug', 'info', 'warning', 'error', 'critical')
               or a logging module constant (logging.DEBUG, logging.INFO, etc.)
    
    Example:
        >>> from GeoDash.utils.logging import set_log_level
        >>> set_log_level('debug')  # Show all debug messages
        >>> set_log_level(logging.WARNING)  # Only show warnings and above
    """
    # Convert string level to logging constant if needed
    if isinstance(level, str):
        level_str = level.lower()
        if level_str not in LOG_LEVELS:
            valid_levels = ", ".join(LOG_LEVELS.keys())
            raise ValueError(f"Invalid log level: {level}. Valid levels are: {valid_levels}")
        level = LOG_LEVELS[level_str]
    
    # Configure with the new level
    configure_logging(level=level)
    
    # Update the root logger level
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Log the change at the new level
    if level <= logging.INFO:
        root_logger.info(f"Log level set to: {logging.getLevelName(level)}")

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger for the specified name with the GeoDash configuration.
    
    This is a convenience function that ensures the logger follows GeoDash conventions.
    
    Args:
        name: Name for the logger, typically __name__ of the calling module
        
    Returns:
        A configured logger
    
    Example:
        >>> from GeoDash.utils.logging import get_logger
        >>> logger = get_logger(__name__)
        >>> logger.info("Processing cities")
    """
    # Ensure logging is configured
    configure_logging()
    
    # Return the named logger
    return logging.getLogger(name)

# Configure logging when this module is imported
configure_logging() 