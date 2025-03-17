"""
Centralized logging configuration for the GeoDash package.

This module provides functions for configuring logging across the GeoDash package.
It allows for centralized control of logging behavior and exposes functions for
users to modify logging behavior (e.g., changing log levels).
"""

import json
import logging
import os
import platform
import socket
import sys
import time
import traceback
import uuid
from logging.handlers import RotatingFileHandler
from typing import Optional, Dict, Any, Union, List, cast

# Default logging format
DEFAULT_LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
DEFAULT_JSON_FORMAT = True

# Environment variable that can be used to set log level and format
LOG_LEVEL_ENV_VAR = 'GEODASH_LOG_LEVEL'
LOG_FORMAT_ENV_VAR = 'GEODASH_LOG_FORMAT'  # Can be 'json' or 'text'
LOG_FILE_ENV_VAR = 'GEODASH_LOG_FILE'

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

# Global service information
_service_info = {
    'service_name': 'geodash',
    'service_version': None,  # Will be populated at import time
    'hostname': socket.gethostname(),
    'os': platform.system(),
    'os_version': platform.release(),
}

class JsonFormatter(logging.Formatter):
    """Format log records as JSON for structured logging."""
    
    def __init__(self) -> None:
        super().__init__()
    
    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a JSON string."""
        log_data = {
            'timestamp': self.formatTime(record, '%Y-%m-%dT%H:%M:%S.%fZ'),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
        }
        
        # Add service info
        for key, value in _service_info.items():
            if value is not None:
                log_data[key] = value
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': self.formatException(record.exc_info),
            }
        
        # Add extra context if present
        if hasattr(record, 'extras') and record.extras:
            for key, value in record.extras.items():
                log_data[key] = value
        
        # Include any fields added by LoggerAdapter
        log_record_dict = record.__dict__
        for key, value in log_record_dict.items():
            if key.startswith('_') and key[1:] not in log_data and not key[1:].startswith('_'):
                log_data[key[1:]] = value
        
        return json.dumps(log_data)

class StructuredLoggerAdapter(logging.LoggerAdapter):
    """
    Adapter for structured logging with consistent fields.
    
    This adapter attaches additional context to log messages
    and ensures consistent field names across all logs.
    """
    
    def __init__(self, logger: logging.Logger, extra: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize the adapter with a logger and optional extra context.
        
        Args:
            logger: The underlying logger instance
            extra: Optional dictionary with extra context fields
        """
        self.logger = logger
        self.extra = extra or {}
        super().__init__(logger, self.extra)
    
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        """
        Process the logging message and keywords.
        
        Args:
            msg: The log message
            kwargs: Additional keyword arguments for the logging call
        
        Returns:
            Tuple of (msg, kwargs) with extra context added
        """
        # Merge existing extras with any passed in this call
        extras = self.extra.copy()
        
        if 'extra' in kwargs:
            extras.update(kwargs['extra'])
        
        # Ensure thread safety by not modifying the original kwargs
        kwargs_copy = kwargs.copy()
        kwargs_copy['extra'] = extras
        
        # Give the LogRecord an 'extras' attribute for the JsonFormatter
        if 'extras' not in kwargs_copy['extra']:
            kwargs_copy['extra']['extras'] = extras
            
        return msg, kwargs_copy
    
    # Add convenience methods for logging with structured data
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log a debug message with structured data."""
        self.log(logging.DEBUG, msg, *args, **kwargs)
    
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log an info message with structured data."""
        self.log(logging.INFO, msg, *args, **kwargs)
    
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log a warning message with structured data."""
        self.log(logging.WARNING, msg, *args, **kwargs)
    
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log an error message with structured data."""
        self.log(logging.ERROR, msg, *args, **kwargs)
    
    def critical(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log a critical message with structured data."""
        self.log(logging.CRITICAL, msg, *args, **kwargs)
    
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log an exception message with structured data and stack trace."""
        kwargs.setdefault('exc_info', True)
        self.log(logging.ERROR, msg, *args, **kwargs)
    
    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log a message with the specified level and structured data."""
        if self.isEnabledFor(level):
            self.logger._log(level, msg, args, **kwargs)

def configure_logging(level: Optional[Union[int, str]] = None, 
                     format_str: Optional[str] = None,
                     use_json: Optional[bool] = None,
                     log_file: Optional[str] = None) -> None:
    """
    Configure logging for the GeoDash package.
    
    This function sets up logging with standard formatting for all GeoDash
    loggers. It should be called early in the application lifecycle, 
    typically during import of the main package.
    
    Args:
        level: Log level to use (default: INFO or value from GEODASH_LOG_LEVEL env var)
        format_str: Log format string for text format (default: predefined format)
        use_json: Whether to use JSON structured logging (default: True)
        log_file: Optional path to a log file (if not set, logs to stderr)
    """
    global _logging_configured
    
    # If already configured, don't configure again unless specifically overriding
    if _logging_configured and level is None and format_str is None and use_json is None and log_file is None:
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
        
    # Determine if we should use JSON format
    if use_json is None:
        env_format = os.environ.get(LOG_FORMAT_ENV_VAR, '').lower()
        if env_format:
            use_json = env_format == 'json'
        else:
            use_json = DEFAULT_JSON_FORMAT
    
    # Check for log file from environment variable
    if log_file is None:
        log_file = os.environ.get(LOG_FILE_ENV_VAR)
    
    # Configure the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Remove existing handlers to avoid duplicate logs
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
    
    # Create and configure handlers
    handlers: List[logging.Handler] = []
    
    # Always add a console handler
    console_handler = logging.StreamHandler(sys.stderr)
    handlers.append(console_handler)
    
    # Add file handler if specified
    if log_file:
        try:
            file_handler = RotatingFileHandler(
                log_file, 
                maxBytes=10 * 1024 * 1024,  # 10 MB
                backupCount=5
            )
            handlers.append(file_handler)
        except Exception as e:
            # Don't fail if we can't create the log file
            print(f"Warning: Could not create log file {log_file}: {e}", file=sys.stderr)
    
    # Configure all handlers with the appropriate formatter
    for handler in handlers:
        if use_json:
            handler.setFormatter(JsonFormatter())
        else:
            handler.setFormatter(logging.Formatter(format_str))
        root_logger.addHandler(handler)
    
    # Mark as configured
    _logging_configured = True
    
    # Set service version if available
    try:
        from GeoDash import __version__
        _service_info['service_version'] = __version__
    except ImportError:
        pass
    
    # Log the configuration at debug level
    log_mode = 'JSON structured' if use_json else 'text'
    root_logger.debug(f"Logging configured with level: {logging.getLevelName(level)}, format: {log_mode}")

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
    
    # Update the root logger level
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Log the change at the new level
    if level <= logging.INFO:
        root_logger.info(f"Log level set to: {logging.getLevelName(level)}")

def get_logger(name: str, extra: Optional[Dict[str, Any]] = None) -> Union[logging.Logger, StructuredLoggerAdapter]:
    """
    Get a logger for the specified name with the GeoDash configuration.
    
    This is a convenience function that ensures the logger follows GeoDash conventions.
    It returns a StructuredLoggerAdapter if JSON logging is enabled, otherwise a standard Logger.
    
    Args:
        name: Name for the logger, typically __name__ of the calling module
        extra: Optional dictionary with extra context fields for structured logging
        
    Returns:
        A configured logger or logger adapter
    
    Example:
        >>> from GeoDash.utils.logging import get_logger
        >>> logger = get_logger(__name__)
        >>> logger.info("Processing cities")
        
        # With structured fields:
        >>> logger = get_logger(__name__, {'component': 'data_import'})
        >>> logger.info("Importing city data", extra={'city_count': 1000})
    """
    # Ensure logging is configured
    configure_logging()
    
    # Get the underlying logger
    logger = logging.getLogger(name)
    
    # Determine if we're using JSON logging by checking the formatter on the first handler
    # of the root logger
    root_logger = logging.getLogger()
    if root_logger.handlers and isinstance(root_logger.handlers[0].formatter, JsonFormatter):
        # Wrap with the structured adapter for JSON logging
        return StructuredLoggerAdapter(logger, extra)
    
    return logger

def get_request_id() -> str:
    """
    Generate a unique request ID for tracing.
    
    Returns:
        A unique ID string
    """
    return str(uuid.uuid4())

# Configure logging when this module is imported
configure_logging() 