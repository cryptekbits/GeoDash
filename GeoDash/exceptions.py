"""
Custom exceptions for the GeoDash package.

This module defines custom exceptions that provide more specific error information
than generic Python exceptions.
"""

class GeoDataError(Exception):
    """Base exception for all GeoDash errors."""
    pass


class DatabaseError(GeoDataError):
    """Base exception for all database-related errors."""
    pass


class ConnectionError(DatabaseError):
    """Exception raised when there's an error connecting to the database."""
    pass


class QueryError(DatabaseError):
    """Exception raised when there's an error executing a database query."""
    pass


class TransactionError(DatabaseError):
    """Exception raised when there's an error during a database transaction."""
    pass


class ConfigurationError(DatabaseError):
    """Exception raised when there's an error in database configuration."""
    pass


class DataImportError(GeoDataError):
    """Exception raised when there's an error importing data."""
    pass


class DataNotFoundError(GeoDataError):
    """Exception raised when requested data is not found."""
    pass


class ValidationError(GeoDataError):
    """Exception raised when data validation fails."""
    pass 