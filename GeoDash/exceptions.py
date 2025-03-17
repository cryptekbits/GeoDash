"""
Custom exceptions for the GeoDash package.

This module defines a hierarchical exception system that provides:
1. Specific, technical error information for debugging and logging
2. User-friendly error messages for end-users
3. Error codes for consistent error identification
4. Optional context information for additional debugging
"""

from typing import Optional, Dict, Any, Type
import traceback
import sys

class GeoDataError(Exception):
    """Base exception for all GeoDash errors."""
    
    # Default values
    status_code = 500
    error_code = "GD-GENERIC-ERROR"
    user_message = "An unexpected error occurred."
    
    def __init__(
        self, 
        message: str = None,
        user_message: str = None,
        error_code: str = None,
        status_code: int = None,
        context: Dict[str, Any] = None,
        cause: Exception = None,
        include_traceback: bool = True
    ):
        # Technical message for logs
        self.message = message or self.__class__.__doc__ or "An error occurred."
        super().__init__(self.message)
        
        # User-friendly message
        self.user_message = user_message or self.__class__.user_message
        
        # Error codes
        self.error_code = error_code or self.__class__.error_code
        self.status_code = status_code or self.__class__.status_code
        
        # Additional context
        self.context = context or {}
        self.cause = cause
        
        # Capture traceback if requested
        self.traceback = None
        if include_traceback:
            self.traceback = traceback.format_exc() if sys.exc_info()[0] else None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the exception to a dictionary for API responses."""
        error_dict = {
            "error_code": self.error_code,
            "message": self.user_message,
            "status_code": self.status_code,
        }
        
        # Include technical details only in debug mode or for logging
        if 'debug' in self.context and self.context.get('debug'):
            error_dict["technical_details"] = {
                "message": self.message,
                "context": self.context,
            }
            if self.traceback:
                error_dict["technical_details"]["traceback"] = self.traceback
            if self.cause:
                error_dict["technical_details"]["cause"] = str(self.cause)
                
        return error_dict


# Database Errors - 1000 range
class DatabaseError(GeoDataError):
    """Base exception for all database-related errors."""
    status_code = 500
    error_code = "GD-DB-1000"
    user_message = "A database error occurred."


class ConnectionError(DatabaseError):
    """Exception raised when there's an error connecting to the database."""
    error_code = "GD-DB-1001"
    user_message = "Unable to connect to the database. Please try again later."


class QueryError(DatabaseError):
    """Exception raised when there's an error executing a database query."""
    error_code = "GD-DB-1002"
    user_message = "An error occurred while processing your request."


class TransactionError(DatabaseError):
    """Exception raised when there's an error during a database transaction."""
    error_code = "GD-DB-1003"
    user_message = "The operation could not be completed. Please try again."


class ConfigurationError(DatabaseError):
    """Exception raised when there's an error in database configuration."""
    error_code = "GD-DB-1004"
    user_message = "The system is incorrectly configured. Please contact support."


# Data Errors - 2000 range
class DataError(GeoDataError):
    """Base exception for all data-related errors."""
    status_code = 400
    error_code = "GD-DATA-2000"
    user_message = "An error occurred with the requested data."


class DataImportError(DataError):
    """Exception raised when there's an error importing data."""
    error_code = "GD-DATA-2001"
    user_message = "Unable to import the requested data. Please check the data format."


class DataNotFoundError(DataError):
    """Exception raised when requested data is not found."""
    status_code = 404
    error_code = "GD-DATA-2002"
    user_message = "The requested resource could not be found."


class ValidationError(DataError):
    """Exception raised when data validation fails."""
    status_code = 400
    error_code = "GD-DATA-2003"
    user_message = "The provided data is invalid. Please check your input."


# API Errors - 3000 range
class APIError(GeoDataError):
    """Base exception for all API-related errors."""
    status_code = 400
    error_code = "GD-API-3000"
    user_message = "An API error occurred while processing your request."


class AuthenticationError(APIError):
    """Exception raised when authentication fails."""
    status_code = 401
    error_code = "GD-API-3001"
    user_message = "Authentication failed. Please check your credentials."


class AuthorizationError(APIError):
    """Exception raised when a user doesn't have permission for an operation."""
    status_code = 403
    error_code = "GD-API-3002"
    user_message = "You don't have permission to perform this operation."


class RateLimitError(APIError):
    """Exception raised when API rate limits are exceeded."""
    status_code = 429
    error_code = "GD-API-3003"
    user_message = "Rate limit exceeded. Please try again later."


class InvalidParameterError(APIError):
    """Exception raised when API parameters are invalid."""
    status_code = 400
    error_code = "GD-API-3004"
    user_message = "Invalid parameters provided. Please check your request."


# System Errors - 4000 range
class SystemError(GeoDataError):
    """Base exception for all system-related errors."""
    status_code = 500
    error_code = "GD-SYS-4000"
    user_message = "A system error occurred. Please try again later."


class ConfigError(SystemError):
    """Exception raised when there's an error in system configuration."""
    error_code = "GD-SYS-4001"
    user_message = "The system is incorrectly configured. Please contact support."


class ResourceError(SystemError):
    """Exception raised when system resources are unavailable."""
    error_code = "GD-SYS-4002"
    user_message = "System resources are currently unavailable. Please try again later." 