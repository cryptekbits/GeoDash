"""
API server module for the GeoDash package.

This module provides a Flask-based API server for accessing city data
through RESTful API endpoints.
"""

import time
import json
from typing import Dict, Any, List, Union, Optional, Tuple, Callable
from functools import wraps
import os

from flask import Flask, request, jsonify, Response, g, current_app
import werkzeug.exceptions

from GeoDash.data.database import DatabaseManager
from GeoDash.data.repositories import cleanup_shared_memory
from GeoDash.services.city_service import CityService
from GeoDash.utils import log_error_with_github_info
from GeoDash.utils.logging import get_logger
from GeoDash.exceptions import GeoDataError, InvalidParameterError

# Get a logger for this module
logger = get_logger(__name__)

def format_response(data: Any = None, message: str = None, 
                   error: str = None, status_code: int = 200, 
                   meta: Dict[str, Any] = None,
                   error_code: str = None) -> Tuple[Dict[str, Any], int]:
    """
    Format API response in a standardized structure.
    
    Args:
        data: Response data payload
        message: Optional success message
        error: Optional error message
        status_code: HTTP status code
        meta: Optional metadata dictionary
        error_code: Optional error code identifier
        
    Returns:
        Tuple of (response_dict, status_code)
    """
    response = {
        'success': 200 <= status_code < 300,
        'status_code': status_code,
    }
    
    if data is not None:
        response['data'] = data
    
    if message:
        response['message'] = message
    
    if error:
        response['error'] = error
        
    if error_code:
        response['error_code'] = error_code
    
    if meta:
        response['meta'] = meta
    
    return response, status_code

def api_response(f: Callable) -> Callable:
    """
    Decorator to standardize API responses and handle exceptions.
    
    This decorator wraps API endpoint functions to ensure all responses
    follow the same structure and error handling patterns.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            # Call the original function
            result = f(*args, **kwargs)
            
            # Handle different return types
            if isinstance(result, tuple) and len(result) == 2:
                # Assume (data, status_code) format
                data, status_code = result
                if isinstance(data, dict) and 'error' in data:
                    # This is an error response
                    return format_response(
                        error=data.get('error'), 
                        message=data.get('message'),
                        status_code=status_code
                    )
                else:
                    return format_response(data=data, status_code=status_code)
            elif isinstance(result, dict):
                # Assume successful data response
                return format_response(data=result)
            else:
                # Just return whatever it is
                return format_response(data=result)
                
        except werkzeug.exceptions.HTTPException as e:
            # Handle HTTP exceptions (e.g. BadRequest, NotFound)
            logger.warning(f"HTTP exception in {f.__name__}: {str(e)}")
            return format_response(
                error=e.__class__.__name__,
                message=str(e.description),
                status_code=e.code
            )
        except Exception as e:
            # Log and handle unexpected exceptions
            log_error_with_github_info(e, f"Exception in endpoint {f.__name__}")
            logger.error(f"Unhandled exception in {f.__name__}: {str(e)}")
            return format_response(
                error="Internal Server Error",
                message="An unexpected error occurred",
                status_code=500
            )
    
    return decorated_function

def create_app(db_uri: Optional[str] = None, debug: bool = False) -> Flask:
    """
    Create and configure a Flask application instance.
    
    Args:
        db_uri: Optional database URI to use for the app
        debug: Enable debug mode with additional error information
        
    Returns:
        A configured Flask application
    """
    app = Flask(__name__)
    
    # Configure the app
    app.config.update(
        JSONIFY_PRETTYPRINT_REGULAR=True,
        JSON_SORT_KEYS=False,
        DEBUG=debug
    )
    
    # Store the database URI in the app config
    if db_uri:
        app.config['DATABASE_URI'] = db_uri
    
    # Worker identification
    worker_id = os.environ.get('GUNICORN_WORKER_ID', 'standalone')
    db_initialized = os.environ.get('GEODASH_DB_INITIALIZED') == '1'
    
    logger.info(f"Worker {worker_id}: Creating Flask application")
    
    # Create a persistent connection to the database for this worker
    try:
        start_time = time.time()
        
        # If no URI provided, use SQLite in data directory
        if db_uri is None:
            data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
            os.makedirs(data_dir, exist_ok=True)
            db_uri = f"sqlite:///{os.path.join(data_dir, 'cities.db')}"
        
        # Create a persistent CityService instance that will use pre-initialized data
        # if available from the master process
        city_service = CityService(db_uri=db_uri, persistent=True)
        
        # Store in app context for reuse in all requests
        app.config['CITY_SERVICE_INSTANCE'] = city_service
        
        # Check if data is properly loaded
        table_info = city_service.get_table_info()
        record_count = table_info.get('row_count', 0)
        init_time = time.time() - start_time
        
        app.config['INITIALIZED'] = True
        
        # Log some stats
        logger.info(f"Worker {worker_id}: Connected to database with {record_count} city records in {init_time:.2f}s")
        
        # Log memory usage if available
        try:
            import psutil
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            logger.info(f"Worker {worker_id}: Memory usage: {memory_info.rss / (1024 * 1024):.1f} MB")
        except ImportError:
            logger.debug("psutil not available for memory usage tracking")
        
    except Exception as e:
        log_error_with_github_info(e, f"Worker {worker_id}: Failed to initialize database")
        app.config['INITIALIZED'] = False
    
    # Configure JSON responses
    app.json.sort_keys = False
    app.json.ensure_ascii = False
    
    # Add before/after request handlers
    @app.before_request
    def before_request() -> None:
        """Set up request context with timing information."""
        g.start_time = time.time()
    
    @app.after_request
    def after_request(response: Response) -> Response:
        """Log request information and add timing headers."""
        # Calculate request duration
        if hasattr(g, 'start_time'):
            duration_ms = (time.time() - g.start_time) * 1000
            response.headers['X-Request-Duration-Ms'] = str(int(duration_ms))
            
            # Log request details
            logger.info(
                f"Request: {request.method} {request.path} | "
                f"Status: {response.status_code} | "
                f"Duration: {duration_ms:.2f}ms"
            )
        
        return response
    
    # Set up error handlers
    @app.errorhandler(werkzeug.exceptions.BadRequest)
    def handle_bad_request(error: werkzeug.exceptions.BadRequest) -> Tuple[Dict[str, Any], int]:
        """Handle bad request errors."""
        return format_response(
            error='Bad Request',
            message=str(error.description),
            status_code=400,
            error_code="GD-API-3004",  # Using our InvalidParameterError code
            meta={'debug_info': str(error)} if debug else None
        )
    
    @app.errorhandler(werkzeug.exceptions.NotFound)
    def handle_not_found(error: werkzeug.exceptions.NotFound) -> Tuple[Dict[str, Any], int]:
        """Handle not found errors."""
        return format_response(
            error='Not Found',
            message=str(error.description),
            status_code=404,
            error_code="GD-DATA-2002",  # Using our DataNotFoundError code
            meta={'debug_info': str(error)} if debug else None
        )
    
    @app.errorhandler(GeoDataError)
    def handle_geodata_error(error: GeoDataError) -> Tuple[Dict[str, Any], int]:
        """Handle GeoDash-specific exceptions."""
        # Log the technical details
        if hasattr(error, 'traceback') and error.traceback:
            logger.error(f"GeoDash Error: {error.error_code} - {error.message}\n{error.traceback}")
        else:
            logger.error(f"GeoDash Error: {error.error_code} - {error.message}")
            
        # Include debug context in the error response
        error.context['debug'] = debug
            
        # Get the error details as a dictionary
        error_dict = error.to_dict()
        
        # Return user-friendly response
        return format_response(
            error=error.__class__.__name__,
            message=error.user_message,
            status_code=error.status_code,
            error_code=error.error_code,
            meta=error_dict.get('technical_details') if debug else None
        )
    
    @app.errorhandler(Exception)
    def handle_exception(error: Exception) -> Tuple[Dict[str, Any], int]:
        """Handle generic exceptions."""
        # Use the GitHub issue reporting function for unhandled exceptions
        log_error_with_github_info(error, "Unhandled server exception")
        
        # Convert to a GeoDataError if it's not already one of our exceptions
        if not isinstance(error, GeoDataError):
            # Create a generic system error with the original exception as the cause
            error_msg = str(error)
            debug_context = {'debug': debug}
            
            system_error = GeoDataError(
                message=f"Unhandled exception: {error_msg}",
                user_message="An unexpected error occurred. Our team has been notified.",
                error_code="GD-SYS-5000",  # Special code for unhandled exceptions
                status_code=500,
                context=debug_context,
                cause=error
            )
            return handle_geodata_error(system_error)
        
        # If it's already a GeoDataError, just delegate to that handler
        return handle_geodata_error(error)
    
    # Register API routes
    register_routes(app)
    
    # Register teardown function to ensure shared memory is cleaned up
    @app.teardown_appcontext
    def teardown_shared_memory(exception=None):
        """Clean up shared memory when the application shuts down."""
        cleanup_shared_memory()
    
    return app

def get_city_service() -> CityService:
    """
    Get a CityService instance for the current application.
    
    Returns:
        CityService instance initialized with the application's DB URI
    """
    if hasattr(current_app, 'config') and 'CITY_SERVICE_INSTANCE' in current_app.config:
        return current_app.config['CITY_SERVICE_INSTANCE']
    
    # Fall back to creating a new instance if no shared one exists
    db_uri = current_app.config.get('DATABASE_URI')
    return CityService(db_uri=db_uri)

def validate_params(required_params: Optional[List[str]] = None,
                    numeric_params: Optional[List[str]] = None,
                    max_limit: Optional[int] = None):
    """
    Decorator for validating request parameters.
    
    Args:
        required_params: List of required parameter names
        numeric_params: List of parameters that must be numeric
        max_limit: Maximum value for the 'limit' parameter
        
    Returns:
        A decorated function
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Get combined parameters from args and query string
            params = {}
            if request.args:
                params.update(request.args.to_dict())
            params.update(kwargs)
            
            errors = []
            
            # Check required parameters
            if required_params:
                for param in required_params:
                    if param not in params or not params[param]:
                        errors.append(f"Missing required parameter: {param}")
            
            # Check numeric parameters
            if numeric_params:
                for param in numeric_params:
                    if param in params and params[param]:
                        try:
                            float(params[param])
                        except ValueError:
                            errors.append(f"Parameter must be numeric: {param}")
            
            # Check limit parameter
            if max_limit and 'limit' in params and params['limit']:
                try:
                    limit = int(params['limit'])
                    if limit > max_limit:
                        errors.append(f"Limit exceeds maximum of {max_limit}")
                except ValueError:
                    errors.append("Limit must be an integer")
            
            # If there are validation errors, raise InvalidParameterError
            if errors:
                raise InvalidParameterError(
                    message=f"Validation errors: {', '.join(errors)}",
                    user_message=f"Invalid parameters: {', '.join(errors)}",
                    context={'errors': errors}
                )
            
            return f(*args, **kwargs)
        
        return decorated_function
    
    return decorator

def register_routes(app: Flask) -> None:
    """
    Register API routes with the Flask application.
    
    Args:
        app: Flask application instance
    """
    @app.route('/health', methods=['GET'])
    @api_response
    def health() -> Dict[str, Any]:
        """
        Health check endpoint.
        
        Returns basic status information about the API service.
        """
        return {
            'status': 'ok',
            'service': 'GeoDash API',
            'initialized': app.config.get('INITIALIZED', False),
            'timestamp': time.time()
        }
    
    @app.route('/api/status', methods=['GET'])
    @api_response
    def status() -> Dict[str, Any]:
        """API status check with database information."""
        city_service = get_city_service()
        table_info = city_service.get_table_info()
        
        return {'table_info': table_info}
    
    @app.route('/api/cities/search', methods=['GET'])
    @validate_params(required_params=['query'], max_limit=100)
    @api_response
    def search() -> Dict[str, Any]:
        """
        Search for cities by name with optional location-aware prioritization.
        
        Query parameters:
            query: Search query string
            limit: Maximum number of results (default: 10)
            country: Optional country filter (restricts results to this country)
            
            user_lat: Optional user latitude for location-aware prioritization
            user_lng: Optional user longitude for location-aware prioritization
            user_country: Optional user country for location-aware prioritization
        """
        query = request.args.get('query', '')
        limit = min(int(request.args.get('limit', 10)), 100)
        
        # Country filter (restricts results to this country)
        country = request.args.get('country')
        
        # Location-aware parameters (for result prioritization)
        user_lat = request.args.get('user_lat')
        user_lng = request.args.get('user_lng')
        user_country = request.args.get('user_country')
        
        # Convert lat/lng to float if provided
        if user_lat is not None:
            try:
                user_lat = float(user_lat)
                if not -90 <= user_lat <= 90:
                    return format_response(
                        error='Invalid Parameter',
                        message=f"user_lat must be between -90 and 90, got {user_lat}",
                        status_code=400
                    )
            except ValueError:
                return format_response(
                    error='Invalid Parameter',
                    message=f"user_lat must be a valid number, got {user_lat}",
                    status_code=400
                )
        
        if user_lng is not None:
            try:
                user_lng = float(user_lng)
                if not -180 <= user_lng <= 180:
                    return format_response(
                        error='Invalid Parameter',
                        message=f"user_lng must be between -180 and 180, got {user_lng}",
                        status_code=400
                    )
            except ValueError:
                return format_response(
                    error='Invalid Parameter',
                    message=f"user_lng must be a valid number, got {user_lng}",
                    status_code=400
                )
        
        # Both lat and lng must be provided if either is provided
        if (user_lat is not None and user_lng is None) or (user_lat is None and user_lng is not None):
            return format_response(
                error='Invalid Parameter',
                message="Both user_lat and user_lng must be provided for location-aware search",
                status_code=400
            )
        
        city_service = get_city_service()
        results = city_service.search_cities(
            query=query, 
            limit=limit, 
            country=country,
            user_lat=user_lat,
            user_lng=user_lng,
            user_country=user_country
        )
        
        location_info = {}
        if user_lat is not None and user_lng is not None:
            location_info['user_coordinates'] = {
                'lat': user_lat,
                'lng': user_lng
            }
        if user_country is not None:
            location_info['user_country'] = user_country
        
        response = {
            'query': query,
            'limit': limit,
            'count': len(results),
            'results': results
        }
        
        # Add optional filter and location info
        if country:
            response['country_filter'] = country
        
        if location_info:
            response['location_info'] = location_info
        
        return response
    
    @app.route('/api/search', methods=['GET'])
    @validate_params(required_params=['q'], max_limit=100)
    @api_response
    def old_search() -> Dict[str, Any]:
        """
        Legacy search endpoint.
        
        Query parameters:
            q: Search query string
            limit: Maximum number of results (default: 10)
            country: Optional country filter
            user_lat: Optional user latitude for location-aware prioritization
            user_lng: Optional user longitude for location-aware prioritization
            user_country: Optional user country for location-aware prioritization
        """
        # Map the 'q' parameter to 'query' for the new endpoint
        request.args = request.args.copy()
        request.args['query'] = request.args.get('q', '')
        
        # Call the main search function
        return search()
    
    @app.route('/api/city/<int:city_id>', methods=['GET'])
    @api_response
    def get_city(city_id: int) -> Dict[str, Any]:
        """
        Get a city by its ID.
        
        Parameters:
            city_id: The ID of the city to fetch
        """
        city_service = get_city_service()
        city = city_service.get_city(city_id=city_id)
        
        if not city:
            return {
                'error': 'Not Found',
                'message': f"City with ID {city_id} not found"
            }, 404
        
        return {
            'city_id': city_id,
            'city': city
        }
    
    @app.route('/api/cities/<int:city_id>', methods=['GET'])
    @api_response
    def get_city_original(city_id: int) -> Dict[str, Any]:
        """
        Legacy endpoint for getting a city by its ID.
        
        Args:
            city_id: The ID of the city to retrieve
        """
        return get_city(city_id)
    
    @app.route('/api/cities/coordinates', methods=['GET'])
    @validate_params(required_params=['lat', 'lng'], 
                      numeric_params=['lat', 'lng', 'radius_km'],
                      max_limit=50)
    @api_response
    def by_coordinates() -> Dict[str, Any]:
        """
        Find cities near specified coordinates.
        
        Query parameters:
            lat: Latitude value (-90 to 90)
            lng: Longitude value (-180 to 180)
            radius_km: Search radius in kilometers (default: 10)
            limit: Maximum number of results (default: 10)
        """
        try:
            lat = float(request.args.get('lat'))
            lng = float(request.args.get('lng'))
            radius_km = float(request.args.get('radius_km', 10))
            limit = min(int(request.args.get('limit', 10)), 50)
            
            # Validate coordinate ranges
            if not -90 <= lat <= 90:
                return format_response(
                    error='Invalid Parameter',
                    message=f"lat must be between -90 and 90, got {lat}",
                    status_code=400
                )
                
            if not -180 <= lng <= 180:
                return format_response(
                    error='Invalid Parameter',
                    message=f"lng must be between -180 and 180, got {lng}",
                    status_code=400
                )
                
            if radius_km <= 0:
                return format_response(
                    error='Invalid Parameter',
                    message=f"radius_km must be greater than 0, got {radius_km}",
                    status_code=400
                )
            
            # Cap radius at a reasonable maximum (e.g., 500 km)
            max_radius = 500
            if radius_km > max_radius:
                radius_km = max_radius
            
            city_service = get_city_service()
            cities = city_service.get_cities_by_coordinates(
                lat=lat,
                lng=lng,
                radius_km=radius_km
            )
            
            # Limit the results if requested
            if limit < len(cities):
                cities = cities[:limit]
            
            return {
                'center': {
                    'lat': lat,
                    'lng': lng
                },
                'radius_km': radius_km,
                'limit': limit,
                'count': len(cities),
                'cities': cities
            }
            
        except ValueError as e:
            return format_response(
                error='Invalid Parameter',
                message=str(e),
                status_code=400
            )
    
    @app.route('/api/coordinates', methods=['GET'])
    @validate_params(required_params=['lat', 'lng'], 
                      numeric_params=['lat', 'lng', 'radius_km'],
                      max_limit=50)
    @api_response
    def coordinates_original() -> Dict[str, Any]:
        """Legacy endpoint for finding cities near coordinates."""
        return by_coordinates()
    
    @app.route('/api/countries', methods=['GET'])
    @api_response
    def countries() -> Dict[str, Any]:
        """
        Get a list of all countries.
        
        Returns a sorted list of all country names in the database.
        """
        city_service = get_city_service()
        countries_list = city_service.get_countries()
        
        return {
            'count': len(countries_list),
            'countries': countries_list
        }
    
    @app.route('/api/states', methods=['GET'])
    @validate_params(required_params=['country'])
    @api_response
    def states() -> Dict[str, Any]:
        """
        Get a list of states in a country.
        
        Query parameters:
            country: Country name
        """
        country = request.args.get('country', '')
        
        city_service = get_city_service()
        states_list = city_service.get_states(country=country)
        
        return {
            'country': country,
            'count': len(states_list),
            'states': states_list
        }
    
    @app.route('/api/countries/<country>/states', methods=['GET'])
    @api_response
    def states_original(country: str) -> Dict[str, Any]:
        """
        Get a list of states in a country (original URL format).
        
        Parameters:
            country: Country name
        """
        city_service = get_city_service()
        states_list = city_service.get_states(country=country)
        
        return {
            'country': country,
            'count': len(states_list),
            'states': states_list
        }
    
    @app.route('/api/cities/state', methods=['GET'])
    @validate_params(required_params=['state', 'country'], max_limit=100)
    @api_response
    def cities_in_state() -> Dict[str, Any]:
        """
        Get a list of cities in a state within a country.
        
        Query parameters:
            state: State name
            country: Country name
            limit: Maximum number of results (default: all cities)
        """
        state = request.args.get('state', '')
        country = request.args.get('country', '')
        limit_str = request.args.get('limit')
        
        city_service = get_city_service()
        cities = city_service.get_cities_in_state(
            state=state,
            country=country
        )
        
        # Apply limit if specified
        if limit_str:
            try:
                limit = min(int(limit_str), 100)
                if limit < len(cities):
                    cities = cities[:limit]
            except ValueError:
                return format_response(
                    error='Invalid Parameter',
                    message=f"limit must be a valid integer, got {limit_str}",
                    status_code=400
                )
        
        return {
            'state': state,
            'country': country,
            'count': len(cities),
            'cities': cities
        }
    
    @app.route('/api/countries/<country>/states/<state>/cities', methods=['GET'])
    @validate_params(max_limit=100)
    @api_response
    def cities_in_state_original(country: str, state: str) -> Dict[str, Any]:
        """
        Get a list of cities in a state (original URL format).
        
        Parameters:
            country: Country name
            state: State name
            
        Query parameters:
            limit: Maximum number of results (default: all cities)
        """
        limit_str = request.args.get('limit')
        
        city_service = get_city_service()
        cities = city_service.get_cities_in_state(state=state, country=country)
        
        # Apply limit if specified
        if limit_str:
            try:
                limit = min(int(limit_str), 100)
                if limit < len(cities):
                    cities = cities[:limit]
            except ValueError:
                return format_response(
                    error='Invalid Parameter',
                    message=f"limit must be a valid integer, got {limit_str}",
                    status_code=400
                )
        
        return {
            'country': country,
            'state': state,
            'count': len(cities),
            'cities': cities
        }

def start_server(host: str = '0.0.0.0', port: int = 5000, 
                db_uri: Optional[str] = None, debug: bool = False) -> None:
    """
    Start the API server.
    
    Args:
        host: Host address to bind to
        port: Port to listen on
        db_uri: Database URI for city data
        debug: Whether to run in debug mode
    """
    app = create_app(db_uri, debug)
    
    # Check if the database is ready
    if app.config.get('INITIALIZED', False):
        logger.info(f"Starting GeoDash API server on {host}:{port} (debug: {debug})")
        
        # Run the Flask application
        app.run(host=host, port=port, debug=debug)
    else:
        # Try to initialize the database if not ready
        logger.warning("Database not initialized. Attempting to initialize now...")
        
        try:
            city_service = CityService(db_uri)
            table_info = city_service.get_table_info()
            
            if table_info.get('row_count', 0) == 0:
                logger.info("Database is empty. Importing city data...")
                city_service.import_city_data()
                
            logger.info(f"Database initialized with {table_info.get('row_count', 0)} cities")
            
            # Run the Flask application
            logger.info(f"Starting GeoDash API server on {host}:{port} (debug: {debug})")
            app.run(host=host, port=port, debug=debug)
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            raise RuntimeError("Could not start server due to database initialization failure") from e

if __name__ == '__main__':
    import argparse
    from GeoDash.utils.logging import set_log_level
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Start the GeoDash API server')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='The host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='The port to bind to')
    parser.add_argument('--db-uri', type=str, help='The database URI')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--log-level', choices=['debug', 'info', 'warning', 'error', 'critical'], 
                        default='info', help='Set the logging level')
    
    args = parser.parse_args()
    
    # Set the log level if specified
    if args.log_level:
        set_log_level(args.log_level)
    
    logger.info(f"Starting server on {args.host}:{args.port}")
    
    start_server(
        host=args.host,
        port=args.port,
        db_uri=args.db_uri,
        debug=args.debug
    ) 