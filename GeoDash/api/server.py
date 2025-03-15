"""
API server module for the GeoDash package.

This module provides a Flask-based API server for accessing city data
through RESTful API endpoints.
"""

import logging
import time
import json
from typing import Dict, Any, List, Union, Optional, Tuple
from functools import wraps

from flask import Flask, request, jsonify, Response, g
import werkzeug.exceptions

from GeoDash.data import CityData
from GeoDash.data.database import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_app(db_uri: Optional[str] = None) -> Flask:
    """
    Create and configure the Flask application.
    
    Args:
        db_uri: Database URI for city data
        
    Returns:
        Configured Flask application
    """
    app = Flask(__name__)
    
    # Store database URI in app config
    app.config['DB_URI'] = db_uri
    
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
        return {
            'error': 'Bad Request',
            'message': str(error.description)
        }, 400
    
    @app.errorhandler(werkzeug.exceptions.NotFound)
    def handle_not_found(error: werkzeug.exceptions.NotFound) -> Tuple[Dict[str, Any], int]:
        """Handle not found errors."""
        return {
            'error': 'Not Found',
            'message': str(error.description)
        }, 404
    
    @app.errorhandler(Exception)
    def handle_exception(error: Exception) -> Tuple[Dict[str, Any], int]:
        """Handle generic exceptions."""
        logger.exception("Unhandled exception occurred")
        return {
            'error': 'Internal Server Error',
            'message': 'An unexpected error occurred'
        }, 500
    
    # Register API routes
    register_routes(app)
    
    return app

def get_city_data() -> CityData:
    """
    Get a CityData instance for the current application.
    
    Returns:
        CityData instance initialized with the application's DB URI
    """
    return CityData(db_uri=Flask.current_app.config.get('DB_URI'))

def validate_params(required_params: Optional[List[str]] = None,
                    numeric_params: Optional[List[str]] = None,
                    max_limit: Optional[int] = None):
    """
    Decorator to validate request parameters.
    
    Args:
        required_params: List of parameter names that must be present
        numeric_params: List of parameter names that must be numeric
        max_limit: Maximum value for the 'limit' parameter
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Get combined parameters from args and query string
            params = request.args.to_dict()
            
            # Check for required parameters
            if required_params:
                for param in required_params:
                    if param not in params:
                        return jsonify({
                            'error': 'Missing Required Parameter',
                            'message': f"Parameter '{param}' is required"
                        }), 400
            
            # Check for numeric parameters
            if numeric_params:
                for param in numeric_params:
                    if param in params:
                        try:
                            float(params[param])
                        except ValueError:
                            return jsonify({
                                'error': 'Invalid Parameter',
                                'message': f"Parameter '{param}' must be numeric"
                            }), 400
            
            # Check limit parameter
            if max_limit and 'limit' in params:
                try:
                    limit = int(params['limit'])
                    if limit > max_limit:
                        return jsonify({
                            'error': 'Parameter Limit Exceeded',
                            'message': f"Parameter 'limit' cannot exceed {max_limit}"
                        }), 400
                except ValueError:
                    pass  # Already checked in numeric_params
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def register_routes(app: Flask) -> None:
    """
    Register API routes with the Flask application.
    
    Args:
        app: Flask application instance
    """
    @app.route('/api/status', methods=['GET'])
    def status() -> Dict[str, Any]:
        """Return API status information."""
        with CityData(app.config.get('DB_URI')) as city_data:
            table_info = city_data.get_table_info()
            
            return {
                'status': 'ok',
                'database': {
                    'type': city_data.db_manager.db_type,
                    'table_exists': True,
                    'row_count': table_info.get('row_count', 0)
                }
            }
    
    @app.route('/api/search', methods=['GET'])
    @validate_params(required_params=['q'], max_limit=100)
    def search() -> Dict[str, Any]:
        """
        Search for cities by name.
        
        Query parameters:
            q: Search query string
            limit: Maximum number of results (default: 10)
            country: Optional country filter
        """
        query = request.args.get('q', '')
        limit = min(int(request.args.get('limit', 10)), 100)
        country = request.args.get('country')
        
        with CityData(app.config.get('DB_URI')) as city_data:
            results = city_data.search_cities(query, limit, country)
            
            return {
                'query': query,
                'limit': limit,
                'country': country,
                'count': len(results),
                'results': results
            }
    
    @app.route('/api/cities/<int:city_id>', methods=['GET'])
    def get_city(city_id: int) -> Dict[str, Any]:
        """
        Get a city by its ID.
        
        Args:
            city_id: The ID of the city to retrieve
        """
        with CityData(app.config.get('DB_URI')) as city_data:
            city = city_data.get_city(city_id)
            
            if not city:
                return jsonify({
                    'error': 'Not Found',
                    'message': f"City with ID {city_id} not found"
                }), 404
            
            return {
                'city': city
            }
    
    @app.route('/api/coordinates', methods=['GET'])
    @validate_params(required_params=['lat', 'lng'], 
                      numeric_params=['lat', 'lng', 'radius_km'],
                      max_limit=50)
    def by_coordinates() -> Dict[str, Any]:
        """
        Find cities near specified coordinates.
        
        Query parameters:
            lat: Latitude
            lng: Longitude
            radius_km: Search radius in kilometers (default: 10)
            limit: Maximum number of results (default: 10)
        """
        try:
            lat = float(request.args.get('lat'))
            lng = float(request.args.get('lng'))
            radius_km = float(request.args.get('radius_km', 10))
            limit = min(int(request.args.get('limit', 10)), 50)
            
            # Validate coordinates
            if not -90 <= lat <= 90:
                return jsonify({
                    'error': 'Invalid Parameter',
                    'message': f"Latitude must be between -90 and 90, got {lat}"
                }), 400
                
            if not -180 <= lng <= 180:
                return jsonify({
                    'error': 'Invalid Parameter',
                    'message': f"Longitude must be between -180 and 180, got {lng}"
                }), 400
                
            if radius_km <= 0:
                return jsonify({
                    'error': 'Invalid Parameter',
                    'message': f"Radius must be positive, got {radius_km}"
                }), 400
            
            with CityData(app.config.get('DB_URI')) as city_data:
                cities = city_data.get_cities_by_coordinates(lat, lng, radius_km)
                
                # Apply limit
                cities = cities[:limit]
                
                return {
                    'coordinates': {
                        'lat': lat,
                        'lng': lng
                    },
                    'radius_km': radius_km,
                    'count': len(cities),
                    'results': cities
                }
                
        except ValueError as e:
            return jsonify({
                'error': 'Invalid Parameter',
                'message': str(e)
            }), 400
    
    @app.route('/api/countries', methods=['GET'])
    def countries() -> Dict[str, Any]:
        """Get a list of all countries."""
        with CityData(app.config.get('DB_URI')) as city_data:
            countries = city_data.get_countries()
            
            return {
                'count': len(countries),
                'countries': countries
            }
    
    @app.route('/api/countries/<country>/states', methods=['GET'])
    def states(country: str) -> Dict[str, Any]:
        """
        Get states in a country.
        
        Args:
            country: Country name
        """
        with CityData(app.config.get('DB_URI')) as city_data:
            states = city_data.get_states(country)
            
            return {
                'country': country,
                'count': len(states),
                'states': states
            }
    
    @app.route('/api/countries/<country>/states/<state>/cities', methods=['GET'])
    @validate_params(max_limit=100)
    def cities_in_state(country: str, state: str) -> Dict[str, Any]:
        """
        Get cities in a state.
        
        Args:
            country: Country name
            state: State name
            
        Query parameters:
            limit: Maximum number of results (default: all)
        """
        limit_str = request.args.get('limit')
        
        with CityData(app.config.get('DB_URI')) as city_data:
            cities = city_data.get_cities_in_state(state, country)
            
            # Apply limit if specified
            if limit_str:
                limit = min(int(limit_str), 100)
                cities = cities[:limit]
            
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
        host: Host to bind to
        port: Port to bind to
        db_uri: Database URI for city data
        debug: Enable Flask debug mode
    """
    app = create_app(db_uri)
    app.run(host=host, port=port, debug=debug)

if __name__ == '__main__':
    start_server(debug=True) 