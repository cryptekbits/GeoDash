"""
API module for the CitiZen package.

This module provides a REST API for accessing city data through HTTP endpoints.
The API allows for searching cities by name, retrieving by coordinates, and
accessing hierarchical data (countries, states, cities).

Key Components:
- start_server: Function to start the API server
- Endpoints: Various endpoints for accessing city data
"""

from citizen.api.server import start_server

__all__ = ['start_server'] 