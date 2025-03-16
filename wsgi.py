#!/usr/bin/env python3
"""
WSGI entry point for GeoDash API with Gunicorn.
"""
from GeoDash.api.server import create_app

# Create the Flask application
app = create_app()

if __name__ == "__main__":
    app.run() 