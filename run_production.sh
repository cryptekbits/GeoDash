#!/bin/bash
# Script to start GeoDash API in production mode with Gunicorn

# Set environment variables (customize as needed)
export PYTHONPATH="$PYTHONPATH:$(pwd)"

# Start Gunicorn with the configuration file
echo "Starting GeoDash API with Gunicorn..."
gunicorn -c gunicorn_config.py wsgi:app 