# GeoDash Deployment

This directory contains example deployment configurations for the GeoDash API server.

## Files

- `gunicorn_config.py`: Configuration file for running the GeoDash API with Gunicorn, a production-ready WSGI HTTP server.
- `wsgi.py`: WSGI entry point file for the GeoDash API with Gunicorn.

## Usage

### Gunicorn Deployment

To deploy the GeoDash API with Gunicorn:

```bash
# Navigate to the project root directory
cd /path/to/GeoDash-py

# Start the server with Gunicorn, using the configuration file
gunicorn -c GeoDash/deployment/gunicorn_config.py GeoDash.deployment.wsgi:app
```

### Configuration

The Gunicorn configuration file includes:

- Worker process settings
- Performance tuning settings
- Database initialization
- Logging configuration
- Memory management

You can customize the configuration file to match your deployment environment requirements.

### Environment Variables

Some useful environment variables that can be set:

- `GUNICORN_WORKER_ID`: Set automatically for worker identification
- `GEODASH_DB_INITIALIZED`: Set to '1' when the database is initialized

## Notes

These deployment examples are provided as starting points. For production deployments, consider:

1. Setting up a reverse proxy (like Nginx or Apache)
2. Configuring SSL/TLS
3. Setting appropriate user permissions
4. Monitoring and logging solutions
5. Use of environment variables for sensitive configuration 