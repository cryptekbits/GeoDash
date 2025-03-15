#!/usr/bin/env python3
"""
Example of using the GeoDash module with a PostgreSQL database.
"""
import json
import os
import sys

# Add the project root directory to Python path
# This allows importing the GeoDash module regardless of where script is run from
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Now import from GeoDash
from GeoDash import CityData

def print_json(data):
    """Print data as formatted JSON."""
    print(json.dumps(data, indent=2, ensure_ascii=False))

def main():
    """Main function."""
    # Replace with your PostgreSQL connection details
    db_uri = 'postgresql://username:password@localhost:5432/dbname'
    
    print(f"Connecting to PostgreSQL database: {db_uri}")
    
    try:
        # Create a CityData instance with PostgreSQL
        city_data = CityData(db_uri=db_uri)
        
        # Search for cities
        print("Searching for 'Mumbai'...")
        cities = city_data.search_cities('Mumbai', limit=5)
        print_json(cities)
        
        # Test India cache
        print("\nTesting India cache...")
        cities = city_data.search_cities('Delhi', country='India')
        print_json(cities)
        
        # Close the connection
        city_data.close()
        
    except Exception as e:
        print(f"Error: {e}")
        print("\nNote: This example requires a PostgreSQL database.")
        print("If you don't have one, you can use the basic_usage.py example instead.")

if __name__ == '__main__':
    main() 