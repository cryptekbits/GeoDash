#!/usr/bin/env python3
"""
Example of using the citizen module directly from the main application.
"""
import json
import os
import sys

# Add the project root directory to Python path
# This allows importing the citizen module regardless of where script is run from
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Now import from citizen
from citizen import CityData

def print_json(data):
    """Print data as formatted JSON."""
    print(json.dumps(data, indent=2, ensure_ascii=False))

def main():
    """Main function."""
    print("=== Using the citizen module in the main application ===")
    
    # Create a CityData instance (will use SQLite in citizen/data/cities.sqlite)
    city_data = CityData()
    
    # Example 1: Get cities by coordinates
    print("\nExample 1: Get cities by coordinates")
    print("Finding cities near Mumbai coordinates...")
    cities = city_data.get_cities_by_coordinates(19.0760, 72.8777, radius_km=5)
    print_json(cities[:2])  # Print first 2 results
    
    # Example 2: Search for cities with autocomplete
    print("\nExample 2: Search for cities with autocomplete")
    print("Searching for 'Delhi' in India...")
    cities = city_data.search_cities('Delhi', country='India', limit=3)
    print_json(cities)
    
    # Example 3: Get states in a country for a dropdown
    print("\nExample 3: Get states in a country for a dropdown")
    print("Getting states in India...")
    states = city_data.get_states('India')
    print(f"Found {len(states)} states")
    print(states[:5])  # Print first 5 states
    
    # Example 4: Use case for form input with location data
    print("\nExample 4: Use case for form input with location data")
    print("User selects 'Mumbai' from autocomplete...")
    
    # Store the city ID in your form/database
    city_id = None
    cities = city_data.search_cities('Mumbai', country='India', limit=1)
    if cities:
        city_id = cities[0]['id']
        print(f"Stored city_id: {city_id} in the form/database")
    
    # Later, retrieve the full city data when needed
    if city_id:
        print("\nLater, retrieving the full city data...")
        city = city_data.get_city_by_id(city_id)
        print_json(city)
        
        # Use the coordinates for calculations
        lat = city['latitude']
        lng = city['longitude']
        print(f"Using coordinates ({lat}, {lng}) for astrological calculations...")
    
    # Don't forget to close the connection when done
    city_data.close()
    
    print("\n=== End of examples ===")

if __name__ == '__main__':
    main() 