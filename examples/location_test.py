#!/usr/bin/env python3
"""
Test script to demonstrate the location-aware search feature.
This shows how search results are prioritized based on:
1. User's coordinates (proximity)
2. User's country
"""
import json

# Now import from GeoDash
from GeoDash import CityData

def print_json(data):
    """Print data as formatted JSON."""
    print(json.dumps(data, indent=2, ensure_ascii=False))

def test_location_based_search():
    """Test how location affects search results."""
    print("=== Location-Aware Search Demo ===")
    
    # Create a CityData instance
    city_data = CityData()
    
    # Test 1: Searching for "San" without location preference
    print("\n--- Test 1: Search for 'San' without location ---")
    results = city_data.search_cities(query="San", limit=5)
    print_json(results)
    
    # Test 2: Searching for "San" from USA
    print("\n--- Test 2: Search for 'San' from USA ---")
    results = city_data.search_cities(
        query="San", 
        limit=5,
        user_country="United States"
    )
    print_json(results)
    
    # Test 3: Searching for "San" from Spain
    print("\n--- Test 3: Search for 'San' from Spain ---")
    results = city_data.search_cities(
        query="San", 
        limit=5,
        user_country="Spain"
    )
    print_json(results)
    
    # Test 4: Searching for "San" from San Francisco coordinates
    print("\n--- Test 4: Search for 'San' from San Francisco coordinates ---")
    # San Francisco coordinates: 37.7749, -122.4194
    results = city_data.search_cities(
        query="San", 
        limit=5,
        user_lat=37.7749, 
        user_lng=-122.4194
    )
    print_json(results)
    
    # Test 5: Searching for "San" from Madrid coordinates
    print("\n--- Test 5: Search for 'San' from Madrid coordinates ---")
    # Madrid coordinates: 40.4168, -3.7038
    results = city_data.search_cities(
        query="San", 
        limit=5,
        user_lat=40.4168, 
        user_lng=-3.7038
    )
    print_json(results)
    
    # Test 6: Searching for "New" from New York coordinates and USA country
    print("\n--- Test 6: Search for 'New' from New York coordinates and USA country ---")
    # New York coordinates: 40.7128, -74.0060
    results = city_data.search_cities(
        query="New", 
        limit=5,
        user_lat=40.7128, 
        user_lng=-74.0060,
        user_country="United States"
    )
    print_json(results)
    
    # Test 7: Searching for "Lon" from United Kingdom
    print("\n--- Test 7: Search for 'Lon' from United Kingdom ---")
    results = city_data.search_cities(
        query="Lon", 
        limit=5,
        user_country="United Kingdom"
    )
    print_json(results)
    
    city_data.close()
    print("\n=== End of Demo ===")

if __name__ == "__main__":
    test_location_based_search() 