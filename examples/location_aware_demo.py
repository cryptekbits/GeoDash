#!/usr/bin/env python3
"""
GeoDash Location-Aware Search Demo

This script demonstrates how search results are prioritized based on user location.
"""

import json
import time
import logging

from GeoDash.data.city_manager import CityData

# Set up logging - only show warnings and errors
logging.basicConfig(level=logging.WARNING, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def print_json(data):
    """Print data in a formatted JSON style."""
    print(json.dumps(data, indent=2, ensure_ascii=False))

def main():
    """Run the location-aware search demo."""
    print("GeoDash Location-Aware Search Demo")
    print("==================================")
    print("This demo showcases how search results are prioritized based on user location.")
    print()

    # Initialize CityData
    print("Initializing CityData...")
    city_data = CityData()
    print("Database connected successfully.")
    print()
    
    # Example 1: Search for "San" without location prioritization
    print("\n1. Searching for 'San' without location prioritization:")
    start_time = time.time()
    results = city_data.search_cities("San", limit=5)
    elapsed = time.time() - start_time
    print(f"Found {len(results)} results in {elapsed:.4f} seconds:")
    print_json(results)
    
    # Example 2: Search for "San" using San Francisco coordinates
    print("\n2. Searching for 'San' using San Francisco coordinates:")
    start_time = time.time()
    results = city_data.search_cities(
        "San", 
        limit=5,
        user_lat=37.7749,  # San Francisco latitude
        user_lng=-122.4194  # San Francisco longitude
    )
    elapsed = time.time() - start_time
    print(f"Found {len(results)} results in {elapsed:.4f} seconds:")
    print_json(results)
    
    # Example 3: Search for "San" from Madrid, Spain
    print("\n3. Searching for 'San' from Madrid, Spain:")
    start_time = time.time()
    results = city_data.search_cities(
        "San", 
        limit=5,
        user_lat=40.4168,  # Madrid latitude
        user_lng=-3.7038  # Madrid longitude
    )
    elapsed = time.time() - start_time
    print(f"Found {len(results)} results in {elapsed:.4f} seconds:")
    print_json(results)
    
    # Example 4: Search for "New" from the United States
    print("\n4. Searching for 'New' from the United States:")
    start_time = time.time()
    results = city_data.search_cities(
        "New", 
        limit=5,
        user_country="United States"
    )
    elapsed = time.time() - start_time
    print(f"Found {len(results)} results in {elapsed:.4f} seconds:")
    print_json(results)
    
    # Example 5: Search for "New" from India
    print("\n5. Searching for 'New' from India:")
    start_time = time.time()
    results = city_data.search_cities(
        "New", 
        limit=5,
        user_country="India"
    )
    elapsed = time.time() - start_time
    print(f"Found {len(results)} results in {elapsed:.4f} seconds:")
    print_json(results)
    
    # Example 6: Combined approach - search for "New" using New York coordinates
    # and specifying the user country as the United States
    print("\n6. Searching for 'New' using New York coordinates and specifying the user country:")
    start_time = time.time()
    results = city_data.search_cities(
        "New", 
        limit=5,
        user_lat=40.7128,  # New York latitude
        user_lng=-74.0060,  # New York longitude
        user_country="United States"
    )
    elapsed = time.time() - start_time
    print(f"Found {len(results)} results in {elapsed:.4f} seconds:")
    print_json(results)
    
    # Close the connection
    city_data.close()
    
    print("\nSummary:")
    print("GeoDash prioritizes search results based on user location, while text")
    print("matching quality remains the primary ranking factor.")
    print()
    print("Notice how the results change based on the user's location:")
    print("- When searching from San Francisco, cities in the Americas are prioritized")
    print("- When searching from Madrid, European cities are prioritized")
    print("- When specifying a country, cities in that country are prioritized")
    print("- When combining coordinates and country, both factors influence the results")

if __name__ == "__main__":
    main() 