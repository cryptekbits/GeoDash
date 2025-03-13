#!/usr/bin/env python3
"""
Demo of the city autocomplete functionality in the citizen module.
This demonstrates how the search functionality can work with just a few characters,
similar to how a UI autocomplete would function.
"""
import json
import time
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

def simulate_user_typing(city_data, query_sequence, country=None):
    """
    Simulate a user typing a search query character by character.
    
    Args:
        city_data: CityData instance
        query_sequence: The sequence to type (e.g. "New York")
        country: Optional country filter
    """
    current_query = ""
    
    for char in query_sequence:
        # Add the next character to the query
        current_query += char
        print(f"\nUser types: '{current_query}'")
        
        # Skip if less than 3 characters (typical UI behavior)
        if len(current_query) < 3:
            print("Waiting for more characters...")
            continue
        
        # Search with the current query
        start_time = time.time()
        results = city_data.search_cities(current_query, limit=5, country=country)
        search_time = time.time() - start_time
        
        # Show results
        print(f"Found {len(results)} matches in {search_time:.4f} seconds:")
        if results:
            print_json(results)
        else:
            print("No matches found.")

def main():
    """Main function demonstrating autocomplete functionality."""
    print("=== City Autocomplete Demo ===")
    print("This demo shows how the city search supports autocomplete functionality.")
    print("The search works with just a few characters, just like a UI autocomplete would.")
    
    # Create a CityData instance
    city_data = CityData()
    
    # Example 1: Finding a city with common prefix
    print("\n--- Example 1: Finding New York ---")
    print("Scenario: User starts typing 'New York'")
    simulate_user_typing(city_data, "New York", country="United States")
    
    # Example 2: Finding a city in India (tests the memory cache)
    print("\n--- Example 2: Finding Mumbai (India) ---")
    print("Scenario: User starts typing 'Mum' to find Mumbai")
    simulate_user_typing(city_data, "Mumbai", country="India")
    
    # Example 3: Searching without a country filter
    print("\n--- Example 3: Finding San Francisco (no country filter) ---")
    print("Scenario: User starts typing 'San Fra'")
    simulate_user_typing(city_data, "San Fra")
    
    # Example 4: Handle short queries
    print("\n--- Example 4: Very common prefix ---")
    print("Scenario: User types 'Sa' (too short) then 'San'")
    simulate_user_typing(city_data, "San")
    
    # Close the connection
    city_data.close()
    
    print("\n=== End of Demo ===")
    print("This is how the citizen module would support a UI autocomplete component.")
    print("The UI would typically start searching after 3 characters,")
    print("and update the suggestions as the user continues typing.")

if __name__ == '__main__':
    main() 