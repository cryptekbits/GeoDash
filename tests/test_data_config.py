"""
Tests for the GeoDash data configuration.
"""

import unittest
from GeoDash.config import get_config
from GeoDash.config.data import (
    is_country_enabled,
    filter_cities_by_countries,
    get_download_url,
    get_batch_size
)

class TestDataConfig(unittest.TestCase):
    """Test cases for the data configuration system."""
    
    def setUp(self):
        """Set up test environment."""
        # Reset the configuration to defaults before each test
        self.config = get_config()
        self.config._initialize()
        
    def test_is_country_enabled(self):
        """Test checking if countries are enabled."""
        # By default, all countries should be enabled
        self.assertTrue(is_country_enabled("US"))
        self.assertTrue(is_country_enabled("GB"))
        self.assertTrue(is_country_enabled("JP"))
        
        # Empty country code should return False
        self.assertFalse(is_country_enabled(""))
        self.assertFalse(is_country_enabled(None))
        
        # Set a specific list of countries
        self.config.set("data.countries", "US,CA,MX")
        
        # These countries should be enabled
        self.assertTrue(is_country_enabled("US"))
        self.assertTrue(is_country_enabled("CA"))
        self.assertTrue(is_country_enabled("MX"))
        
        # Case-insensitivity test
        self.assertTrue(is_country_enabled("us"))
        self.assertTrue(is_country_enabled("Ca"))
        
        # Other countries should be disabled
        self.assertFalse(is_country_enabled("GB"))
        self.assertFalse(is_country_enabled("JP"))
    
    def test_filter_cities_by_countries(self):
        """Test filtering city lists by country."""
        # Sample city data
        cities = [
            {"name": "New York", "country_code": "US"},
            {"name": "London", "country_code": "GB"},
            {"name": "Paris", "country_code": "FR"},
            {"name": "Tokyo", "country_code": "JP"},
            {"name": "Toronto", "country_code": "CA"}
        ]
        
        # No filtering when all countries are enabled
        self.assertEqual(len(filter_cities_by_countries(cities, None)), 5)
        
        # Filter for specific countries
        filtered = filter_cities_by_countries(cities, ["US", "GB"])
        self.assertEqual(len(filtered), 2)
        self.assertEqual(filtered[0]["name"], "New York")
        self.assertEqual(filtered[1]["name"], "London")
        
        # Case-insensitivity test
        filtered = filter_cities_by_countries(cities, ["us", "gb"])
        self.assertEqual(len(filtered), 2)
        
        # Empty list should return empty result
        self.assertEqual(len(filter_cities_by_countries(cities, [])), 0)
    
    def test_get_enabled_countries(self):
        """Test getting the list of enabled countries."""
        # Default should be None (meaning all countries are enabled)
        self.assertIsNone(self.config.get_enabled_countries())
        
        # Set specific countries
        self.config.set("data.countries", "US,CA,MX")
        enabled = self.config.get_enabled_countries()
        
        # Should be a list of uppercase country codes
        self.assertEqual(len(enabled), 3)
        self.assertEqual(enabled, ["US", "CA", "MX"])
        
        # Test with whitespace in the list
        self.config.set("data.countries", "US, CA, MX")
        enabled = self.config.get_enabled_countries()
        self.assertEqual(enabled, ["US", "CA", "MX"])
        
        # Test with lowercase codes
        self.config.set("data.countries", "us,ca,mx")
        enabled = self.config.get_enabled_countries()
        self.assertEqual(enabled, ["US", "CA", "MX"])
    
    def test_get_download_url(self):
        """Test getting the configured download URL."""
        # Default URL should be set
        default_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/csv/cities.csv"
        self.assertEqual(get_download_url(), default_url)
        
        # Set a custom URL
        custom_url = "https://example.com/cities.csv"
        self.config.set("data.download_url", custom_url)
        self.assertEqual(get_download_url(), custom_url)
    
    def test_get_batch_size(self):
        """Test getting the configured batch size."""
        # Default batch size should be 5000
        self.assertEqual(get_batch_size(), 5000)
        
        # Set a custom batch size
        self.config.set("data.batch_size", 1000)
        self.assertEqual(get_batch_size(), 1000)
    
    def test_should_auto_download(self):
        """Test checking if auto-download is enabled."""
        # Should be enabled by default
        self.assertTrue(self.config.should_auto_download())
        
        # Disable the feature
        self.config.disable_feature("auto_fetch_data")
        self.assertFalse(self.config.should_auto_download())


if __name__ == "__main__":
    unittest.main() 