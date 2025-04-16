"""
Tests for the GeoDash feature flags system.
"""

import unittest
from GeoDash.config import get_config

class TestFeatureFlags(unittest.TestCase):
    """Test cases for the feature flag system."""
    
    def setUp(self):
        """Set up test environment."""
        # Reset the configuration to defaults before each test
        self.config = get_config()
        self.config._initialize()
        
    def test_is_feature_enabled(self):
        """Test checking if features are enabled."""
        # All features should be enabled by default in advanced mode
        self.assertTrue(self.config.is_feature_enabled("enable_fuzzy_search"))
        self.assertTrue(self.config.is_feature_enabled("enable_location_aware"))
        self.assertTrue(self.config.is_feature_enabled("enable_memory_caching"))
        self.assertTrue(self.config.is_feature_enabled("enable_shared_memory"))
        self.assertTrue(self.config.is_feature_enabled("enable_advanced_db"))
        self.assertTrue(self.config.is_feature_enabled("auto_fetch_data"))
        
        # Non-existent feature should be disabled
        self.assertFalse(self.config.is_feature_enabled("non_existent_feature"))
    
    def test_disable_feature(self):
        """Test disabling features."""
        # Disable a feature
        self.config.disable_feature("enable_fuzzy_search")
        
        # Check if it's properly disabled
        self.assertFalse(self.config.is_feature_enabled("enable_fuzzy_search"))
        
        # Other features should still be enabled
        self.assertTrue(self.config.is_feature_enabled("enable_location_aware"))
    
    def test_enable_feature(self):
        """Test enabling features."""
        # Disable a feature first
        self.config.disable_feature("enable_fuzzy_search")
        self.assertFalse(self.config.is_feature_enabled("enable_fuzzy_search"))
        
        # Re-enable it
        self.config.enable_feature("enable_fuzzy_search")
        
        # Check if it's properly enabled
        self.assertTrue(self.config.is_feature_enabled("enable_fuzzy_search"))
    
    def test_simple_mode(self):
        """Test the simple mode configuration."""
        # Set to simple mode
        self.config.set_mode("simple")
        
        # Check that specific features are disabled in simple mode
        self.assertFalse(self.config.is_feature_enabled("enable_fuzzy_search"))
        self.assertFalse(self.config.is_feature_enabled("enable_shared_memory"))
        self.assertFalse(self.config.is_feature_enabled("enable_advanced_db"))
        
        # Other features should still be enabled
        self.assertTrue(self.config.is_feature_enabled("enable_location_aware"))
        self.assertTrue(self.config.is_feature_enabled("enable_memory_caching"))
        self.assertTrue(self.config.is_feature_enabled("auto_fetch_data"))
    
    def test_advanced_mode(self):
        """Test switching back to advanced mode."""
        # Start in simple mode
        self.config.set_mode("simple")
        
        # Confirm features are disabled
        self.assertFalse(self.config.is_feature_enabled("enable_fuzzy_search"))
        
        # Switch back to advanced mode
        self.config.set_mode("advanced")
        
        # Features should be enabled again
        self.assertTrue(self.config.is_feature_enabled("enable_fuzzy_search"))
        self.assertTrue(self.config.is_feature_enabled("enable_shared_memory"))
        self.assertTrue(self.config.is_feature_enabled("enable_advanced_db"))
    
    def test_feature_cache(self):
        """Test that feature checks are cached for performance."""
        # Call is_feature_enabled to populate the cache
        self.config.is_feature_enabled("enable_fuzzy_search")
        
        # Check that the cache contains the feature
        self.assertIn("enable_fuzzy_search", self.config._feature_cache)
        
        # Modify the config directly (bypassing the set method)
        self.config._config["features"]["enable_fuzzy_search"] = False
        
        # The cached value should still be used
        self.assertTrue(self.config.is_feature_enabled("enable_fuzzy_search"))
        
        # Clear the cache
        self.config._clear_feature_cache()
        
        # Now the new value should be used
        self.assertFalse(self.config.is_feature_enabled("enable_fuzzy_search"))


if __name__ == "__main__":
    unittest.main() 