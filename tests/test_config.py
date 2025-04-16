"""
Tests for the GeoDash configuration system.
"""

import os
import yaml
import tempfile
from pathlib import Path
import unittest
import shutil

from GeoDash.config import get_config, deep_merge


class TestConfig(unittest.TestCase):
    """Test cases for the configuration system."""
    
    def setUp(self):
        """Set up test environment."""
        # Reset the configuration to defaults before each test
        self.config = get_config()
        self.config._initialize()
        
        # Create temporary directories for test files
        self.temp_dir = tempfile.mkdtemp()
        self.home_temp = Path(self.temp_dir) / ".geodash"
        os.makedirs(self.home_temp, exist_ok=True)
        
    def tearDown(self):
        """Clean up after tests."""
        # Remove temporary directory
        shutil.rmtree(self.temp_dir)
    
    def test_find_config_file(self):
        """Test finding configuration files in standard locations."""
        # No files should be found initially
        self.assertIsNone(self.config.find_config_file())
        
        # Create a config file in the current directory
        cwd_config = Path.cwd() / "geodash.yml"
        try:
            with open(cwd_config, "w") as f:
                f.write("database:\n  type: sqlite\n")
            
            # Should find the file in current directory
            self.assertEqual(self.config.find_config_file(), cwd_config)
        finally:
            # Clean up
            if cwd_config.exists():
                os.remove(cwd_config)
    
    def test_load_config(self):
        """Test loading configuration from a file."""
        # Create a test config file
        config_path = Path(self.temp_dir) / "geodash.yml"
        test_config = {
            "database": {
                "type": "postgresql",
                "uri": "postgresql://user:pass@localhost/geodash",
                "pool_size": 20
            },
            "logging": {
                "level": "debug"
            }
        }
        
        with open(config_path, "w") as f:
            yaml.dump(test_config, f)
        
        # Patch the find_config_file method to return our test file
        original_find = self.config.find_config_file
        self.config.find_config_file = lambda: config_path
        
        try:
            # Load the configuration
            result = self.config.load_config()
            self.assertTrue(result)
            
            # Check if configuration was loaded correctly
            self.assertEqual(self.config.get("database.type"), "postgresql")
            self.assertEqual(self.config.get("database.uri"), "postgresql://user:pass@localhost/geodash")
            self.assertEqual(self.config.get("database.pool_size"), 20)
            self.assertEqual(self.config.get("logging.level"), "debug")
            
            # Check that default values are preserved for unspecified options
            self.assertEqual(self.config.get("logging.format"), "json")
        finally:
            # Restore the original method
            self.config.find_config_file = original_find
    
    def test_deep_merge(self):
        """Test deep merging of configuration dictionaries."""
        base = {
            "database": {
                "type": "sqlite",
                "pool_size": 5,
                "options": {
                    "timeout": 10,
                    "retry": True
                }
            },
            "logging": {
                "level": "info",
                "format": "json"
            },
            "list_item": [1, 2, 3]
        }
        
        override = {
            "database": {
                "type": "postgresql",
                "uri": "postgresql://localhost/geodash",
                "options": {
                    "timeout": 20
                }
            },
            "list_item": [4, 5, 6]
        }
        
        merged = deep_merge(base, override)
        
        # Check merged values
        self.assertEqual(merged["database"]["type"], "postgresql")
        self.assertEqual(merged["database"]["pool_size"], 5)  # Preserved from base
        self.assertEqual(merged["database"]["uri"], "postgresql://localhost/geodash")  # Added from override
        self.assertEqual(merged["database"]["options"]["timeout"], 20)  # Overridden
        self.assertEqual(merged["database"]["options"]["retry"], True)  # Preserved from base
        
        # Check that lists are replaced, not merged
        self.assertEqual(merged["list_item"], [4, 5, 6])
        
        # Check that unspecified values are preserved
        self.assertEqual(merged["logging"]["level"], "info")
        self.assertEqual(merged["logging"]["format"], "json")


if __name__ == "__main__":
    unittest.main() 