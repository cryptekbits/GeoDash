from setuptools import setup, find_packages
import os
import urllib.request
import shutil
from setuptools.command.install import install
from setuptools.command.develop import develop

# Import version from GeoDash/__init__.py
import re
with open(os.path.join('GeoDash', '__init__.py'), 'r') as f:
    version = re.search(r"__version__\s*=\s*'(.*)'", f.read()).group(1)

def download_city_data(max_retries=3):
    """Download the cities.csv file from a remote source.
    
    Args:
        max_retries: Maximum number of download attempts before giving up.
    
    Returns:
        bool: True if download succeeded, False otherwise.
    """
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'GeoDash', 'data')
    os.makedirs(data_dir, exist_ok=True)
    
    csv_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/csv/cities.csv"
    csv_path = os.path.join(data_dir, 'cities.csv')
    
    print(f"Downloading cities.csv to {csv_path}...")
    
    for attempt in range(1, max_retries + 1):
        try:
            urllib.request.urlretrieve(csv_url, csv_path)
            print("Download complete!")
            return True
        except urllib.error.URLError as e:
            print(f"Attempt {attempt}/{max_retries} failed: Network error: {e.reason}")
            if attempt < max_retries:
                print(f"Retrying in 3 seconds...")
                import time
                time.sleep(3)
            else:
                _print_manual_download_instructions(data_dir, csv_url)
                return False
        except Exception as e:
            print(f"Attempt {attempt}/{max_retries} failed: Unexpected error: {e}")
            if attempt < max_retries:
                print(f"Retrying in 3 seconds...")
                import time
                time.sleep(3)
            else:
                _print_manual_download_instructions(data_dir, csv_url)
                return False

def _print_manual_download_instructions(data_dir, csv_url):
    """Print detailed instructions for manual download."""
    print("\n" + "="*80)
    print("ERROR: Failed to download city data after multiple attempts.")
    print("="*80)
    print("\nTo use GeoDash, you need to manually download the cities.csv file:")
    print("\n1. Download the file from:")
    print(f"   {csv_url}")
    print("\n2. Save the file to this location:")
    print(f"   {data_dir}/cities.csv")
    print("\n3. Verify the file exists in the correct location before using GeoDash.")
    print("\nAlternative download methods:")
    print("- Using wget:   wget -O {}/cities.csv {}".format(data_dir, csv_url))
    print("- Using curl:   curl -o {}/cities.csv {}".format(data_dir, csv_url))
    print("="*80)

class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        # First run the standard install
        install.run(self)
        
        # Then download city data
        download_success = download_city_data()
        
        # Ensure data is in the installed package
        src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'GeoDash', 'data')
        if self.install_lib:
            dest_dir = os.path.join(self.install_lib, 'GeoDash', 'data')
            os.makedirs(dest_dir, exist_ok=True)
            
            # Copy cities.csv if it exists
            csv_src = os.path.join(src_dir, 'cities.csv')
            csv_dest = os.path.join(dest_dir, 'cities.csv')
            if os.path.exists(csv_src):
                print(f"Copying cities.csv to installed package: {csv_dest}")
                shutil.copy2(csv_src, csv_dest)
            else:
                if not download_success:
                    print("\nNote: GeoDash will have limited functionality without city data.")
                    print("You can run the package, but city-related features will not work.")
                    print("See instructions above for manually installing city data.\n")

class PostDevelopCommand(develop):
    """Post-installation for development mode."""
    def run(self):
        develop.run(self)
        download_success = download_city_data()
        
        if not download_success:
            print("\nNote: GeoDash will have limited functionality without city data.")
            print("You can run the package, but city-related features will not work.")
            print("Please follow the manual download instructions above before using city-related features.\n")

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="GeoDash",
    version=version,
    author="Manan Ramnani",
    author_email="email@cryptek.dev",
    description="A Python module for managing city data with fast coordinate queries and autocomplete functionality",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cryptekbits/GeoDash-py",
    project_urls={
        "Bug Tracker": "https://github.com/cryptekbits/GeoDash-py/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "GeoDash": ["data/*.csv", "data/*.db", "static/*"],
    },
    python_requires=">=3.6",
    install_requires=[
        "pandas>=1.3.0",
        "flask>=2.0.0",
        "psycopg2-binary>=2.9.0",
        "click>=8.0.0",
    ],
    entry_points={
        "console_scripts": [
            "GeoDash=GeoDash.__main__:main",
        ],
    },
    cmdclass={
        'install': PostInstallCommand,
        'develop': PostDevelopCommand,
    },
) 