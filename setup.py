from setuptools import setup, find_packages
import os
import urllib.request
import shutil
from setuptools.command.install import install
from setuptools.command.develop import develop

def download_city_data():
    """Download the cities.csv file from a remote source."""
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'GeoDash', 'data')
    os.makedirs(data_dir, exist_ok=True)
    
    csv_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/csv/cities.csv"
    csv_path = os.path.join(data_dir, 'cities.csv')
    
    print(f"Downloading cities.csv to {csv_path}...")
    try:
        urllib.request.urlretrieve(csv_url, csv_path)
        print("Download complete!")
    except Exception as e:
        print(f"Warning: Failed to download cities.csv: {e}")
        print("You can manually download it later from https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/csv/cities.csv")
        print(f"Place it in the directory: {data_dir}")

class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        # First run the standard install
        install.run(self)
        
        # Then download city data
        download_city_data()
        
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

class PostDevelopCommand(develop):
    """Post-installation for development mode."""
    def run(self):
        develop.run(self)
        download_city_data()

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="GeoDash",
    version="1.0.0",
    author="GeoDash Team",
    author_email="your.email@example.com",
    description="A Python module for managing city data with fast coordinate queries and autocomplete functionality",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/GeoDash",
    project_urls={
        "Bug Tracker": "https://github.com/yourusername/GeoDash/issues",
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