from setuptools import setup, find_packages
import os
import urllib.request
from setuptools.command.install import install
from setuptools.command.develop import develop

def download_city_data():
    """Download the cities.csv file from a remote source."""
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'citizen', 'data')
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

class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        install.run(self)
        download_city_data()

class PostDevelopCommand(develop):
    """Post-installation for development mode."""
    def run(self):
        develop.run(self)
        download_city_data()

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="CitiZen",
    version="1.0.0",
    author="CitiZen Team",
    author_email="your.email@example.com",
    description="A Python module for managing city data with fast coordinate queries and autocomplete functionality",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/CitiZen",
    project_urls={
        "Bug Tracker": "https://github.com/yourusername/CitiZen/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "citizen": ["data/*.csv", "static/*"],
    },
    python_requires=">=3.6",
    install_requires=[
        "pandas>=1.3.0",
        "flask>=2.0.0",
        "psycopg2-binary>=2.9.0",
    ],
    entry_points={
        "console_scripts": [
            "citizen=citizen.__main__:main",
        ],
    },
    cmdclass={
        'install': PostInstallCommand,
        'develop': PostDevelopCommand,
    },
) 