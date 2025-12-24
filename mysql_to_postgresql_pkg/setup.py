"""Setup configuration for mysql_to_postgresql package."""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="mysql_to_postgresql",
    version="0.1.0",
    author="Your Name",
    description="A package for migrating data from MySQL to PostgreSQL",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        "pymysql>=1.0.0",
        "psycopg2-binary>=2.9.0",
        "pandas>=1.3.0",
    ],
    entry_points={
        "console_scripts": [
            "mysql-to-postgresql=runner:main",
        ],
    },
)
