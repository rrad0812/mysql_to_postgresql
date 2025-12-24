"""MySQL to PostgreSQL migration package.

This package provides tools and utilities for migrating data from MySQL to PostgreSQL.
"""

from base import MigrationManager, DataFetcher, DataWriter
from mysql_to_postgresql_pkg.config import MYSQL_CONFIG, POSTGRES_CONFIG
from mysql_to_postgresql_pkg.mysql_fetcher import MySQLFetcher
from mysql_to_postgresql_pkg.postgres_writer import PostgresWriter
from mysql_to_postgresql_pkg.mysql_to_postgresql_manager import MySQLtoPostgreSQLMigrationManager
from mysql_to_postgresql_pkg.mysql_postgres_mapping import (
    map_mysql_to_postgres_type,
    get_mysql_type_category,
    transform_data_types
)

__all__ = [
    "MigrationManager",
    "DataFetcher", 
    "DataWriter",
    "MYSQL_CONFIG",
    "POSTGRES_CONFIG",
    "MySQLFetcher",
    "PostgresWriter",
    "MySQLtoPostgreSQLMigrationManager",
    "map_mysql_to_postgres_type",
    "get_mysql_type_category",
    "transform_data_types",
]

__version__ = "0.1.0"
