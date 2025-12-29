"""MySQL to PostgreSQL migration package.

This package provides tools and utilities for migrating data from MySQL to PostgreSQL.
"""

import sys
from pathlib import Path
# Add parent directory to path so we can import base
sys.path.insert(0, str(Path(__file__).parent.parent))

from base import MigrationManager, DataFetcher, DataWriter
from mysql_to_postgresql_pkg.config import MYSQL_CONFIG, POSTGRES_CONFIG
from mysql_to_postgresql_pkg.mysql_fetcher import MySQLFetcher
from mysql_to_postgresql_pkg.postgres_writer import PostgresWriter
from mysql_to_postgresql_pkg.mysql_to_postgresql_manager import (
    MySQLtoPostgreSQLBaseManager,
    MySQLtoPostgreSQLCreateTablesManager,
    MySQLtoPostgreSQLSingleTableManager,
    MySQLtoPostgreSQLFullMigrationManager,
    MySQLtoPostgreSQLDeltaSyncManager,
)
from mysql_to_postgresql_pkg.mysql_postgres_mapping import (
    map_mysql_to_postgres_type,
    get_mysql_type_category,
    transform_data_types
)

__all__ = [
    # Base classes
    "MigrationManager",
    "DataFetcher", 
    "DataWriter",
    # Config
    "MYSQL_CONFIG",
    "POSTGRES_CONFIG",
    # Fetcher and Writer
    "MySQLFetcher",
    "PostgresWriter",
    # Managers - recommended
    "MySQLtoPostgreSQLBaseManager",
    "MySQLtoPostgreSQLCreateTablesManager",
    "MySQLtoPostgreSQLSingleTableManager",
    "MySQLtoPostgreSQLFullMigrationManager",
    "MySQLtoPostgreSQLDeltaSyncManager",
    # Deprecated
    "MySQLtoPostgreSQLMigrationManager",
    # Mapping functions
    "map_mysql_to_postgres_type",
    "get_mysql_type_category",
    "transform_data_types",
]

__version__ = "0.1.0"
