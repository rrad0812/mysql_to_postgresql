import pymysql
from pymysql.connections import Connection
from config import MYSQL_CONFIG
from typing import Optional, List, Tuple, Any

import sys
from pathlib import Path
# Dodaj parent direktorijum u sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from base import DataFetcher

# MySQL Data Fetcher Implementation
class MySQLFetcher(DataFetcher):

    # Constructor. Initialize MySQLFetcher.
    # Sets up the MySQL connection parameters.
    def __init__(self):
        self.conn: Optional[Connection] = None

    def connect(self):
        """Create and return a MySQL connection."""
        self.conn = pymysql.connect(**MYSQL_CONFIG)
        return self.conn

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    # Get list of all tables
    # Returns a list of table names in the MySQL database.
    def get_table_list(self):
        """Fetch the list of tables from MySQL."""
        assert self.conn is not None, "Connection not established. Call connect() first."
        with self.conn.cursor() as cursor:
            cursor.execute("SHOW TABLES;")
            return [row[0] for row in cursor.fetchall()]

    # Get table structure
    # Returns the structure of a specified table including columns and indexes.
    def get_table_structure(self, table_name: str):
        """Get complete table structure including columns, types, nullability, keys, etc."""
        assert self.conn is not None, "Connection not established. Call connect() first."
        with self.conn.cursor() as cursor:
            # Get column information
            cursor.execute(f"DESCRIBE {table_name};")
            columns = cursor.fetchall()
            
            # Get indexes and keys
            cursor.execute(f"SHOW INDEX FROM {table_name};")
            indexes = cursor.fetchall()
            
            return columns, indexes

    # Fetch data in batches
    # Returns a batch of data from the specified table using LIMIT and OFFSET.
    def fetch_data_in_batch(self, table_name:str, offset: int, batch_size: int):
        """Fetch a batch of data from MySQL using LIMIT and OFFSET."""
        assert self.conn is not None, "Connection not established. Call connect() first."
        with self.conn.cursor() as cursor:
            query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset};"
            cursor.execute(query)
            return list(cursor.fetchall())

    # Get total number of rows in table
    # Returns the total row count of the specified table.
    def get_total_rows(self, table_name: str):
        """Get the total number of rows in a MySQL table."""
        assert self.conn is not None, "Connection not established. Call connect() first."
        with self.conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            result = cursor.fetchone()
            if result is None:
                return 0
            return int(result[0])
    
    # Fetch specific rows by their IDs
    # Returns rows from the specified table that match the given list of IDs.
    def fetch_rows_by_ids(self, table_name: str, id_list: List[Any], id_column: str = "id"):
        """Fetch specific rows from MySQL by their IDs."""
        if not id_list:
            return []
        
        assert self.conn is not None, "Connection not established. Call connect() first."
        with self.conn.cursor() as cursor:
            placeholders = ",".join(["%s"] * len(id_list))
            query = f"SELECT * FROM {table_name} WHERE {id_column} IN ({placeholders});"
            cursor.execute(query, id_list)
            return list(cursor.fetchall())
