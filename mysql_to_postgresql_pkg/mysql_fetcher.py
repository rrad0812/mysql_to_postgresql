from base import DataFetcher
import pymysql
from pymysql.connections import Connection
from mysql_to_postgresql_pkg.config import MYSQL_CONFIG
from typing import Optional, List, Tuple, Any


class MySQLFetcher(DataFetcher):
    def __init__(self):
        self.conn: Optional[Connection] = None

    def connect(self) -> Connection:
        """Create and return a MySQL connection."""
        self.conn = pymysql.connect(**MYSQL_CONFIG)
        return self.conn

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_table_list(self) -> List[str]:
        """Fetch the list of tables from MySQL."""
        assert self.conn is not None, "Connection not established. Call connect() first."
        with self.conn.cursor() as cursor:
            cursor.execute("SHOW TABLES;")
            return [row[0] for row in cursor.fetchall()]

    def get_table_structure(self, table_name: str) -> Tuple[Any, Any]:
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

    def fetch_data_in_batch(self, table_name: str, offset: int, batch_size: int) -> List[Any]:
        """Fetch a batch of data from MySQL using LIMIT and OFFSET."""
        assert self.conn is not None, "Connection not established. Call connect() first."
        with self.conn.cursor() as cursor:
            query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset};"
            cursor.execute(query)
            return cursor.fetchall()

    def get_total_rows(self, table_name: str) -> int:
        """Get the total number of rows in a MySQL table."""
        assert self.conn is not None, "Connection not established. Call connect() first."
        with self.conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            return cursor.fetchone()[0]

    def fetch_rows_by_ids(self, table_name: str, id_list: List[Any], id_column: str = "id") -> List[Any]:
        """Fetch specific rows from MySQL by their IDs."""
        if not id_list:
            return []
        
        assert self.conn is not None, "Connection not established. Call connect() first."
        with self.conn.cursor() as cursor:
            placeholders = ",".join(["%s"] * len(id_list))
            query = f"SELECT * FROM {table_name} WHERE {id_column} IN ({placeholders});"
            cursor.execute(query, id_list)
            return cursor.fetchall()
