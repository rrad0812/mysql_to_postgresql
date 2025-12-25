import sys
from pathlib import Path
# Add parent directory to path so we can import base
sys.path.insert(0, str(Path(__file__).parent.parent))

from base import DataWriter
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
from psycopg2.extensions import connection as PostgresConnection
from typing import Optional, Any
from config import POSTGRES_CONFIG
from mysql_postgres_mapping import map_mysql_to_postgres_type
import logging

logger = logging.getLogger(__name__)


class PostgresWriter(DataWriter):
    def __init__(self):
        self.conn: Optional[PostgresConnection] = None

    def connect(self):
        """Create and return a PostgreSQL connection."""
        self.conn = psycopg2.connect(**POSTGRES_CONFIG)
        return self.conn

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def create_table(self, table_name: str, columns: tuple, indexes: tuple) -> None:
        """Create a PostgreSQL table based on provided table structure."""
        # Build CREATE TABLE statement
        col_definitions = []
        primary_keys = []
        unique_keys = {}
        non_unique_keys = {}
        
        for col in columns:
            col_name = col[0]
            col_type = col[1]
            is_nullable = col[2]
            key = col[3]
            default = col[4]
            extra = col[5]
            
            # Map MySQL type to PostgreSQL type
            pg_type = map_mysql_to_postgres_type(col_type)
            
            # Build column definition
            col_def = f"{col_name} {pg_type}"
            
            # Handle auto_increment/serial
            if "auto_increment" in str(extra).lower():
                if "bigint" in col_type.lower():
                    col_def = f"{col_name} BIGSERIAL"
                else:
                    col_def = f"{col_name} SERIAL"
            
            # Handle NOT NULL
            if is_nullable == "NO" and "auto_increment" not in str(extra).lower():
                col_def += " NOT NULL"
            
            # Handle default values
            if default is not None and default != "NULL" and "auto_increment" not in str(extra).lower():
                if "CURRENT_TIMESTAMP" in str(default).upper():
                    col_def += " DEFAULT CURRENT_TIMESTAMP"
                elif pg_type in ["INTEGER", "BIGINT", "SMALLINT", "REAL", "DOUBLE PRECISION"] or "NUMERIC" in pg_type:
                    col_def += f" DEFAULT {default}"
                elif pg_type == "BOOLEAN":
                    col_def += f" DEFAULT {default}"
                else:
                    col_def += f" DEFAULT '{default}'"
            
            col_definitions.append(col_def)
            
            # Track primary key
            if key == "PRI":
                primary_keys.append(col_name)
        
        # Track unique keys from indexes
        for idx in indexes:
            key_name = idx[2]
            column_name = idx[4]
            non_unique = idx[1]
            
            if non_unique == 0 and key_name != "PRIMARY":
                if key_name not in unique_keys:
                    unique_keys[key_name] = []
                unique_keys[key_name].append(column_name)
            elif key_name != "PRIMARY":
                # collect non-unique index columns to create regular indexes later
                if key_name not in non_unique_keys:
                    non_unique_keys[key_name] = []
                non_unique_keys[key_name].append(column_name)
        
        # Add primary key constraint
        if primary_keys:
            col_definitions.append(f"PRIMARY KEY ({', '.join(primary_keys)})")
        
        # Add unique constraints
        for key_name, cols in unique_keys.items():
            col_definitions.append(f"UNIQUE ({', '.join(cols)})")
        
        # Create the table
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n  {',\n  '.join(col_definitions)}\n);"
        
        if not self.conn:
            raise RuntimeError("PostgreSQL connection not established")
        
        try:
            with self.conn.cursor() as cursor:
                logger.info(f"Creating table: {table_name}")
                cursor.execute(create_table_sql)
                self.conn.commit()

                # create non-unique indexes collected from MySQL SHOW INDEX
                for key_name, cols in non_unique_keys.items():
                    raw_idx_name = f"{table_name}_{key_name}_idx"
                    idx_name = raw_idx_name[:63]
                    try:
                        cursor.execute(
                            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                                sql.Identifier(idx_name),
                                sql.Identifier(table_name),
                                sql.SQL(', ').join([sql.Identifier(c) for c in cols])
                            )
                        )
                        self.conn.commit()
                        logger.info(f"Created index {idx_name} on {table_name}({', '.join(cols)})")
                    except Exception as ie:
                        if self.conn:
                            self.conn.rollback()
                        logger.error(f"Failed to create index {idx_name} on {table_name}: {ie}")
                logger.info(f"Successfully created table: {table_name}")
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            logger.error(f"Error creating table {table_name}: {e}")
            logger.error(f"SQL was: {create_table_sql}")
            raise

    def insert_into_table(self, df, table_name: str) -> None:
        """Insert DataFrame into PostgreSQL using execute_values for efficiency."""
        if df.empty:
            logger.info(f"No data to insert for {table_name}")
            return
        
        if not self.conn:
            raise RuntimeError("PostgreSQL connection not established")
        
        with self.conn.cursor() as cursor:
            # Prepare column names and values
            columns = ",".join(df.columns)
            values = [tuple(row) for row in df.values]
            
            # Create insert query
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES %s ON CONFLICT DO NOTHING;"
            
            try:
                execute_values(cursor, insert_query, values)
                self.conn.commit()
                logger.info(f"Inserted {len(df)} rows into {table_name}")
            except Exception as e:
                if self.conn:
                    self.conn.rollback()
                logger.error(f"Error inserting into {table_name}: {e}")
                raise

    def update_sequence(self, cursor, table_name):
        """Fix the primary key sequence in PostgreSQL after data migration."""
        # Get primary key column(s)
        query = """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary;
        """
        cursor.execute(query, (table_name,))
        primary_keys = [row[0] for row in cursor.fetchall()]
        
        if primary_keys:
            pk_column = primary_keys[0]  # Assuming a single primary key
            cursor.execute(f"SELECT setval(pg_get_serial_sequence('{table_name}', '{pk_column}'), "
                           f"COALESCE((SELECT MAX({pk_column}) FROM {table_name}), 1), true);")
            logger.info(f"Sequence updated for {table_name}.{pk_column}")
