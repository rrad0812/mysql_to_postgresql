import pymysql
import psycopg2
import pandas as pd
import logging
from psycopg2.extras import execute_values
from psycopg2 import sql
from concurrent.futures import ThreadPoolExecutor, as_completed
from base import MigrationManager
from mysql_to_postgresql_pkg.mysql_fetcher import MySQLFetcher
from mysql_to_postgresql_pkg.postgres_writer import PostgresWriter
from mysql_to_postgresql_pkg.mysql_postgres_mapping import get_mysql_type_category, transform_data_types, map_mysql_to_postgres_type
from mysql_to_postgresql_pkg.config import MYSQL_CONFIG, POSTGRES_CONFIG

logger = logging.getLogger(__name__)


class MySQLtoPostgreSQLMigrationManager(MigrationManager):
    """Migration manager for MySQL to PostgreSQL migrations."""
    
    def __init__(self, fetcher=None, writer=None, batch_size=10000, threads=4):
        """Initialize MySQL to PostgreSQL migration manager."""
        self.fetcher = fetcher or MySQLFetcher()
        self.writer = writer or PostgresWriter()
        self.batch_size = batch_size
        self.threads = threads
        self.mysql_conn = None
        self.postgres_conn = None

    def create_mysql_connection(self):
        """Create and return a MySQL connection."""
        return pymysql.connect(**MYSQL_CONFIG)

    def create_postgres_connection(self):
        """Create and return a PostgreSQL connection."""
        return psycopg2.connect(**POSTGRES_CONFIG)

    def create_connections(self):
        """Create connections to MySQL and PostgreSQL."""
        self.mysql_conn = self.fetcher.connect()
        self.postgres_conn = self.writer.connect()

    def close_connections(self):
        """Close all database connections."""
        self.fetcher.close()
        self.writer.close()

    def get_table_list(self):
        """Fetch the list of tables from MySQL."""
        return self.fetcher.get_table_list()

    def get_total_rows(self, table_name):
        """Get the total number of rows in a MySQL table."""
        return self.fetcher.get_total_rows(table_name)

    def fetch_data_in_batch(self, table_name, offset, batch_size):
        """Fetch a batch of data from MySQL using LIMIT and OFFSET."""
        return self.fetcher.fetch_data_in_batch(table_name, offset, batch_size)

    def get_table_structure(self, table_name):
        """Get complete table structure including columns, types, nullability, keys, etc."""
        return self.fetcher.get_table_structure(table_name)

    def create_postgres_table(self, table_name):
        """Create a PostgreSQL table based on MySQL table structure."""
        columns, indexes = self.get_table_structure(table_name)
        
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
        for key_name, columns in unique_keys.items():
            col_definitions.append(f"UNIQUE ({', '.join(columns)})")
        
        # Create the table
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n  {',\n  '.join(col_definitions)}\n);"
        
        try:
            with self.postgres_conn.cursor() as cursor:
                logger.info(f"Creating table: {table_name}")
                cursor.execute(create_table_sql)
                self.postgres_conn.commit()

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
                        self.postgres_conn.commit()
                        logger.info(f"Created index {idx_name} on {table_name}({', '.join(cols)})")
                    except Exception as ie:
                        self.postgres_conn.rollback()
                        logger.error(f"Failed to create index {idx_name} on {table_name}: {ie}")
                logger.info(f"Successfully created table: {table_name}")
        except Exception as e:
            self.postgres_conn.rollback()
            logger.error(f"Error creating table {table_name}: {e}")
            logger.error(f"SQL was: {create_table_sql}")
            raise

    def create_tables(self):
        """Create all tables in PostgreSQL."""
        tables = self.get_table_list()
        for table in tables:
            logger.info(f"Creating table: {table}")
            self.create_postgres_table(table)

    def insert_into_postgres(self, df, table_name):
        """Insert DataFrame into PostgreSQL using execute_values for efficiency."""
        if df.empty:
            logger.info(f"No data to insert for {table_name}")
            return
        
        with self.postgres_conn.cursor() as cursor:
            # Prepare column names and values
            columns = ",".join(df.columns)
            values = [tuple(row) for row in df.values]
            
            # Create insert query
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES %s ON CONFLICT DO NOTHING;"
            
            try:
                execute_values(cursor, insert_query, values)
                self.postgres_conn.commit()
                logger.info(f"Inserted {len(df)} rows into {table_name}")
            except Exception as e:
                self.postgres_conn.rollback()
                logger.error(f"Error inserting into {table_name}: {e}")
                raise

    def migrate_rows(self, table_name, rows):
        """Helper function to transform and insert rows into PostgreSQL."""
        if not rows:
            logger.info(f"No rows to migrate for {table_name}")
            return
        
        columns, _ = self.get_table_structure(table_name)
        column_names = [col[0] for col in columns]
        column_types = {col[0]: col[1] for col in columns}
        
        df = pd.DataFrame(rows, columns=column_names)
        df = transform_data_types(df, column_types)
        self.insert_into_postgres(df, table_name)

    def migrate_table(self, table_name):
        """Migrate a full table from MySQL to PostgreSQL."""
        total = self.get_total_rows(table_name)
        offset = 0
        
        logger.info(f"Migrating {total} rows from {table_name}")

        while offset < total:
            rows = self.fetch_data_in_batch(table_name, offset, self.batch_size)
            self.migrate_rows(table_name, rows)
            offset += self.batch_size
            logger.info(f"Progress: {min(offset, total)}/{total} rows for {table_name}")

    def migrate_table_parallel(self, table_name):
        """Migrate a table using multiple threads each with their own DB connections."""
        total = self.get_total_rows(table_name)
        if total == 0:
            logger.info(f"No rows to migrate for {table_name}")
            return

        offsets = list(range(0, total, self.batch_size))

        # Limit worker count to number of chunks
        workers = min(self.threads or 1, len(offsets))

        # Partition offsets into per-worker lists (round-robin)
        groups = [[] for _ in range(workers)]
        for idx, off in enumerate(offsets):
            groups[idx % workers].append(off)

        def worker(offset_list):
            if not offset_list:
                return 0
            mysql_conn = self.create_mysql_connection()
            postgres_conn = self.create_postgres_connection()
            migrated_count = 0
            try:
                for off in offset_list:
                    try:
                        # Create temporary fetcher/writer for this worker
                        with mysql_conn.cursor() as cursor:
                            query = f"SELECT * FROM {table_name} LIMIT {self.batch_size} OFFSET {off};"
                            cursor.execute(query)
                            rows = cursor.fetchall()
                        
                        if not rows:
                            continue
                        
                        # Transform and insert
                        columns, _ = self.get_table_structure(table_name)
                        column_names = [col[0] for col in columns]
                        column_types = {col[0]: col[1] for col in columns}
                        
                        df = pd.DataFrame(rows, columns=column_names)
                        df = transform_data_types(df, column_types)
                        
                        with postgres_conn.cursor() as cursor:
                            cols = ",".join(df.columns)
                            values = [tuple(row) for row in df.values]
                            insert_query = f"INSERT INTO {table_name} ({cols}) VALUES %s ON CONFLICT DO NOTHING;"
                            execute_values(cursor, insert_query, values)
                            postgres_conn.commit()
                        
                        migrated_count += len(rows)
                    except Exception as e:
                        logger.error(f"Error migrating chunk offset {off} for {table_name}: {e}")
                return migrated_count
            finally:
                try:
                    mysql_conn.close()
                except Exception:
                    pass
                try:
                    postgres_conn.close()
                except Exception:
                    pass

        migrated = 0
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(worker, grp): grp for grp in groups}
            for fut in as_completed(futures):
                try:
                    count = fut.result()
                except Exception as e:
                    logger.error(f"Worker failed for {table_name}: {e}")
                    count = 0
                migrated += count
                logger.info(f"Table {table_name}: migrated {migrated}/{total} rows (chunk completed)")

    def migrate_all(self):
        """Migrate all tables from MySQL to PostgreSQL."""
        tables = self.get_table_list()
        for table in tables:
            self.migrate_table(table)

    def get_missing_ids(self, table_name, id_column="id"):
        """Find IDs present in MySQL but missing in PostgreSQL."""
        # Get all IDs from MySQL
        with self.mysql_conn.cursor() as cursor:
            cursor.execute(f"SELECT {id_column} FROM {table_name};")
            mysql_ids = set(row[0] for row in cursor.fetchall())
        
        # Get all IDs from PostgreSQL
        with self.postgres_conn.cursor() as cursor:
            cursor.execute(f"SELECT {id_column} FROM {table_name};")
            postgres_ids = set(row[0] for row in cursor.fetchall())
        
        # Find the difference
        missing_ids = list(mysql_ids - postgres_ids)
        logger.info(f"Found {len(missing_ids)} missing IDs in {table_name}")
        return sorted(missing_ids)

    def fetch_missing_rows(self, table_name, id_list, id_column="id"):
        """Fetch specific rows from MySQL by their IDs."""
        if not id_list:
            return []
        
        with self.mysql_conn.cursor() as cursor:
            placeholders = ",".join(["%s"] * len(id_list))
            query = f"SELECT * FROM {table_name} WHERE {id_column} IN ({placeholders});"
            cursor.execute(query, id_list)
            return cursor.fetchall()

    def migrate_missing_rows(self, table_name, id_column="id"):
        """Migrate missing rows from MySQL to PostgreSQL."""
        missing = self.get_missing_ids(table_name, id_column)
        
        if not missing:
            logger.info(f"No missing rows for {table_name}")
            return
        
        for i in range(0, len(missing), self.batch_size):
            batch = missing[i:i+self.batch_size]
            rows = self.fetch_missing_rows(table_name, batch, id_column)
            self.migrate_rows(table_name, rows)

    def migrate_missing_rows_parallel(self, table_name, id_column="id"):
        """Find and migrate missing rows from MySQL to PostgreSQL in parallel."""
        missing_ids = self.get_missing_ids(table_name, id_column)
        
        if not missing_ids:
            logger.info(f"No missing rows for {table_name}")
            return

        def migrate_batch(batch_ids):
            """Migrate a batch of rows identified by their IDs."""
            mysql_conn = self.create_mysql_connection()
            postgres_conn = self.create_postgres_connection()
            
            try:
                with mysql_conn.cursor() as cursor:
                    placeholders = ",".join(["%s"] * len(batch_ids))
                    query = f"SELECT * FROM {table_name} WHERE {id_column} IN ({placeholders});"
                    cursor.execute(query, batch_ids)
                    rows = cursor.fetchall()
                
                if not rows:
                    return f"No rows found for batch in {table_name}"
                
                # Transform and insert
                columns, _ = self.get_table_structure(table_name)
                column_names = [col[0] for col in columns]
                column_types = {col[0]: col[1] for col in columns}
                
                df = pd.DataFrame(rows, columns=column_names)
                df = transform_data_types(df, column_types)
                
                with postgres_conn.cursor() as cursor:
                    cols = ",".join(df.columns)
                    values = [tuple(row) for row in df.values]
                    insert_query = f"INSERT INTO {table_name} ({cols}) VALUES %s ON CONFLICT DO NOTHING;"
                    execute_values(cursor, insert_query, values)
                    postgres_conn.commit()
                
                return f"Migrated {len(batch_ids)} rows from {table_name}"
            except Exception as e:
                logger.error(f"Error migrating batch for {table_name}: {e}")
                return f"Failed to migrate batch: {e}"
            finally:
                mysql_conn.close()
                postgres_conn.close()

        # Process missing rows in parallel
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            futures = []
            for i in range(0, len(missing_ids), self.batch_size):
                batch = missing_ids[i:i + self.batch_size]
                futures.append(executor.submit(migrate_batch, batch))

            for future in futures:
                logger.info(future.result())

    def get_primary_key(self, table_name):
        """Get the primary key column(s) for a PostgreSQL table."""
        query = """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary;
        """
        if not self.postgres_conn:
            try:
                self.postgres_conn = self.create_postgres_connection()
            except Exception as e:
                logger.error(f"Could not create postgres connection for get_primary_key: {e}")
                return []

        with self.postgres_conn.cursor() as cursor:
            cursor.execute(query, (table_name,))
            return [row[0] for row in cursor.fetchall()]

    def update_sequence(self, table_name):
        """Fix the primary key sequence in PostgreSQL after data migration."""
        primary_keys = self.get_primary_key(table_name)
        if primary_keys:
            pk_column = primary_keys[0]  # Assuming a single primary key

            if not self.postgres_conn:
                try:
                    self.postgres_conn = self.create_postgres_connection()
                except Exception as e:
                    logger.error(f"Could not create postgres connection for update_sequence: {e}")
                    return

            try:
                with self.postgres_conn.cursor() as cursor:
                    cursor.execute(
                        f"SELECT setval(pg_get_serial_sequence('{table_name}', '{pk_column}'), "
                        f"COALESCE((SELECT MAX({pk_column}) FROM {table_name}), 1), true);"
                    )
                    self.postgres_conn.commit()
                    logger.info(f"Sequence updated for {table_name}.{pk_column}")
            except Exception as e:
                try:
                    self.postgres_conn.rollback()
                except Exception:
                    pass
                logger.error(f"Failed to update sequence for {table_name}.{pk_column}: {e}")

    def update_all_sequences(self):
        """Update sequences for all tables."""
        tables = self.get_table_list()
        for table_name in tables:
            try:
                self.update_sequence(table_name)
            except Exception as e:
                logger.error(f"Failed to update sequence for {table_name}: {e}")
                self.postgres_conn.rollback()

    def full_migration(self):
        """Complete migration: create tables, migrate data, update sequences."""
        logger.info("Starting full MySQL to PostgreSQL migration...")
        
        # Create tables in PostgreSQL
        logger.info("\n=== Creating tables in PostgreSQL ===")
        self.create_tables()
        
        # Migrate data from MySQL to PostgreSQL
        logger.info("\n=== Starting data migration ===")
        tables = self.get_table_list()
        for table_name in tables:
            logger.info(f"Migrating table: {table_name}")
            try:
                self.migrate_table(table_name)
                logger.info(f"Successfully migrated {table_name}")
            except Exception as e:
                logger.error(f"Failed to migrate {table_name}: {e}")
                continue
        
        # Fix primary key sequences for all tables
        logger.info("\n=== Updating primary key sequences ===")
        self.update_all_sequences()
        
        logger.info("\n=== Migration completed successfully! ===")
