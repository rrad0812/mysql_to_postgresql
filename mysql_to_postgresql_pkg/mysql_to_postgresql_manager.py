import sys
from pathlib import Path
# Add parent directory to path so we can import base
sys.path.insert(0, str(Path(__file__).parent.parent))

import pymysql
import psycopg2
import pandas as pd
import logging
from typing import Optional
from psycopg2.extensions import connection as PostgresConnection
from concurrent.futures import ThreadPoolExecutor, as_completed
# from ..base import MigrationManager
from mysql_fetcher import MySQLFetcher
from postgres_writer import PostgresWriter
from mysql_postgres_mapping import transform_data_types
from config import MYSQL_CONFIG, POSTGRES_CONFIG
from base import MigrationManager

logger = logging.getLogger(__name__)


class MySQLtoPostgreSQLMigrationManager(MigrationManager):
    """Migration manager for MySQL to PostgreSQL migrations."""
    
    def __init__(self, fetcher=None, writer=None, batch_size=10000, threads=4):
        """Initialize MySQL to PostgreSQL migration manager."""
        self.fetcher = fetcher or MySQLFetcher()
        self.writer = writer or PostgresWriter()
        self.batch_size = batch_size
        self.threads = threads
        self.mysql_conn: Optional[pymysql.Connection] = None
        self.postgres_conn: Optional[PostgresConnection] = None

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

    def create_tables(self):
        """Create all tables in PostgreSQL."""
        tables = self.get_table_list()
        for table in tables:
            logger.info(f"Creating table: {table}")
            columns, indexes = self.get_table_structure(table)
            self.writer.create_table(table, columns, indexes)

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
        self.writer.insert_into_table(df, table_name)

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
                        
                        # Use writer to insert data
                        temp_writer = PostgresWriter()
                        temp_writer.conn = postgres_conn
                        temp_writer.insert_into_table(df, table_name)
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
        if not self.mysql_conn:
            raise RuntimeError("MySQL connection not established")
        if not self.postgres_conn:
            raise RuntimeError("PostgreSQL connection not established")
        
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
        
        if not self.mysql_conn:
            raise RuntimeError("MySQL connection not established")
        
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
                
                # Use writer to insert data
                temp_writer = PostgresWriter()
                temp_writer.conn = postgres_conn
                temp_writer.insert_into_table(df, table_name)
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
        
        if not self.postgres_conn:
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
            
            if not self.postgres_conn:
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
                if self.postgres_conn:
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
                if self.postgres_conn:
                    try:
                        self.postgres_conn.rollback()
                    except Exception:
                        pass

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
