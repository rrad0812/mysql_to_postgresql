
import sys
from pathlib import Path

# Add parent directory to path so we can import base
sys.path.insert(0, str(Path(__file__).parent.parent))

import pymysql
import psycopg2
import pandas as pd
import logging
from typing import Optional, Sequence, Any
from psycopg2.extensions import connection as PostgresConnection
from concurrent.futures import ThreadPoolExecutor, as_completed
from mysql_fetcher import MySQLFetcher
from postgres_writer import PostgresWriter
from mysql_postgres_mapping import transform_data_types
from config import MYSQL_CONFIG, POSTGRES_CONFIG
from base import MigrationManager

logger = logging.getLogger(__name__)


class MySQLtoPostgreSQLBaseManager(MigrationManager):
    """Base manager with minimal shared infrastructure for all MySQL to PostgreSQL migrations.
    
    Only contains:
    - Connection management
    - Helper utilities used by multiple child classes
    """
    
    def __init__(self, fetcher=None, writer=None):
        """Initialize base manager with only fetcher and writer."""
        self.fetcher = fetcher or MySQLFetcher()
        self.writer = writer or PostgresWriter()
        self.mysql_conn: Optional[pymysql.Connection] = None
        self.postgres_conn: Optional[PostgresConnection] = None

    def create_mysql_connection(self):
        """Create and return a MySQL connection - used by parallel workers."""
        return pymysql.connect(**MYSQL_CONFIG)

    def create_postgres_connection(self):
        """Create and return a PostgreSQL connection - used by parallel workers."""
        return psycopg2.connect(**POSTGRES_CONFIG)

    def create_connections(self):
        """Create connections to MySQL and PostgreSQL."""
        self.mysql_conn = self.fetcher.connect()
        self.postgres_conn = self.writer.connect()

    def close_connections(self):
        """Close all database connections."""
        self.fetcher.close()
        self.writer.close()
    
    def transform_and_insert(self, table_name: str, rows: Sequence[Any]):
        """Helper: Transform rows and insert into PostgreSQL."""
        if not rows:
            logger.debug(f"No rows to insert for {table_name}")
            return
        
        columns, _ = self.fetcher.get_table_structure(table_name)
        column_names = [col[0] for col in columns]
        column_types = {col[0]: col[1] for col in columns}
        
        df = pd.DataFrame(rows, columns=column_names)
        df = transform_data_types(df, column_types)
        self.writer.insert_into_table(df, table_name)
    
    def update_sequence(self, table_name: str):
        """Helper: Fix the primary key sequence in PostgreSQL after data migration."""
        query = """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary;
        """
        
        if not self.postgres_conn:
            logger.warning(f"No postgres connection for update_sequence on {table_name}")
            return
        
        with self.postgres_conn.cursor() as cursor:
            cursor.execute(query, (table_name,))
            primary_keys = [row[0] for row in cursor.fetchall()]
        
        if not primary_keys:
            logger.debug(f"No primary key found for {table_name}")
            return
        
        pk_column = primary_keys[0]
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
                self.postgres_conn.rollback()
            logger.error(f"Failed to update sequence for {table_name}.{pk_column}: {e}")
    
    # Abstract methods - child classes must implement
    def create_tables(self):
        raise NotImplementedError("Child classes must implement create_tables()")
    
    def migrate_table(self, table_name: str) -> None:
        raise NotImplementedError("Child classes must implement migrate_table()")
    
    def migrate_all(self) -> None:
        raise NotImplementedError("Child classes must implement migrate_all()")


# ========== Specialized Manager Classes ==========


class MySQLtoPostgreSQLCreateTablesManager(MySQLtoPostgreSQLBaseManager):
    """Manager for creating table structures in PostgreSQL (no data migration)."""
    
    def create_tables(self):
        """Create all tables in PostgreSQL."""
        tables = self.fetcher.get_table_list()
        for table in tables:
            logger.info(f"Creating table: {table}")
            columns, indexes = self.fetcher.get_table_structure(table)
            self.writer.create_table(table, columns, indexes)
    
    def migrate_table(self, table_name: str) -> None:
        raise NotImplementedError("This manager only creates tables")
    
    def migrate_all(self) -> None:
        raise NotImplementedError("This manager only creates tables")
    
    def run(self):
        """Execute table creation."""
        logger.info("Creating all tables in PostgreSQL...")
        self.create_tables()
        logger.info("Table creation completed!")


class MySQLtoPostgreSQLSingleTableManager(MySQLtoPostgreSQLBaseManager):
    """Manager for migrating a single table."""
    
    def __init__(self, table_name: str, fetcher=None, writer=None, batch_size=10000, threads=4, parallel=False):
        super().__init__(fetcher, writer)
        self.table_name = table_name
        self.batch_size = batch_size
        self.threads = threads
        self.parallel = parallel
    
    def create_tables(self):
        """Create the specific table."""
        logger.info(f"Creating table: {self.table_name}")
        columns, indexes = self.fetcher.get_table_structure(self.table_name)
        self.writer.create_table(self.table_name, columns, indexes)
    
    def migrate_table(self, table_name: str) -> None:
        """Migrate table data."""
        # Use self.table_name, not parameter
        if self.parallel and self.threads > 1:
            self._migrate_parallel()
        else:
            self._migrate_sequential()
    
    def _migrate_sequential(self):
        """Migrate table sequentially in batches."""
        total = self.fetcher.get_total_rows(self.table_name)
        offset = 0
        
        logger.info(f"Migrating {total} rows from {self.table_name}")
        
        while offset < total:
            rows = self.fetcher.fetch_data_in_batch(self.table_name, offset, self.batch_size)
            self.transform_and_insert(self.table_name, rows)
            offset += self.batch_size
            logger.info(f"Progress: {min(offset, total)}/{total} rows for {self.table_name}")
    
    def _migrate_parallel(self):
        """Migrate table using parallel workers."""
        total = self.fetcher.get_total_rows(self.table_name)
        if total == 0:
            logger.info(f"No rows to migrate for {self.table_name}")
            return
        
        offsets = list(range(0, total, self.batch_size))
        workers = min(self.threads, len(offsets))
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
                        with mysql_conn.cursor() as cursor:
                            query = f"SELECT * FROM {self.table_name} LIMIT {self.batch_size} OFFSET {off};"
                            cursor.execute(query)
                            rows = cursor.fetchall()
                        
                        if not rows:
                            continue
                        
                        # Transform and insert
                        columns, _ = self.fetcher.get_table_structure(self.table_name)
                        column_names = [col[0] for col in columns]
                        column_types = {col[0]: col[1] for col in columns}
                        
                        df = pd.DataFrame(rows, columns=column_names)
                        df = transform_data_types(df, column_types)
                        
                        temp_writer = PostgresWriter()
                        temp_writer.conn = postgres_conn
                        temp_writer.insert_into_table(df, self.table_name)
                        postgres_conn.commit()
                        
                        migrated_count += len(rows)
                    except Exception as e:
                        logger.error(f"Error migrating chunk offset {off}: {e}")
            finally:
                try:
                    mysql_conn.close()
                except Exception:
                    pass
                try:
                    postgres_conn.close()
                except Exception:
                    pass
            return migrated_count
        
        migrated = 0
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(worker, grp): grp for grp in groups}
            for fut in as_completed(futures):
                try:
                    count = fut.result()
                except Exception as e:
                    logger.error(f"Worker failed: {e}")
                    count = 0
                migrated += count
                logger.info(f"Table {self.table_name}: migrated {migrated}/{total} rows")
    
    def migrate_all(self) -> None:
        """Migrate the single table."""
        self.migrate_table(self.table_name)
    
    def run(self):
        """Execute single table migration."""
        logger.info(f"Starting migration for table: {self.table_name}")
        self.create_tables()
        self.migrate_table(self.table_name)
        self.update_sequence(self.table_name)
        logger.info(f"Migration completed for {self.table_name}!")


class MySQLtoPostgreSQLFullMigrationManager(MySQLtoPostgreSQLBaseManager):
    """Manager for full migration: create tables + migrate all data + update sequences."""
    
    def __init__(self, fetcher=None, writer=None, batch_size=10000, threads=4, parallel=False):
        super().__init__(fetcher, writer)
        self.batch_size = batch_size
        self.threads = threads
        self.parallel = parallel
    
    def create_tables(self):
        """Create all tables in PostgreSQL."""
        tables = self.fetcher.get_table_list()
        for table in tables:
            logger.info(f"Creating table: {table}")
            columns, indexes = self.fetcher.get_table_structure(table)
            self.writer.create_table(table, columns, indexes)
    
    def migrate_table(self, table_name: str) -> None:
        """Migrate a single table."""
        if self.parallel and self.threads > 1:
            # Use SingleTableManager for parallel migration
            single_manager = MySQLtoPostgreSQLSingleTableManager(
                table_name=table_name,
                fetcher=self.fetcher,
                writer=self.writer,
                batch_size=self.batch_size,
                threads=self.threads,
                parallel=True
            )
            # Don't use context manager - connections already open
            single_manager.mysql_conn = self.mysql_conn
            single_manager.postgres_conn = self.postgres_conn
            single_manager._migrate_parallel()
        else:
            # Sequential migration
            total = self.fetcher.get_total_rows(table_name)
            offset = 0
            logger.info(f"Migrating {total} rows from {table_name}")
            
            while offset < total:
                rows = self.fetcher.fetch_data_in_batch(table_name, offset, self.batch_size)
                self.transform_and_insert(table_name, rows)
                offset += self.batch_size
                logger.info(f"Progress: {min(offset, total)}/{total} rows for {table_name}")
    
    def migrate_all(self) -> None:
        """Migrate all tables."""
        tables = self.fetcher.get_table_list()
        for table in tables:
            logger.info(f"Migrating table: {table}")
            try:
                self.migrate_table(table)
                logger.info(f"Successfully migrated {table}")
            except Exception as e:
                logger.error(f"Failed to migrate {table}: {e}")
                continue
    
    def run(self):
        """Execute complete migration workflow."""
        logger.info("Starting full MySQL to PostgreSQL migration...")
        
        logger.info("\n=== Creating tables in PostgreSQL ===")
        self.create_tables()
        
        logger.info("\n=== Starting data migration ===")
        self.migrate_all()
        
        logger.info("\n=== Updating primary key sequences ===")
        tables = self.fetcher.get_table_list()
        for table_name in tables:
            try:
                self.update_sequence(table_name)
            except Exception as e:
                logger.error(f"Failed to update sequence for {table_name}: {e}")
        
        logger.info("\n=== Migration completed successfully! ===")


class MySQLtoPostgreSQLDeltaSyncManager(MySQLtoPostgreSQLBaseManager):
    """Manager for delta synchronization: migrate only missing/new rows."""
    
    def __init__(self, table_name=None, id_column="id", fetcher=None, writer=None, batch_size=10000, threads=4, parallel=False):
        super().__init__(fetcher, writer)
        self.table_name = table_name
        self.id_column = id_column
        self.batch_size = batch_size
        self.threads = threads
        self.parallel = parallel
    
    def create_tables(self):
        raise NotImplementedError("Delta sync assumes tables already exist")
    
    def get_missing_ids(self, table_name: str, id_column: str = "id") -> list:
        """Find IDs present in MySQL but missing in PostgreSQL."""
        if not self.mysql_conn or not self.postgres_conn:
            raise RuntimeError("Connections not established. Call create_connections() first.")
        
        with self.mysql_conn.cursor() as cursor:
            cursor.execute(f"SELECT {id_column} FROM {table_name};")
            mysql_ids = set(row[0] for row in cursor.fetchall())
        
        with self.postgres_conn.cursor() as cursor:
            cursor.execute(f"SELECT {id_column} FROM {table_name};")
            postgres_ids = set(row[0] for row in cursor.fetchall())
        
        missing_ids = list(mysql_ids - postgres_ids)
        logger.info(f"Found {len(missing_ids)} missing IDs in {table_name}")
        return sorted(missing_ids)
    
    def migrate_table(self, table_name: str) -> None:
        """Migrate missing rows for a single table."""
        missing_ids = self.get_missing_ids(table_name, self.id_column)
        
        if not missing_ids:
            logger.info(f"No missing rows for {table_name}")
            return
        
        if self.parallel and self.threads > 1:
            self._migrate_missing_parallel(table_name, missing_ids)
        else:
            self._migrate_missing_sequential(table_name, missing_ids)
    
    def _migrate_missing_sequential(self, table_name: str, missing_ids: list):
        """Migrate missing rows sequentially."""
        if not self.mysql_conn:
            raise RuntimeError("MySQL connection not established")
        
        for i in range(0, len(missing_ids), self.batch_size):
            batch = missing_ids[i:i + self.batch_size]
            
            with self.mysql_conn.cursor() as cursor:
                placeholders = ",".join(["%s"] * len(batch))
                query = f"SELECT * FROM {table_name} WHERE {self.id_column} IN ({placeholders});"
                cursor.execute(query, batch)
                rows = cursor.fetchall()
            
            if rows:
                self.transform_and_insert(table_name, rows)
                logger.info(f"Migrated {len(batch)} missing rows for {table_name}")
    
    def _migrate_missing_parallel(self, table_name: str, missing_ids: list):
        """Migrate missing rows in parallel."""
        def migrate_batch(batch_ids):
            mysql_conn = self.create_mysql_connection()
            postgres_conn = self.create_postgres_connection()
            
            try:
                with mysql_conn.cursor() as cursor:
                    placeholders = ",".join(["%s"] * len(batch_ids))
                    query = f"SELECT * FROM {table_name} WHERE {self.id_column} IN ({placeholders});"
                    cursor.execute(query, batch_ids)
                    rows = cursor.fetchall()
                
                if not rows:
                    return f"No rows found for batch in {table_name}"
                
                columns, _ = self.fetcher.get_table_structure(table_name)
                column_names = [col[0] for col in columns]
                column_types = {col[0]: col[1] for col in columns}
                
                df = pd.DataFrame(rows, columns=column_names)
                df = transform_data_types(df, column_types)
                
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
        
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            futures = []
            for i in range(0, len(missing_ids), self.batch_size):
                batch = missing_ids[i:i + self.batch_size]
                futures.append(executor.submit(migrate_batch, batch))
            
            for future in futures:
                logger.info(future.result())
    
    def migrate_all(self) -> None:
        """Migrate missing rows for all tables."""
        tables = self.fetcher.get_table_list()
        for table in tables:
            logger.info(f"Delta syncing table: {table}")
            try:
                self.migrate_table(table)
            except Exception as e:
                logger.error(f"Failed to delta sync {table}: {e}")
                continue
    
    def run(self):
        """Execute delta synchronization."""
        if self.table_name:
            logger.info(f"Starting delta sync for table: {self.table_name}")
            self.migrate_table(self.table_name)
            logger.info(f"Delta sync completed for {self.table_name}!")
        else:
            logger.info("Starting delta sync for all tables...")
            self.migrate_all()
            logger.info("Delta sync completed for all tables!")
