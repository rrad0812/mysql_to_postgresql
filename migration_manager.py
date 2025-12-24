from mysql_fetcher import MySQLFetcher
from postgres_writer import PostgresWriter
import mysql_to_postgresql as core
from mysql_postgres_mapping import transform_data_types
import logging

logger = logging.getLogger(__name__)

class MigrationManager:
    def __init__(self, fetcher=None, writer=None, batch_size=10000, threads=4):
        self.fetcher = fetcher or MySQLFetcher()
        self.writer = writer or PostgresWriter()
        self.batch_size = batch_size
        self.threads = threads

    def create_connections(self):
        self.mysql_conn = self.fetcher.connect()
        self.postgres_conn = self.writer.connect()

    def close_connections(self):
        self.fetcher.close()
        self.writer.close()

    def create_tables(self):
        tables = self.fetcher.get_table_list()
        for table in tables:
            logger.info(f"Creating table: {table}")
            self.writer.create_table(table, self.mysql_conn)

    def migrate_table(self, table_name):
        total = self.fetcher.get_total_rows(table_name)
        offset = 0
        while offset < total:
            rows = self.fetcher.fetch_data_in_batch(table_name, offset, self.batch_size)
            columns, _ = self.fetcher.get_table_structure(table_name)
            column_names = [col[0] for col in columns]
            df = core.pd.DataFrame(rows, columns=column_names)
            df = transform_data_types(df, {col[0]: col[1] for col in columns})
            self.writer.insert_rows(df, table_name)
            offset += self.batch_size
            logger.info(f"Progress: {min(offset, total)}/{total} rows for {table_name}")

    def migrate_table_parallel(self, table_name):
        """Migrate a table using multiple threads each with their own DB connections.

        Each worker creates its own MySQL and Postgres connection, fetches a batch,
        transforms and inserts it. This avoids sharing connections across threads.
        """
        total = self.fetcher.get_total_rows(table_name)
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
            mysql_conn = core.create_mysql_connection()
            postgres_conn = core.create_postgres_connection()
            migrated_count = 0
            try:
                for off in offset_list:
                    try:
                        rows = core.fetch_data_in_batch(mysql_conn, table_name, off, self.batch_size)
                        if not rows:
                            continue
                        core.migrate_rows(mysql_conn, postgres_conn, table_name, rows)
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

        from concurrent.futures import ThreadPoolExecutor, as_completed

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
        tables = self.fetcher.get_table_list()
        for table in tables:
            self.migrate_table(table)

    def migrate_missing_rows(self, table_name, id_column="id"):
        missing = core.get_missing_ids(self.mysql_conn, self.postgres_conn, table_name, id_column)
        for i in range(0, len(missing), self.batch_size):
            batch = missing[i:i+self.batch_size]
            rows = self.fetcher.fetch_rows_by_ids(table_name, batch, id_column)
            columns, _ = self.fetcher.get_table_structure(table_name)
            column_names = [col[0] for col in columns]
            df = core.pd.DataFrame(rows, columns=column_names)
            df = transform_data_types(df, {col[0]: col[1] for col in columns})
            self.writer.insert_rows(df, table_name)
