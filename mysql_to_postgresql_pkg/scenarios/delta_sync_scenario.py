from mysql_to_postgresql_pkg.mysql_to_postgresql_manager import MySQLtoPostgreSQLMigrationManager


class DeltaSyncScenario:
    """Sync missing rows (delta) for one table or all tables.

    If `table_name` is provided, only that table is synced; otherwise all tables
    are scanned and missing rows are fetched.
    """

    def __init__(self, fetcher=None, writer=None, batch_size=None, threads=None):
        self.manager = MySQLtoPostgreSQLMigrationManager(fetcher, writer, batch_size=batch_size or 10000, threads=threads)

    def run(self, table_name=None, id_column="id"):
        self.manager.create_connections()
        try:
            if table_name:
                self.manager.migrate_missing_rows(table_name, id_column=id_column)
            else:
                # iterate all tables and migrate missing rows per-table
                tables = self.manager.fetcher.get_table_list()
                for t in tables:
                    self.manager.migrate_missing_rows(t, id_column=id_column)
        finally:
            self.manager.close_connections()
