from mysql_to_postgresql_pkg.mysql_to_postgresql_manager import MySQLtoPostgreSQLMigrationManager


class SingleTableScenario:
    """Migrate a single specified table."""

    def __init__(self, table_name, fetcher=None, writer=None, batch_size=None, threads=None):
        self.table_name = table_name
        self.manager = MySQLtoPostgreSQLMigrationManager(fetcher, writer, batch_size=batch_size or 10000, threads=threads)

    def run(self):
        self.manager.create_connections()
        try:
            self.manager.migrate_table(self.table_name)
        finally:
            self.manager.close_connections()
