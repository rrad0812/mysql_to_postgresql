from mysql_to_postgresql_pkg.mysql_to_postgresql_manager import MySQLtoPostgreSQLMigrationManager


class FullMigrationScenario:
    """Perform a full migration of all tables from MySQL to PostgreSQL."""

    def __init__(self, fetcher=None, writer=None, batch_size=None, threads=None):
        self.manager = MySQLtoPostgreSQLMigrationManager(fetcher, writer,
                                        batch_size=batch_size or 10000,
                                        threads=threads or 4)

    def run(self):
        with self.manager:
            # If manager has multiple threads configured, perform per-table parallel migration
            tables = self.manager.fetcher.get_table_list()
            for t in tables:
                if self.manager.threads and self.manager.threads > 1:
                    self.manager.migrate_table_parallel(t)
                else:
                    self.manager.migrate_table(t)
