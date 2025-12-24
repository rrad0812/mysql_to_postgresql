from migration_manager import MigrationManager


class FullMigrationScenario:
    """Perform a full migration of all tables from MySQL to PostgreSQL."""

    def __init__(self, fetcher=None, writer=None, batch_size=None, threads=None):
        self.manager = MigrationManager(fetcher, writer,
                                        batch_size=batch_size or 10000,
                                        threads=threads or 4)

    def run(self):
        self.manager.create_connections()
        try:
            # If manager has multiple threads configured, perform per-table parallel migration
            tables = self.manager.fetcher.get_table_list()
            for t in tables:
                if self.manager.threads and self.manager.threads > 1:
                    self.manager.migrate_table_parallel(t)
                else:
                    self.manager.migrate_table(t)
        finally:
            self.manager.close_connections()
