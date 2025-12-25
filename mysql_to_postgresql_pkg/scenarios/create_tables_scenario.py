from mysql_to_postgresql_pkg.mysql_to_postgresql_manager import MySQLtoPostgreSQLMigrationManager


class CreateTablesScenario:
    """Create all target PostgreSQL tables based on MySQL structure."""

    def __init__(self, fetcher=None, writer=None):
        self.manager = MySQLtoPostgreSQLMigrationManager(fetcher, writer)

    def run(self):
        with self.manager:
            self.manager.create_tables()
