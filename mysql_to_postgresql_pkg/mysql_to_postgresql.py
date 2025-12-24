import logging
from mysql_to_postgresql_pkg.mysql_to_postgresql_manager import MySQLtoPostgreSQLMigrationManager
from mysql_to_postgresql_pkg.config import MYSQL_CONFIG, POSTGRES_CONFIG

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main function to orchestrate the migration process."""
    logger.info("Starting MySQL to PostgreSQL migration...")
    
    # Create migration manager
    manager = MySQLtoPostgreSQLMigrationManager()
    
    # Connect to databases
    manager.create_connections()
    
    try:
        # Perform full migration
        manager.full_migration()
        
        logger.info("\n=== Migration completed successfully! ===")
        
    except Exception as e:
        logger.error(f"Migration failed with error: {e}")
        raise
    finally:
        manager.close_connections()
        logger.info("Database connections closed.")


if __name__ == "__main__":
    # Configure database connections
    MYSQL_CONFIG.update({
        "host": 'host',
        "user": 'user',
        "password": '******',
        "database": '***',
        "port": '****'
    })
    
    POSTGRES_CONFIG.update({
        "host": 'host',
        "user": 'user',
        "password": '******',
        "database": '***',
        "port": '****'
    })
        
    # VARIJANTA 1: Kompletna migracija (trenutno aktivna)
    # - Kreira tabele
    # - Migrira sve podatke
    # - Ažurira sekvence
    main()
    
    # VARIJANTA 2: Samo kreiranje tabela (bez podataka)
    # manager = MySQLtoPostgreSQLMigrationManager()
    # manager.create_connections()
    # try:
    #     manager.create_tables()
    # finally:
    #     manager.close_connections()
    
    # VARIJANTA 3: Migracija samo jedne tabele
    # manager = MySQLtoPostgreSQLMigrationManager()
    # manager.create_connections()
    # try:
    #     table_name = "users"  # Promeni ime tabele
    #     manager.create_postgres_table(table_name)
    #     manager.migrate_table(table_name)
    #     manager.update_sequence(table_name)
    # finally:
    #     manager.close_connections()
    
    # VARIJANTA 4: Sinhronizuj samo missing rows (delta sync)
    # Koristi ovo ako već imaš podatke i želiš samo da dodaš nove
    # manager = MySQLtoPostgreSQLMigrationManager()
    # manager.create_connections()
    # try:
    #     table_name = "orders"  # Promeni ime tabele
    #     manager.migrate_missing_rows(table_name, id_column="id")
    # finally:
    #     manager.close_connections()
    
    # VARIJANTA 5: Paralelna migracija za velike tabele
    # Koristi ovo za tabele sa milionima redova - brže je
    # manager = MySQLtoPostgreSQLMigrationManager()
    # manager.create_connections()
    # try:
    #     manager.migrate_missing_rows_parallel("big_table", id_column="id")
    # finally:
    #     manager.close_connections()
    
    # VARIJANTA 6: Kombinovano - bulk + delta sync
    # manager = MySQLtoPostgreSQLMigrationManager()
    # manager.create_connections()
    # try:
    #     tables = manager.get_table_list()
    #     
    #     # Prvo kreiraj tabele i bulk migracija
    #     for table in tables:
    #         manager.create_postgres_table(table)
    #         manager.migrate_table(table)
    #     
    #     # Zatim proveri i dodaj missing rows
    #     for table in tables:
    #         manager.migrate_missing_rows(table, id_column="id")
    #     
    #     # Fix sekvence
    #     manager.update_all_sequences()
    # finally:
    #     manager.close_connections()

