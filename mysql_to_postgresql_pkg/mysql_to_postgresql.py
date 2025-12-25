import logging
from mysql_to_postgresql_manager import MySQLtoPostgreSQLMigrationManager
from config import MYSQL_CONFIG, POSTGRES_CONFIG

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main function to orchestrate the migration process."""
    logger.info("Starting MySQL to PostgreSQL migration...")
    
    # Create migration manager and use context manager for automatic connection handling
    manager = MySQLtoPostgreSQLMigrationManager()
    
    try:
        with manager:
            # Perform full migration
            manager.full_migration()
            
        logger.info("\n=== Migration completed successfully! ===")
        
    except Exception as e:
        logger.error(f"Migration failed with error: {e}")
        raise


if __name__ == "__main__":
    # VARIJANTA 1: Kompletna migracija (trenutno aktivna)
    # - Kreira tabele
    # - Migrira sve podatke
    # - Ažurira sekvence
    main()
    
    # VARIJANTA 2: Samo kreiranje tabela (bez podataka)
    # manager = MySQLtoPostgreSQLMigrationManager()
    # with manager:
    #     manager.create_tables()
    
    # VARIJANTA 3: Migracija samo jedne tabele
    # manager = MySQLtoPostgreSQLMigrationManager()
    # with manager:
    #     table_name = "users"  # Promeni ime tabele
    #     columns, indexes = manager.get_table_structure(table_name)
    #     manager.writer.create_table(table_name, columns, indexes)
    #     manager.migrate_table(table_name)
    #     manager.update_sequence(table_name)
    
    # VARIJANTA 4: Sinhronizuj samo missing rows (delta sync)
    # Koristi ovo ako već imaš podatke i želiš samo da dodaš nove
    # manager = MySQLtoPostgreSQLMigrationManager()
    # with manager:
    #     table_name = "orders"  # Promeni ime tabele
    #     manager.migrate_missing_rows(table_name, id_column="id")
    
    # VARIJANTA 5: Paralelna migracija za velike tabele
    # Koristi ovo za tabele sa milionima redova - brže je
    # manager = MySQLtoPostgreSQLMigrationManager()
    # with manager:
    #     manager.migrate_missing_rows_parallel("big_table", id_column="id")
    
    # VARIJANTA 6: Kombinovano - bulk + delta sync
    # manager = MySQLtoPostgreSQLMigrationManager()
    # with manager:
    #     tables = manager.get_table_list()
    #     
    #     # Prvo kreiraj tabele i bulk migracija
    #     for table in tables:
    #         columns, indexes = manager.get_table_structure(table)
    #         manager.writer.create_table(table, columns, indexes)
    #         manager.migrate_table(table)
    #     
    #     # Zatim proveri i dodaj missing rows
    #     for table in tables:
    #         manager.migrate_missing_rows(table, id_column="id")
    #     
    #     # Fix sekvence
    #     manager.update_all_sequences()

