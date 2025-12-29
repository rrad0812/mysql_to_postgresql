"""
MySQL to PostgreSQL Migration - Usage Examples

This file demonstrates various ways to use the specialized migration managers.
Run the desired example by uncommenting the corresponding code block.
"""

import logging
from mysql_to_postgresql_manager import (
    MySQLtoPostgreSQLCreateTablesManager,
    MySQLtoPostgreSQLFullMigrationManager,
    MySQLtoPostgreSQLSingleTableManager,
    MySQLtoPostgreSQLDeltaSyncManager
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# =============================================================================
# PRIMER 1: Potpuna migracija (create tables + migrate data + update sequences)
# =============================================================================
def example_full_migration():
    """
    Najčešći scenario - kompletna migracija svih tabela.
    Automatski:
    - Kreira sve tabele u PostgreSQL
    - Migrira sve podatke
    - Ažurira primary key sekvence
    """
    logger.info("=== PRIMER 1: Full Migration ===")
    
    manager = MySQLtoPostgreSQLFullMigrationManager(
        batch_size=10000,
        threads=4,
        parallel=False  # True za paralelnu migraciju unutar tabela
    )
    
    with manager:
        manager.run()


# =============================================================================
# PRIMER 2: Kreiranje samo strukture tabela (bez podataka)
# =============================================================================
def example_create_tables_only():
    """
    Koristi kada želiš samo da kreiraš tabele bez migracije podataka.
    Korisno za:
    - Setup inicijalnih struktura
    - Testiranje table mapping-a
    - Priprema pre ručne migracije
    """
    logger.info("=== PRIMER 2: Create Tables Only ===")
    
    manager = MySQLtoPostgreSQLCreateTablesManager()
    
    with manager:
        manager.run()


# =============================================================================
# PRIMER 3: Migracija samo jedne tabele
# =============================================================================
def example_single_table_migration():
    """
    Migrira samo jednu specifičnu tabelu.
    Koristi kada:
    - Želiš da testiraj migraciju na maloj tabeli
    - Migriraš tabele inkrementalno
    - Restartuješ neuspešnu migraciju
    """
    logger.info("=== PRIMER 3: Single Table Migration ===")
    
    table_name = "users"  # Promeni ime tabele
    
    manager = MySQLtoPostgreSQLSingleTableManager(
        table_name=table_name,
        batch_size=10000,
        threads=4,
        parallel=True  # Koristi paralelnu migraciju za velike tabele
    )
    
    with manager:
        manager.run()


# =============================================================================
# PRIMER 4: Delta sync - samo novi/nedostajući redovi
# =============================================================================
def example_delta_sync_single_table():
    """
    Migrira samo redove koji postoje u MySQL ali ne postoje u PostgreSQL.
    Koristi kada:
    - Već imaš podatke u PostgreSQL
    - Želiš da dodaš samo nove redove
    - Sinhronizuješ incremental promene
    """
    logger.info("=== PRIMER 4: Delta Sync Single Table ===")
    
    table_name = "orders"
    id_column = "order_id"  # Ime kolone koja se koristi za poređenje
    
    manager = MySQLtoPostgreSQLDeltaSyncManager(
        table_name=table_name,
        id_column=id_column,
        batch_size=10000,
        threads=4,
        parallel=True
    )
    
    with manager:
        manager.run()


# =============================================================================
# PRIMER 5: Delta sync za sve tabele
# =============================================================================
def example_delta_sync_all_tables():
    """
    Sinhronizuje sve tabele - pronalazi i migrira missing rows.
    Koristi kada:
    - Želiš da sinhronizuješ celu bazu
    - Dodaješ nove podatke nakon inicijalne migracije
    """
    logger.info("=== PRIMER 5: Delta Sync All Tables ===")
    
    manager = MySQLtoPostgreSQLDeltaSyncManager(
        table_name=None,  # None = sve tabele
        id_column="id",   # default ID kolona
        batch_size=5000,
        threads=8,
        parallel=True
    )
    
    with manager:
        manager.run()


# =============================================================================
# PRIMER 6: Paralelna full migracija (brža za velike baze)
# =============================================================================
def example_parallel_full_migration():
    """
    Full migracija sa paralelnim procesiranjem unutar svake tabele.
    Koristi kada:
    - Imaš tabele sa milionima redova
    - Želiš da maksimalno iskoristiš resurse
    - Želiš najbrži mogući transfer
    
    NAPOMENA: Koristi više DB konekcija - proveri connection limit!
    """
    logger.info("=== PRIMER 6: Parallel Full Migration ===")
    
    manager = MySQLtoPostgreSQLFullMigrationManager(
        batch_size=20000,  # Veći batch za bolje performanse
        threads=16,        # Više thread-ova
        parallel=True      # Paralelno procesiranje
    )
    
    with manager:
        manager.run()


# =============================================================================
# PRIMER 7: Kombinovana migracija - Full + Delta
# =============================================================================
def example_combined_migration():
    """
    Prvo radi full migration, zatim periodično delta sync.
    Koristi kada:
    - Migriraš live sistem
    - Želiš minimalni downtime
    - Potrebna ti inkrementalna sinhronizacija
    """
    logger.info("=== PRIMER 7: Combined Migration ===")
    
    # Korak 1: Full migration
    logger.info("Step 1: Full migration...")
    full_manager = MySQLtoPostgreSQLFullMigrationManager(
        batch_size=10000,
        threads=8,
        parallel=True
    )
    
    with full_manager:
        full_manager.run()
    
    # Korak 2: Delta sync (npr. nakon nekog vremena)
    logger.info("\nStep 2: Delta sync for updates...")
    delta_manager = MySQLtoPostgreSQLDeltaSyncManager(
        table_name=None,  # sve tabele
        id_column="id",
        batch_size=5000,
        threads=4,
        parallel=True
    )
    
    with delta_manager:
        delta_manager.run()


# =============================================================================
# PRIMER 8: Custom workflow - selektivna migracija
# =============================================================================
def example_custom_workflow():
    """
    Kreiraš custom workflow sa više koraka.
    Koristi kada:
    - Trebaš specijalnu logiku
    - Želiš različite postavke za različite tabele
    - Radiš kompleksnu migraciju sa specifičnim zahtevima
    """
    logger.info("=== PRIMER 8: Custom Workflow ===")
    
    # Prvo kreiraj strukture
    logger.info("Creating table structures...")
    create_manager = MySQLtoPostgreSQLCreateTablesManager()
    with create_manager:
        create_manager.run()
    
    # Migriraj male tabele sekvencijalno
    small_tables = ["users", "roles", "permissions"]
    logger.info(f"\nMigrating small tables: {small_tables}")
    for table in small_tables:
        manager = MySQLtoPostgreSQLSingleTableManager(
            table_name=table,
            batch_size=5000,
            threads=2,
            parallel=False
        )
        with manager:
            manager.run()
    
    # Migriraj velike tabele paralelno
    large_tables = ["orders", "transactions", "logs"]
    logger.info(f"\nMigrating large tables in parallel: {large_tables}")
    for table in large_tables:
        manager = MySQLtoPostgreSQLSingleTableManager(
            table_name=table,
            batch_size=50000,
            threads=16,
            parallel=True
        )
        with manager:
            manager.run()


# =============================================================================
# MAIN - Odaberi primer koji želiš da pokreneš
# =============================================================================
if __name__ == "__main__":
    """
    Odkomentiraj željeni primer i pokreni:
    
    python mysql_to_postgresql.py
    """
    
    # Aktivni primer - promeni po potrebi
    example_full_migration()
    
    # Ostali primeri - odkomentiraj da pokreneš
    # example_create_tables_only()
    # example_single_table_migration()
    # example_delta_sync_single_table()
    # example_delta_sync_all_tables()
    # example_parallel_full_migration()
    # example_combined_migration()
    # example_custom_workflow()

