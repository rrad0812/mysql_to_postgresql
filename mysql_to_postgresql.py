import pymysql
import psycopg2
import pandas as pd
import logging
from psycopg2.extras import execute_values
from mysql_postgres_mapping import get_mysql_type_category, transform_data_types, map_mysql_to_postgres_type
from psycopg2 import sql
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# mysql/postgres connection parameters come from env via config.py
from config import MYSQL_CONFIG, POSTGRES_CONFIG

# Constants for parallel processing
BATCH_SIZE = 1000
THREADS = 4

# Mysql database connection
def create_mysql_connection():
    """Create and return a MySQL connection."""
    return pymysql.connect(**MYSQL_CONFIG)

# Postgresql database connection
def create_postgres_connection():
    """Create and return a PostgreSQL connection."""
    return psycopg2.connect(**POSTGRES_CONFIG)

# Get list of tables from MySQL
def get_table_list(mysql_conn):
    """Fetch the list of tables from MySQL."""
    with mysql_conn.cursor() as cursor:
        cursor.execute("SHOW TABLES;")
        return [row[0] for row in cursor.fetchall()]

# Helper functions moved to utils.py: get_mysql_type_category, transform_data_types,
# map_mysql_to_postgres_type

# Helper function to get total rows in a MySQL table.
# Used for batch processing.
def get_total_rows(mysql_conn, table_name):
    """Get the total number of rows in a MySQL table."""
    with mysql_conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        return cursor.fetchone()[0]

# Fetch data in batches from MySQL
# Used for batch processing.
def fetch_data_in_batch(mysql_conn, table_name, offset, batch_size):
    """Fetch a batch of data from MySQL using LIMIT and OFFSET."""
    with mysql_conn.cursor() as cursor:
        query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset};"
        cursor.execute(query)
        return cursor.fetchall()

# Map MySQL data types to PostgreSQL data types
# Used when creating tables in PostgreSQL.
def map_mysql_to_postgres_type(mysql_type):
    """Map MySQL data types to PostgreSQL data types."""
    import re
    mysql_type_lower = mysql_type.lower()
    
    category = get_mysql_type_category(mysql_type)
    
    # Integer types
    if category == "boolean":
        return "BOOLEAN"
    elif category == "tinyint":
        return "SMALLINT"
    elif category == "smallint":
        return "SMALLINT"
    elif category == "bigint":
        return "BIGINT"
    elif category == "int":
        return "INTEGER"
    elif category == "year":
        return "INTEGER"
    
    # Floating point types
    elif category == "float":
        if "decimal" in mysql_type_lower or "numeric" in mysql_type_lower:
            match = re.search(r'\((\d+),(\d+)\)', mysql_type)
            if match:
                return f"NUMERIC({match.group(1)},{match.group(2)})"
            return "NUMERIC"
        elif "double" in mysql_type_lower:
            return "DOUBLE PRECISION"
        else:
            return "REAL"
    
    # String types
    elif category == "string":
        if "char" in mysql_type_lower and "varchar" not in mysql_type_lower:
            match = re.search(r'\((\d+)\)', mysql_type)
            if match:
                return f"CHAR({match.group(1)})"
            return "CHAR(255)"
        elif "varchar" in mysql_type_lower:
            match = re.search(r'\((\d+)\)', mysql_type)
            if match:
                return f"VARCHAR({match.group(1)})"
            return "VARCHAR(255)"
        else:  # text, tinytext, mediumtext, longtext
            return "TEXT"
    
    # Date and time types
    elif category == "datetime":
        return "TIMESTAMP"
    elif category == "date":
        return "DATE"
    elif category == "time":
        return "TIME"
    
    # Binary types
    elif category == "binary":
        return "BYTEA"
    
    # JSON type
    elif category == "json":
        return "JSONB"
    
    # Enum and Set
    elif category == "enum":
        return "VARCHAR(255)"
    
    # Default fallback
    else:
        logger.warning(f"Unknown MySQL type: {mysql_type}, using TEXT")
        return "TEXT"

# Get complete table structure from MySQL
# Used when creating tables in PostgreSQL.
def get_table_structure(mysql_conn, table_name):
    """Get complete table structure including columns, types, nullability, keys, etc."""
    with mysql_conn.cursor() as cursor:
        # Get column information
        cursor.execute(f"DESCRIBE {table_name};")
        columns = cursor.fetchall()
        
        # Get indexes and keys
        cursor.execute(f"SHOW INDEX FROM {table_name};")
        indexes = cursor.fetchall()
        
        return columns, indexes

# Create PostgreSQL table based on MySQL structure
# Used during migration setup.
def create_postgres_table(postgres_conn, mysql_conn, table_name):
    """Create a PostgreSQL table based on MySQL table structure."""
    columns, indexes = get_table_structure(mysql_conn, table_name)
    
    # Build CREATE TABLE statement
    col_definitions = []
    primary_keys = []
    unique_keys = {}
    non_unique_keys = {}
    
    for col in columns:
        col_name = col[0]
        col_type = col[1]
        is_nullable = col[2]
        key = col[3]
        default = col[4]
        extra = col[5]
        
        # Map MySQL type to PostgreSQL type
        pg_type = map_mysql_to_postgres_type(col_type)
        
        # Build column definition
        col_def = f"{col_name} {pg_type}"
        
        # Handle auto_increment/serial
        if "auto_increment" in str(extra).lower():
            if "bigint" in col_type.lower():
                col_def = f"{col_name} BIGSERIAL"
            else:
                col_def = f"{col_name} SERIAL"
        
        # Handle NOT NULL
        if is_nullable == "NO" and "auto_increment" not in str(extra).lower():
            col_def += " NOT NULL"
        
        # Handle default values
        if default is not None and default != "NULL" and "auto_increment" not in str(extra).lower():
            if "CURRENT_TIMESTAMP" in str(default).upper():
                col_def += " DEFAULT CURRENT_TIMESTAMP"
            elif pg_type in ["INTEGER", "BIGINT", "SMALLINT", "REAL", "DOUBLE PRECISION"] or "NUMERIC" in pg_type:
                col_def += f" DEFAULT {default}"
            elif pg_type == "BOOLEAN":
                col_def += f" DEFAULT {default}"
            else:
                col_def += f" DEFAULT '{default}'"
        
        col_definitions.append(col_def)
        
        # Track primary key
        if key == "PRI":
            primary_keys.append(col_name)
    
    # Track unique keys from indexes
    for idx in indexes:
        key_name = idx[2]
        column_name = idx[4]
        non_unique = idx[1]
        
        if non_unique == 0 and key_name != "PRIMARY":
            if key_name not in unique_keys:
                unique_keys[key_name] = []
            unique_keys[key_name].append(column_name)
        elif key_name != "PRIMARY":
            # collect non-unique index columns to create regular indexes later
            if key_name not in non_unique_keys:
                non_unique_keys[key_name] = []
            non_unique_keys[key_name].append(column_name)
    
    # Add primary key constraint
    if primary_keys:
        col_definitions.append(f"PRIMARY KEY ({', '.join(primary_keys)})")
    
    # Add unique constraints
    for key_name, columns in unique_keys.items():
        col_definitions.append(f"UNIQUE ({', '.join(columns)})")
    
    # Create the table
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n  {',\n  '.join(col_definitions)}\n);"
    
    try:
        with postgres_conn.cursor() as cursor:
            logger.info(f"Creating table: {table_name}")

            # logger.setLevel(logging.DEBUG)
            # logger.debug(f"SQL: {create_table_sql}")
            # logger.setLevel(logging.INFO)

            cursor.execute(create_table_sql)
            postgres_conn.commit()

            # create non-unique indexes collected from MySQL SHOW INDEX
            for key_name, cols in non_unique_keys.items():
                raw_idx_name = f"{table_name}_{key_name}_idx"
                idx_name = raw_idx_name[:63]
                try:
                    cursor.execute(
                        sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                            sql.Identifier(idx_name),
                            sql.Identifier(table_name),
                            sql.SQL(', ').join([sql.Identifier(c) for c in cols])
                        )
                    )
                    postgres_conn.commit()
                    logger.info(f"Created index {idx_name} on {table_name}({', '.join(cols)})")
                except Exception as ie:
                    postgres_conn.rollback()
                    logger.error(f"Failed to create index {idx_name} on {table_name}: {ie}")
            logger.info(f"Successfully created table: {table_name}")
    except Exception as e:
        postgres_conn.rollback()
        logger.error(f"Error creating table {table_name}: {e}")
        logger.error(f"SQL was: {create_table_sql}")
        raise

# Insert DataFrame into PostgreSQL
def insert_into_postgres(df, postgres_conn, table_name):
    """Insert DataFrame into PostgreSQL using execute_values for efficiency."""
    if df.empty:
        logger.info(f"No data to insert for {table_name}")
        return
    
    with postgres_conn.cursor() as cursor:
        # Prepare column names and values
        columns = ",".join(df.columns)
        values = [tuple(row) for row in df.values]
        
        # Create insert query
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES %s ON CONFLICT DO NOTHING;"
        
        try:
            execute_values(cursor, insert_query, values)
            postgres_conn.commit()
            logger.info(f"Inserted {len(df)} rows into {table_name}")
        except Exception as e:
            postgres_conn.rollback()
            logger.error(f"Error inserting into {table_name}: {e}")
            raise

# Find missing IDs between MySQL and PostgreSQL
# Used for delta synchronization.
def get_missing_ids(mysql_conn, postgres_conn, table_name, id_column="id"):
    """Find IDs present in MySQL but missing in PostgreSQL."""
    # Get all IDs from MySQL
    with mysql_conn.cursor() as cursor:
        cursor.execute(f"SELECT {id_column} FROM {table_name};")
        mysql_ids = set(row[0] for row in cursor.fetchall())
    
    # Get all IDs from PostgreSQL
    with postgres_conn.cursor() as cursor:
        cursor.execute(f"SELECT {id_column} FROM {table_name};")
        postgres_ids = set(row[0] for row in cursor.fetchall())
    
    # Find the difference
    missing_ids = list(mysql_ids - postgres_ids)
    logger.info(f"Found {len(missing_ids)} missing IDs in {table_name}")
    return sorted(missing_ids)

# Fetch specific rows from MySQL by their IDs
# Used for delta synchronization.
def fetch_missing_rows(mysql_conn, table_name, id_list, id_column="id"):
    """Fetch specific rows from MySQL by their IDs."""
    if not id_list:
        return []
    
    with mysql_conn.cursor() as cursor:
        placeholders = ",".join(["%s"] * len(id_list))
        query = f"SELECT * FROM {table_name} WHERE {id_column} IN ({placeholders});"
        cursor.execute(query, id_list)
        return cursor.fetchall()

# Migrate rows helper function
# Used for both full and delta migrations. 
def migrate_rows(mysql_conn, postgres_conn, table_name, rows):
    """Helper function to transform and insert rows into PostgreSQL."""
    if not rows:
        logger.info(f"No rows to migrate for {table_name}")
        return
    
    columns, _ = get_table_structure(mysql_conn, table_name)
    column_names = [col[0] for col in columns]
    column_types = {col[0]: col[1] for col in columns}
    
    df = pd.DataFrame(rows, columns=column_names)
    df = transform_data_types(df, column_types)
    insert_into_postgres(df, postgres_conn, table_name)

# Migrate a batch of rows by IDs
# Used for parallel delta synchronization.
def migrate_batch(table_name, batch_ids, id_column="id"):
    """Migrate a batch of rows identified by their IDs."""
    mysql_conn = create_mysql_connection()
    postgres_conn = create_postgres_connection()
    
    try:
        rows = fetch_missing_rows(mysql_conn, table_name, batch_ids, id_column)
        migrate_rows(mysql_conn, postgres_conn, table_name, rows)
        return f"Migrated {len(batch_ids)} rows from {table_name}"
    except Exception as e:
        logger.error(f"Error migrating batch for {table_name}: {e}")
        return f"Failed to migrate batch: {e}"
    finally:
        mysql_conn.close()
        postgres_conn.close()

# Get primary key columns for a PostgreSQL table
# Used for sequence updates.
def get_primary_key(cursor, table_name):
    """Get the primary key column(s) for a PostgreSQL table."""
    query = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisprimary;
    """
    cursor.execute(query, (table_name,))
    return [row[0] for row in cursor.fetchall()]

# Migrate full table from MySQL to PostgreSQL
# Used for bulk migration.
def migrate_table(mysql_conn, postgres_conn, table_name):
    """Migrate a full table from MySQL to PostgreSQL."""
    offset = 0
    batch_size = 10000
    total_rows = get_total_rows(mysql_conn, table_name)
    
    logger.info(f"Migrating {total_rows} rows from {table_name}")

    while offset < total_rows:
        rows = fetch_data_in_batch(mysql_conn, table_name, offset, batch_size)
        migrate_rows(mysql_conn, postgres_conn, table_name, rows)
        offset += batch_size
        logger.info(f"Progress: {min(offset, total_rows)}/{total_rows} rows")

# Parallel migration of missing rows
# Used for delta synchronization in large tables.
def migrate_missing_rows_parallel(table_name, id_column="id"):
    """Find and migrate missing rows from MySQL to PostgreSQL in parallel."""
    mysql_conn = create_mysql_connection()
    postgres_conn = create_postgres_connection()

    # Fetch missing IDs
    missing_ids = get_missing_ids(mysql_conn, postgres_conn, table_name, id_column)

    # Process missing rows in parallel
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = []
        for i in range(0, len(missing_ids), BATCH_SIZE):
            batch = missing_ids[i:i + BATCH_SIZE]
            futures.append(executor.submit(migrate_batch, table_name, batch))

        for future in futures:
            logger.info(future.result())

# Migrate missing rows from MySQL to PostgreSQL
# Used for delta synchronization.
def migrate_missing_rows(mysql_conn, postgres_conn, table_name, id_column="id"):
    """Migrate missing rows from MySQL to PostgreSQL."""
    missing_ids = get_missing_ids(mysql_conn, postgres_conn, table_name, id_column)
    
    if not missing_ids:
        logger.info(f"No missing rows for {table_name}")
        return
    
    for i in range(0, len(missing_ids), BATCH_SIZE):
        batch_ids = missing_ids[i:i + BATCH_SIZE]
        rows = fetch_missing_rows(mysql_conn, table_name, batch_ids, id_column)
        migrate_rows(mysql_conn, postgres_conn, table_name, rows)

# Update primary key sequence in PostgreSQL
# Used after data migration to ensure sequences are correct.
def update_sequence(cursor, table_name):
    """Fix the primary key sequence in PostgreSQL after data migration."""
    primary_keys = get_primary_key(cursor, table_name)
    if primary_keys:
        pk_column = primary_keys[0]  # Assuming a single primary key

        cursor.execute(f"SELECT setval(pg_get_serial_sequence('{table_name}', '{pk_column}'), "
                       f"COALESCE((SELECT MAX({pk_column}) FROM {table_name}), 1), true);")
        print(f"Sequence updated for {table_name}.{pk_column}")

def main():
    """Main function to orchestrate the migration process."""
    logger.info("Starting MySQL to PostgreSQL migration...")
    
    # Connect to databases
    mysql_conn = create_mysql_connection()
    postgres_conn = create_postgres_connection()
    
    try:
        # Get list of tables to migrate
        tables = get_table_list(mysql_conn)
        logger.info(f"Found {len(tables)} tables to migrate: {tables}")
        
        # Create tables in PostgreSQL
        logger.info("\n=== Creating tables in PostgreSQL ===")
        for table_name in tables:
            try:
                create_postgres_table(postgres_conn, mysql_conn, table_name)
            except Exception as e:
                logger.error(f"Failed to create table {table_name}: {e}")
                # Continue with other tables even if one fails
                continue
        
        # Migrate data from MySQL to PostgreSQL
        logger.info("\n=== Starting data migration ===")
        for table_name in tables:
            logger.info(f"Migrating table: {table_name}")
            try:
                migrate_table(mysql_conn, postgres_conn, table_name)
                logger.info(f"Successfully migrated {table_name}")
            except Exception as e:
                logger.error(f"Failed to migrate {table_name}: {e}")
                continue
        
        # Parallel migration for large tables
        # Uncomment this for tables with millions of rows
        # logger.info("\n=== Running parallel migration for large tables ===")
        # large_tables = ["users", "orders", "transactions"]  # Specify your large tables
        # for table_name in large_tables:
        #     try:
        #         migrate_missing_rows_parallel(table_name, id_column="id")
        #         logger.info(f"Parallel migration completed for {table_name}")
        #     except Exception as e:
        #         logger.error(f"Parallel migration failed for {table_name}: {e}")
        
        # Fix primary key sequences for all tables
        logger.info("\n=== Updating primary key sequences ===")
        with postgres_conn.cursor() as cursor:
            for table_name in tables:
                try:
                    update_sequence(cursor, table_name)
                    postgres_conn.commit()
                except Exception as e:
                    logger.error(f"Failed to update sequence for {table_name}: {e}")
                    postgres_conn.rollback()
        
        logger.info("\n=== Migration completed successfully! ===")
        
    except Exception as e:
        logger.error(f"Migration failed with error: {e}")
        raise
    finally:
        mysql_conn.close()
        postgres_conn.close()
        logger.info("Database connections closed.")

if __name__ == "__main__":

    MYSQL_CONFIG.update({
        "host": 'host',
        "user": 'user',
        "password": '******',
        "database": '***',
        "port": '****'
    })
    
    MYSQL_CONFIG.update({
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
    # mysql_conn = create_mysql_connection()
    # postgres_conn = create_postgres_connection()
    
    # VARIJANTA 3: Migracija samo jedne tabele
    # mysql_conn = create_mysql_connection()
    # postgres_conn = create_postgres_connection()
    # try:
    #     table_name = "users"  # Promeni ime tabele
    #     create_postgres_table(postgres_conn, mysql_conn, table_name)
    #     migrate_table(mysql_conn, postgres_conn, table_name)
    #     with postgres_conn.cursor() as cursor:
    #         update_sequence(cursor, table_name)
    #         postgres_conn.commit()
    # finally:
    #     mysql_conn.close()
    #     postgres_conn.close()
    
    # VARIJANTA 4: Sinhronizuj samo missing rows (delta sync)
    # Koristi ovo ako već imaš podatke i želiš samo da dodaš nove
    # mysql_conn = create_mysql_connection()
    # postgres_conn = create_postgres_connection()
    # try:
    #     table_name = "orders"  # Promeni ime tabele
    #     migrate_missing_rows(mysql_conn, postgres_conn, table_name, id_column="id")
    # finally:
    #     mysql_conn.close()
    #     postgres_conn.close()
    
    # VARIJANTA 5: Paralelna migracija za velike tabele
    # Koristi ovo za tabele sa milionima redova - brže je
    # migrate_missing_rows_parallel("big_table", id_column="id")
    
    # VARIJANTA 6: Kombinovano - bulk + delta sync
    # mysql_conn = create_mysql_connection()
    # postgres_conn = create_postgres_connection()
    # try:
    #     tables = get_table_list(mysql_conn)
    #     
    #     # Prvo kreiraj tabele i bulk migracija
    #     for table in tables:
    #         create_postgres_table(postgres_conn, mysql_conn, table)
    #         migrate_table(mysql_conn, postgres_conn, table)
    #     
    #     # Zatim proveri i dodaj missing rows
    #     for table in tables:
    #         migrate_missing_rows(mysql_conn, postgres_conn, table, id_column="id")
    #     
    #     # Fix sekvence
    #     with postgres_conn.cursor() as cursor:
    #         for table in tables:
    #             update_sequence(cursor, table)
    #             postgres_conn.commit()
    # finally:
    #     mysql_conn.close()
    #     postgres_conn.close()
