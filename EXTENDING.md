# Primer: Dodavanje MSSQL → PostgreSQL Migracije

Ovo je vodič kako da dodate novu migraciju koristeći postojeći `base.py`.

## Struktura nakon dodavanja MSSQL podrške

```sh
transfer_db1_to_db2/  
├── base.py                           # ⭐ Zajedničke apstraktne klase
├── mysql_to_postgresql_pkg/          # MySQL implementacija
│   ├── mysql_fetcher.py
│   ├── postgres_writer.py
│   ├── mysql_to_postgresql_manager.py
│   └── runner.py
└── mssql_to_postgresql_pkg/          # 🆕 MSSQL implementacija
    ├── __init__.py
    ├── config.py                      # MSSQL konfiguracija
    ├── mssql_fetcher.py               # MSSQL data fetcher
    ├── postgres_writer.py             # reuse ili customize
    ├── mssql_postgres_mapping.py      # MSSQL → PostgreSQL type mapping
    ├── mssql_to_postgresql_manager.py
    ├── runner.py                      # CLI za MSSQL → PostgreSQL
    └── scenarios/
        ├── create_tables_scenario.py
        ├── full_migration_scenario.py
        └── ...
```

## Koraci za implementaciju

1. Kreiraj novi direktorijum

   ```bash
   mkdir mssql_to_postgresql_pkg
   cd mssql_to_postgresql_pkg
   ```

2. Implementiraj MSSQLFetcher (mssql_fetcher.py)

   ```python
   from base import DataFetcher
   import pymssql  # ili pyodbc
   from typing import Optional, List, Tuple, Any
   
   class MSSQLFetcher(DataFetcher):
       def __init__(self):
           self.conn: Optional[pymssql.Connection] = None
   
       def connect(self):
           """Create and return a MSSQL connection."""
           self.conn = pymssql.connect(
               server='localhost',
               user='sa',
               password='your_password',
               database='mydb'
           )
           return self.conn
   
       def close(self) -> None:
           if self.conn:
               self.conn.close()
               self.conn = None
   
       def get_table_list(self) -> List[str]:
           """Fetch the list of tables from MSSQL."""
           with self.conn.cursor() as cursor:
               cursor.execute("""
                   SELECT TABLE_NAME 
                   FROM INFORMATION_SCHEMA.TABLES 
                   WHERE TABLE_TYPE = 'BASE TABLE'
               """)
               return [row[0] for row in cursor.fetchall()]
   
       def get_table_structure(self, table_name: str) -> Tuple[Any, Any]:
           """Get complete table structure."""
           with self.conn.cursor() as cursor:
               # Dobavi kolone
               cursor.execute(f"""
                   SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, 
                          CHARACTER_MAXIMUM_LENGTH, COLUMN_DEFAULT
                   FROM INFORMATION_SCHEMA.COLUMNS
                   WHERE TABLE_NAME = '{table_name}'
               """)
               columns = cursor.fetchall()
               
               # Dobavi indekse
               cursor.execute(f"""
                   SELECT i.name, c.name, i.is_unique, i.is_primary_key
                   FROM sys.indexes i
                   JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                   JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                   WHERE OBJECT_NAME(i.object_id) = '{table_name}'
               """)
               indexes = cursor.fetchall()
               
               return columns, indexes
   
       def fetch_data_in_batch(self, table_name: str, offset: int, batch_size: int) -> List[Any]:
           """Fetch a batch of data using OFFSET/FETCH."""
           with self.conn.cursor() as cursor:
               query = f"""
                   SELECT * FROM {table_name}
                   ORDER BY (SELECT NULL)
                   OFFSET {offset} ROWS
                   FETCH NEXT {batch_size} ROWS ONLY
               """
               cursor.execute(query)
               return cursor.fetchall()
   
       def get_total_rows(self, table_name: str) -> int:
           """Get the total number of rows."""
           with self.conn.cursor() as cursor:
               cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
               return cursor.fetchone()[0]
   
       def fetch_rows_by_ids(self, table_name: str, id_list: List[Any], id_column: str = "id") -> List[Any]:
           """Fetch specific rows by their IDs."""
           if not id_list:
               return []
           
           with self.conn.cursor() as cursor:
               placeholders = ",".join(["%s"] * len(id_list))
               query = f"SELECT * FROM {table_name} WHERE {id_column} IN ({placeholders})"
               cursor.execute(query, id_list)
               return cursor.fetchall()
   ```

3. Kreiraj Type Mapping (mssql_postgres_mapping.py)

   ```python
   import re
   import logging
   
   logger = logging.getLogger(__name__)
   
   def map_mssql_to_postgres_type(mssql_type):
       """Map MSSQL data types to PostgreSQL data types."""
       mssql_type_lower = (mssql_type or "").lower()
   
       mapping = {
           "bit": "BOOLEAN",
           "tinyint": "SMALLINT",
           "smallint": "SMALLINT",
           "int": "INTEGER",
           "bigint": "BIGINT",
           "float": "DOUBLE PRECISION",
           "real": "REAL",
           "datetime": "TIMESTAMP",
           "datetime2": "TIMESTAMP",
           "smalldatetime": "TIMESTAMP",
           "date": "DATE",
           "time": "TIME",
           "datetimeoffset": "TIMESTAMP WITH TIME ZONE",
           "uniqueidentifier": "UUID",
           "xml": "XML",
           "image": "BYTEA",
           "binary": "BYTEA",
           "varbinary": "BYTEA",
       }
   
       # Direct mapping
       for mssql, pg in mapping.items():
           if mssql in mssql_type_lower:
               return pg
   
       # Decimal/Numeric with precision
       if "decimal" in mssql_type_lower or "numeric" in mssql_type_lower:
           m = re.search(r"\((\d+),(\d+)\)", mssql_type)
           if m:
               return f"NUMERIC({m.group(1)},{m.group(2)})"
           return "NUMERIC"
   
       # String types
       if "nvarchar" in mssql_type_lower or "varchar" in mssql_type_lower:
           if "max" in mssql_type_lower:
               return "TEXT"
           m = re.search(r"\((\d+)\)", mssql_type)
           if m:
               return f"VARCHAR({m.group(1)})"
           return "VARCHAR(255)"
   
       if "nchar" in mssql_type_lower or "char" in mssql_type_lower:
           m = re.search(r"\((\d+)\)", mssql_type)
           if m:
               return f"CHAR({m.group(1)})"
           return "CHAR(255)"
   
       if "ntext" in mssql_type_lower or "text" in mssql_type_lower:
           return "TEXT"
   
       logger.warning(f"Unknown MSSQL type: {mssql_type}, using TEXT")
       return "TEXT"
   ```

4. Kreiraj Config (config.py)

   ```python
   """Load MSSQL and PostgreSQL configuration from environment variables."""
   import os
   
   MSSQL_CONFIG = {
       "server": os.getenv("MSSQL_HOST", "localhost"),
       "port": int(os.getenv("MSSQL_PORT", "1433")) if os.getenv("MSSQL_PORT") else 1433,
       "user": os.getenv("MSSQL_USER", "sa"),
       "password": os.getenv("MSSQL_PASSWORD", ""),
       "database": os.getenv("MSSQL_DATABASE", ""),
   }
   
   POSTGRES_CONFIG = {
       "host": os.getenv("POSTGRES_HOST", "localhost"),
       "port": int(os.getenv("POSTGRES_PORT", "5432")) if os.getenv("POSTGRES_PORT") else 5432,
       "user": os.getenv("POSTGRES_USER", "postgres"),
       "password": os.getenv("POSTGRES_PASSWORD", ""),
       "dbname": os.getenv("POSTGRES_DB", ""),
   }
   
   def _clean(config):
       return {k: v for k, v in config.items() if v is not None and v != ""}
   
   MSSQL_CONFIG = _clean(MSSQL_CONFIG)
   POSTGRES_CONFIG = _clean(POSTGRES_CONFIG)
   ```

5. Reuse ili Customize PostgresWriter

   Možete koristiti postojeći `PostgresWriter` iz `mysql_to_postgresql_pkg` ili napraviti prilagođenu verziju za MSSQL specifičnosti.
   
   ```python
   # Reuse postojećeg writer-a
   from mysql_to_postgresql_pkg.postgres_writer import PostgresWriter
   
   # Ili nasledi i prilagodi
   class MSSQLPostgresWriter(PostgresWriter):
       def create_table(self, table_name, mssql_conn):
           # Custom logika za MSSQL strukturu
           pass
   ```

6. Kreiraj MigrationManager

   ```python
   # mssql_to_postgresql_manager.py
   from base import MigrationManager
   from mssql_to_postgresql_pkg.mssql_fetcher import MSSQLFetcher
   from mssql_to_postgresql_pkg.postgres_writer import MSSQLPostgresWriter
   import logging
   
   logger = logging.getLogger(__name__)
   
   class MSSQLtoPostgreSQLMigrationManager(MigrationManager):
       def __init__(self, fetcher=None, writer=None, batch_size=10000, threads=4):
           self.fetcher = fetcher or MSSQLFetcher()
           self.writer = writer or MSSQLPostgresWriter()
           self.batch_size = batch_size
           self.threads = threads
           self.mssql_conn = None
           self.postgres_conn = None
   
       def create_connections(self):
           self.mssql_conn = self.fetcher.connect()
           self.postgres_conn = self.writer.connect()
   
       def close_connections(self):
           self.fetcher.close()
           self.writer.close()
   
       def create_tables(self):
           """Create all PostgreSQL tables based on MSSQL structure."""
           tables = self.fetcher.get_table_list()
           for table in tables:
               logger.info(f"Creating table: {table}")
               self.writer.create_table(table, self.mssql_conn)
   
       def migrate_table(self, table_name):
           """Migrate a single table."""
           # Implementacija slična kao u MySQL verziji
           pass
   
       def migrate_all(self):
           """Migrate all tables."""
           tables = self.fetcher.get_table_list()
           for table in tables:
               self.migrate_table(table)
   ```

7. Kreiraj Runner (runner.py)

   ```python
   # mssql_to_postgresql_pkg/runner.py
   import argparse
   import logging
   from mssql_to_postgresql_pkg.config import MSSQL_CONFIG, POSTGRES_CONFIG
   from mssql_to_postgresql_pkg.scenarios import (
       CreateTablesScenario, 
       FullMigrationScenario, 
       SingleTableScenario
   )
   
   logger = logging.getLogger(__name__)
   
   def main():
       parser = argparse.ArgumentParser(description="MSSQL to PostgreSQL Migration")
       parser.add_argument("scenario", choices=["create-tables", "full", "single"])
       parser.add_argument("--table", help="Table name for single scenario")
       parser.add_argument("--threads", type=int, default=4)
       args = parser.parse_args()
   
       logging.basicConfig(level=logging.INFO)
   
       if args.scenario == "create-tables":
           s = CreateTablesScenario()
           s.run()
       elif args.scenario == "full":
           s = FullMigrationScenario(threads=args.threads)
           s.run()
       elif args.scenario == "single":
           if not args.table:
               raise SystemExit("--table is required for 'single' scenario")
           s = SingleTableScenario(args.table, threads=args.threads)
           s.run()
   
   if __name__ == "__main__":
       main()
   ```

8. Korišćenje

   ```bash
   # Postavi environment varijable
   export MSSQL_HOST=localhost
   export MSSQL_USER=sa
   export MSSQL_PASSWORD=YourPassword123
   export MSSQL_DATABASE=mydb
   
   export POSTGRES_HOST=localhost
   export POSTGRES_USER=postgres
   export POSTGRES_PASSWORD=postgres
   export POSTGRES_DB=mydb_pg
   
   # Pokreni migraciju
   python -m mssql_to_postgresql_pkg.runner full --threads 8
   ```

## Prednosti ovog pristupa

   ✅ **Reuse zajedničkih komponenti** (`base.py`)  
   ✅ **Konsistentna arhitektura** za sve tipove migracija  
   ✅ **Lako održavanje** - izmene u `base.py` utiču na sve  
   ✅ **Skalabilnost** - lako dodavanje novih izvora i ciljeva  
   ✅ **Testabilnost** - svaka komponenta je nezavisno testabilna  

## Buduća proširenja

- `oracle_to_postgresql_pkg/`
- `mongodb_to_postgresql_pkg/`
- `csv_to_postgresql_pkg/`
- `api_to_postgresql_pkg/`
- `mysql_to_clickhouse_pkg/`
- `mssql_to_snowflake_pkg/`
