# Arhitektura projekta

## Trenutna struktura (posle refaktoringa)

```sh
mysql_to_postgresql/
│
├── 📄 base.py                         ⭐ ZAJEDNIČKA OSNOVA ZA SVE MIGRACIJE
│   ├── class MigrationManager(ABC)   - Apstraktna bazna klasa za managere
│   ├── class DataFetcher(ABC)        - Apstraktna klasa za izvore podataka
│   └── class DataWriter(ABC)         - Apstraktna klasa za ciljeve podataka
│
├── 📂 mysql_to_postgresql_pkg/        MySQL → PostgreSQL implementacija
│   ├── mysql_fetcher.py              - nasledjuje DataFetcher
│   ├── postgres_writer.py            - nasledjuje DataWriter
│   ├── mysql_to_postgresql_manager.py - nasledjuje MigrationManager
│   ├── config.py
│   ├── mysql_postgres_mapping.py
│   ├── runner.py
│   └── scenarios/
│
├── 📂 mssql_to_postgresql_pkg/        🔜 BUDUĆA MSSQL → PostgreSQL impl.
│   ├── mssql_fetcher.py              - nasledjuje DataFetcher iz base.py
│   ├── postgres_writer.py            - reuse ili custom
│   ├── mssql_to_postgresql_manager.py - nasledjuje MigrationManager
│   └── ...
│
├── 📂 csv_to_postgresql_pkg/          🔜 BUDUĆA CSV → PostgreSQL impl.
│   ├── csv_fetcher.py                - nasledjuje DataFetcher iz base.py
│   ├── postgres_writer.py            - reuse postojećeg
│   └── ...
│
└── 📂 mysql_to_clickhouse_pkg/        🔜 BUDUĆA MySQL → ClickHouse impl.
    ├── mysql_fetcher.py              - reuse postojećeg
    ├── clickhouse_writer.py          - nasledjuje DataWriter iz base.py
    └── ...
```

## Dijagram Zavisnosti

```sh
                         ┌─────────────────┐
                         │    base.py      │
                         │  (root level)   │
                         │                 │
                         │ MigrationManager│
                         │   DataFetcher   │
                         │   DataWriter    │
                         └────────┬────────┘
                                  │
                    ┌─────────────┼────────────┐
                    │             │            │
         ┌──────────▼─────────┐   │  ┌─────────▼──────────┐
         │  mysql_to_*_pkg/   │   │  │  mssql_to_*_pkg/   │
         │                    │   │  │                    │
         │  MySQLFetcher      │   │  │  MSSQLFetcher      │
         │  (DataFetcher)     │   │  │  (DataFetcher)     │
         │                    │   │  │                    │
         │  PostgresWriter    │   │  │  PostgresWriter    │
         │  (DataWriter)      │   │  │  (DataWriter)      │
         │                    │   │  │                    │
         │  MySQLPGManager    │   │  │  MSSQLPGManager    │
         │  (MigrationMgr)    │   │  │  (MigrationMgr)    │
         └────────────────────┘   │  └────────────────────┘
                                  │
                       ┌──────────▼─────────┐
                       │   csv_to_*_pkg/    │
                       │                    │
                       │   CSVFetcher       │
                       │   (DataFetcher)    │
                       │                    │
                       │   PostgresWriter   │
                       │   (reuse)          │
                       └────────────────────┘
```

## Prednosti Ovog Pristupa

### 1. ✅ Reusability (Ponovna Upotreba)

- `base.py` deli zajedničku funkcionalnost
- `PostgresWriter` može biti reuse-ovan u svim *_to_postgresql paketima
- Svaki `DataFetcher` se može kombinovati sa bilo kojim `DataWriter`

### 2. ✅ Extensibility (Proširivost)

```python
# Dodavanje novog izvora je trivijalno:
from base import DataFetcher

class MongoDBFetcher(DataFetcher):
    def connect(self): ...
    def get_table_list(self): ...
    # implementiraj ostale metode
```

### 3. ✅ Separation of Concerns (Razdvajanje Odgovornosti)

- **base.py**: Definiše interfejse
- **_fetcher.py**: Odgovoran za čitanje podataka
- **_writer.py**: Odgovoran za pisanje podataka
- **_manager.py**: Orkestrira migraciju
- **_mapping.py**: Type mapping logika

### 4. ✅ Mix & Match Komponente

```python
# Možeš kombinovati bilo koji fetcher sa bilo kojim writer:
from mysql_to_postgresql_pkg.mysql_fetcher import MySQLFetcher
from mssql_to_postgresql_pkg.postgres_writer import PostgresWriter
from base import MigrationManager

# Kreiraj custom migraciju
class CustomMigrationManager(MigrationManager):
    def __init__(self):
        self.fetcher = MySQLFetcher()
        self.writer = PostgresWriter()  # reuse MSSQL-ovog writer-a
```

## Kako dodati novi tip migracije

### Korak po korak za MSSQL → PostgreSQL

1. **Kreiraj direktorijum:**

   ```bash
   mkdir mssql_to_postgresql_pkg
   ```

2. **Implementiraj DataFetcher:**

   ```python
   # mssql_to_postgresql_pkg/mssql_fetcher.py
   from base import DataFetcher
   
   class MSSQLFetcher(DataFetcher):
       # implementacija metoda iz bazne klase
   ```

3. **Reuse ili kreiraj Writer:**

   ```python
   # Option 1: Reuse postojećeg
   from mysql_to_postgresql_pkg.postgres_writer import PostgresWriter
   
   # Option 2: Customize
   from mysql_to_postgresql_pkg.postgres_writer import PostgresWriter
   
   class MSSQLPostgresWriter(PostgresWriter):
       def create_table(self, table_name, mssql_conn):
           # custom logika za MSSQL
   ```

4. **Implementiraj MigrationManager:**

   ```python
   from base import MigrationManager
   from mssql_to_postgresql_pkg.mssql_fetcher import MSSQLFetcher
   
   class MSSQLtoPostgreSQLMigrationManager(MigrationManager):
       # implementacija
   ```

5. **Kreiraj runner i scenarije:**

   ```python
   # mssql_to_postgresql_pkg/runner.py
   # Vrlo sličan postojećem MySQL runner-u
   ```

## Primeri budućih proširenja

- **Izvori Podataka (DataFetcher implementations)**

- ✅ MySQLFetcher (trenutno)
- 🔜 MSSQLFetcher
- 🔜 OracleFetcher
- 🔜 MongoDBFetcher
- 🔜 CSVFetcher
- 🔜 ExcelFetcher
- 🔜 APIFetcher
- 🔜 ParquetFetcher

- **Ciljevi podataka (DataWriter implementations)**

- ✅ PostgresWriter (trenutno)
- 🔜 ClickHouseWriter
- 🔜 BigQueryWriter
- 🔜 SnowflakeWriter
- 🔜 RedshiftWriter
- 🔜 ParquetWriter
- 🔜 DeltaLakeWriter

- **Moguće kombinacije**

- MySQL → PostgreSQL ✅
- MySQL → ClickHouse
- MySQL → BigQuery
- MSSQL → PostgreSQL
- MSSQL → Snowflake
- MongoDB → PostgreSQL
- CSV → PostgreSQL
- API → PostgreSQL
- Oracle → PostgreSQL
- Parquet → PostgreSQL

## Zaključak

Sa `base.py` na root nivou, projekat postaje **platforma za migraciju podataka**.
umesto samo alata za MySQL → PostgreSQL. Svaki novi izvor ili cilj je samo još
jedna implementacija baznih klasa!
