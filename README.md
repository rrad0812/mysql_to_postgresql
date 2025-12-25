
# Migracija baze podataka na drugi tip baze podataka

Ovaj projekat omogućava migraciju podataka sa jednog tipa baze podataka ( MySQL ) na drugi tip baze podataka ( PostgreSQL ).

## Struktura projekta

Projekat je organizovan kao modularni Python paket koji omogućava lako dodavanje novih tipova migracija:

```sh
transfer_db1_to_db2/
├── ARCHITECTURE.md
├── EXTENDING.md
├── README.md
├── base.py                        # ⭐ Zajedničke apstraktne bazne klase
└── mysql_to_postgresql_pkg/       # MySQL → PostgreSQL implementacija
    ├── __init__.py
    ├── config.py                  # Konfiguracija iz env varijabli
    ├── mysql_fetcher.py           # MySQL data fetcher
    ├── postgres_writer.py         # PostgreSQL writer
    ├── requirements.txt
    ├── mysql_postgres_mapping.py  # Mapiranje tipova
    ├── mysql_to_postgresql_manager.py  # Migration manager
    ├── runner.py                      # CLI entry point za MySQL → PostgreSQL
    ├── setup.py
    └── scenarios/                 # Migration scenariji
        ├── __init__.py
        ├── create_tables_scenario.py
        ├── full_migration_scenario.py
        ├── single_table_scenario.py
        └── delta_sync_scenario.py
```

**Napomena:** `base.py` je na root nivou kako bi bio dostupan za sve tipove migracija (npr. budući `mssql_to_postgresql_pkg`, `csv_to_postgresql_pkg`, itd.)

## Instalacija

- **Opcija 1: Instalacija kao paket**

```bash
pip install -e .
```

- **Opcija 2: Instalacija zavisnosti**

```bash
pip install -r requirements.txt
```

## Konfiguracija

Postavite environment varijable za konekciju na baze:

- **MySQL:**
  - `MYSQL_HOST` (default: localhost)
  - `MYSQL_PORT` (default: 3306)
  - `MYSQL_USER` (default: root)
  - `MYSQL_PASSWORD`
  - `MYSQL_DATABASE`

- **PostgreSQL:**
  - `POSTGRES_HOST` (default: localhost)
  - `POSTGRES_PORT` (default: 5432)
  - `POSTGRES_USER` (default: postgres)
  - `POSTGRES_PASSWORD`
  - `POSTGRES_DB`

### Primer

```bash
export MYSQL_HOST=127.0.0.1
export MYSQL_USER=root
export MYSQL_PASSWORD=secret
export MYSQL_DATABASE=mydb

export POSTGRES_HOST=127.0.0.1
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=secret
export POSTGRES_DB=mydb_pg
```

## Upotreba

### CLI Komande

- **Pregled konfiguracije (bez konekcije na bazu)**

```bash
python runner.py --config-preview
```

- **Dry run (simulacija bez izvršenja)**

```bash
python runner.py full --dry-run --threads 8
```

- **Kreiranje tabela u PostgreSQL**

```bash
python runner.py create-tables
```

- **Potpuna migracija svih tabela**

```bash
python runner.py full --threads 8
```

- **Migracija jedne tabele**

```bash
python runner.py single --table users --threads 4
```

- **Delta sinhronizacija (migracija samo novih redova)**

```bash
python runner.py delta --table orders --id-column order_id --threads 4
```

- **Upotreba kao Python Paket**

```python
from mysql_to_postgresql_pkg import MySQLtoPostgreSQLMigrationManager
from mysql_to_postgresql_pkg.scenarios import FullMigrationScenario

# Kreiranje migration manager-a
manager = MySQLtoPostgreSQLMigrationManager(batch_size=10000, threads=4)

# Ili korišćenje scenario klasa
scenario = FullMigrationScenario(threads=8)
scenario.run()
```

## Napredne Opcije

- **Paralelna Migracija**

  Koristite `--threads` opciju za paralelno procesiranje:
  
  ```bash
  python runner.py full --threads 16
  ```

- **Prilagođavanje Batch Veličine**

  Batch veličina se može podesiti u kodu:
  
  ```python
  manager = MySQLtoPostgreSQLMigrationManager(batch_size=50000, threads=8)
  ```

## Karakteristike

- ✅ Automatsko mapiranje MySQL -> PostgreSQL tipova
- ✅ Podrška za indekse, primarne i strane ključeve
- ✅ Paralelna migracija sa thread pool-om
- ✅ Delta sinhronizacija (migracija samo novih podataka)
- ✅ Dry-run mod za testiranje
- ✅ CLI interfejs sa多nim opcijama
- ✅ Modularni scenario sistem

## Arhitektura

### Bazne Klase (base.py)

- `MigrationManager`: Apstraktna bazna klasa za sve tipove migracija
- `DataFetcher`: Apstraktna klasa za izvore podataka
- `DataWriter`: Apstraktna klasa za ciljeve podataka

### Implementacije

- `MySQLFetcher`: Implementacija za čitanje iz MySQL-a
- `PostgresWriter`: Implementacija za pisanje u PostgreSQL
- `MySQLtoPostgreSQLMigrationManager`: Glavni manager za migraciju

### Scenariji

Scenario klase omogućavaju različite tipove migracija:

- `CreateTablesScenario`: Samo kreira tabele
- `FullMigrationScenario`: Potpuna migracija
- `SingleTableScenario`: Jedna tabela
- `DeltaSyncScenario`: Samo novi podaci

## Proširenja i Dodavanje Novih Tipova Migracija

Paket je dizajniran da bude lako proširiv. `base.py` sadrži apstraktne klase koje možete nasleđivati:

### Primer: Kreiranje MSSQL → PostgreSQL migracije

1. **Kreiraj novi direktorijum:**

   ```bash
   mkdir mssql_to_postgresql_pkg
   ```

2. **Implementiraj DataFetcher za MSSQL:**

   ```python
   # mssql_to_postgresql_pkg/mssql_fetcher.py
   from base import DataFetcher
   import pymssql
   
   class MSSQLFetcher(DataFetcher):
       def connect(self):
           return pymssql.connect(...)
       # ... implementacija ostalih metoda
   ```

3. **Reuse postojećeg PostgreSQLWriter-a** ili kreiraj novi

4. **Kreiraj MigrationManager:**

   ```python
   # mssql_to_postgresql_pkg/mssql_to_postgresql_manager.py
   from base import MigrationManager
   from mssql_to_postgresql_pkg.mssql_fetcher import MSSQLFetcher
   from mysql_to_postgresql_pkg.postgres_writer import PostgresWriter
   
   class MSSQLtoPostgreSQLMigrationManager(MigrationManager):
       # ... implementacija
   ```

5. **Kreiraj runner za MSSQL:**

   ```python
   # mssql_runner.py
   from mssql_to_postgresql_pkg import MSSQLtoPostgreSQLMigrationManager
   ```

### Moguća proširenja

- **Novi izvori:** CSV, Excel, MongoDB, API, MSSQL, Oracle
- **Novi ciljevi:** Parquet, ClickHouse, BigQuery, Snowflake
- **Novi scenariji:** Incremental sync, CDC, Data validation

### Preporuke

- Za velike tabele koristite više thread-ova
- Testirajte prvo sa `--dry-run`
- Podesite `batch_size` prema veličini RAM-a
- Koristite `delta sync` za inkrementalne update-e

### Sledeći Koraci

- Dodavanje automatskih testova
- Podrška za više izvora i ciljeva podataka
- Web UI za monitoring migracija
- Logovanje i izveštavanje napretka
