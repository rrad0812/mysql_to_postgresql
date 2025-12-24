**Project Overview**

This repository contains a small utility to migrate data from MySQL to PostgreSQL. It provides:

- A procedural core in `mysql_to_postgresql.py` for table/schema inspection and row-level migration.
- A `MigrationManager` and scenario classes under `scenarios/` for common workflows.
- A CLI runner `runner.py` to invoke scenarios with `--dry-run` and `--threads` options.

**Environment / Configuration**

Set connection settings via environment variables or export them before running. Supported variables:

- MySQL: `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`
- Postgres: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`

Examples:

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

**Quick CLI Usage**

- Preview resolved config (no DB connection):

```bash
python3 runner.py --config-preview
```

- Dry-run a full migration (no DB connections):

```bash
python3 runner.py full --dry-run --threads 8
```

- Run a single-table migration:

```bash
python3 runner.py single --table users --threads 4
```

- Run delta sync for a single table:

```bash
python3 runner.py delta --table orders --id-column order_id --threads 4
```

**Notes & Recommendations**

- The mapping helpers live in `mysql_postgres_mapping.py`.
- For large tables use `--threads` and tune `--threads` + `batch_size` in `MigrationManager`.
- By default the runner uses `--threads=None` which defers to scenario defaults.
- Consider running with `--dry-run` first to validate config and behavior.

**Next steps**

- Add automated tests and extend README with examples for common edge cases.
