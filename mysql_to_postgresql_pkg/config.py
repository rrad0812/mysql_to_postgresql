"""Load DB configuration from environment variables with sane defaults."""
import os

# MySQL / pymysql connection keys
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", "3306")) if os.getenv("MYSQL_PORT") else None,
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "db": os.getenv("MYSQL_DATABASE", os.getenv("MYSQL_DB", "")),
}

# PostgreSQL / psycopg2 connection keys
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")) if os.getenv("POSTGRES_PORT") else None,
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
    "dbname": os.getenv("POSTGRES_DB", os.getenv("POSTGRES_DATABASE", "")),
}

def _clean(config):
    """Remove None values to keep connection calls happy."""
    return {k: v for k, v in config.items() if v is not None and v != ""}

MYSQL_CONFIG = _clean(MYSQL_CONFIG)
POSTGRES_CONFIG = _clean(POSTGRES_CONFIG)
