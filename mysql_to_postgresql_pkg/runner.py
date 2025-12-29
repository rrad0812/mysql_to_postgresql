"""CLI runner to invoke migration scenarios."""
import argparse
import logging
import sys
from pathlib import Path

# Add parent directory to path so we can import base
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import MYSQL_CONFIG, POSTGRES_CONFIG
from mysql_to_postgresql_manager import (
    MySQLtoPostgreSQLCreateTablesManager,
    MySQLtoPostgreSQLFullMigrationManager,
    MySQLtoPostgreSQLSingleTableManager,
    MySQLtoPostgreSQLDeltaSyncManager
)

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Run MySQL to PostgreSQL migration")
    parser.add_argument("scenario", nargs="?", choices=["create-tables", "full", "single", "delta"],
                        help="Migration scenario to run")
    parser.add_argument("--table", help="Table name for single/delta scenarios")
    parser.add_argument("--id-column", default="id", help="ID column for delta sync")
    parser.add_argument("--dry-run", action="store_true", help="Don't connect to DB; just print actions")
    parser.add_argument("--config-preview", action="store_true", help="Print resolved DB config and exit")
    parser.add_argument("--threads", type=int, default=4, help="Number of threads for parallel migration")
    parser.add_argument("--batch-size", type=int, default=10000, help="Batch size for data migration")
    parser.add_argument("--parallel", action="store_true", help="Use parallel migration within tables")
    args = parser.parse_args()

    if args.config_preview:
        print("MYSQL_CONFIG:")
        print(MYSQL_CONFIG)
        print("\nPOSTGRES_CONFIG:")
        print(POSTGRES_CONFIG)
        return

    if not args.scenario:
        parser.print_help()
        return

    if args.dry_run:
        print(f"DRY RUN: would execute scenario '{args.scenario}'")
        if args.scenario == "single":
            print(f"Would migrate single table: {args.table}")
        elif args.scenario == "delta":
            if args.table:
                print(f"Would delta sync table: {args.table}")
            else:
                print("Would delta sync all tables")
        return

    # Real execution
    logging.basicConfig(level=logging.INFO)

    if args.scenario == "create-tables":
        manager = MySQLtoPostgreSQLCreateTablesManager()
        with manager:
            manager.run()
            
    elif args.scenario == "full":
        manager = MySQLtoPostgreSQLFullMigrationManager(
            batch_size=args.batch_size,
            threads=args.threads,
            parallel=args.parallel
        )
        with manager:
            manager.run()
            
    elif args.scenario == "single":
        if not args.table:
            raise SystemExit("--table is required for 'single' scenario")
        manager = MySQLtoPostgreSQLSingleTableManager(
            table_name=args.table,
            batch_size=args.batch_size,
            threads=args.threads,
            parallel=args.parallel
        )
        with manager:
            manager.run()
            
    elif args.scenario == "delta":
        manager = MySQLtoPostgreSQLDeltaSyncManager(
            table_name=args.table,  # None for all tables
            id_column=args.id_column,
            batch_size=args.batch_size,
            threads=args.threads,
            parallel=args.parallel
        )
        with manager:
            manager.run()


if __name__ == "__main__":
    main()
