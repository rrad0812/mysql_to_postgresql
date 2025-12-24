"""CLI runner to invoke migration scenarios."""
import argparse
import logging
from config import MYSQL_CONFIG, POSTGRES_CONFIG
from scenarios import CreateTablesScenario, FullMigrationScenario, SingleTableScenario, DeltaSyncScenario

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Run migration scenarios")
    parser.add_argument("scenario", nargs="?", choices=["create-tables", "full", "single", "delta"],
                        help="Scenario to run")
    parser.add_argument("--table", help="Table name for single/delta scenarios")
    parser.add_argument("--id-column", default="id", help="ID column for delta sync")
    parser.add_argument("--dry-run", action="store_true", help="Don't connect to DB; just print actions")
    parser.add_argument("--config-preview", action="store_true", help="Print resolved DB config and exit")
    parser.add_argument("--threads", type=int, default=None, help="Number of threads for parallel migration")
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
        return

    # Real execution
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
    elif args.scenario == "delta":
        s = DeltaSyncScenario(threads=args.threads)
        s.run(table_name=args.table, id_column=args.id_column)


if __name__ == "__main__":
    main()
