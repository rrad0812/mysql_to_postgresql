Ready to proceed. Safety checklist (please confirm before I continue):

Do you have recent backups/snapshots of both MySQL and Postgres? (required)
Do you want me to run only dry-runs while you provide credentials, or are you ready to run a live single-table test now?
If you want to start yourself, here are the exact commands to follow.

Preview resolved config (no DB connections)

python3 runner.py --config-preview
Dry-run full migration (no DB connections)

python3 runner.py full --dry-run --threads 8
Dry-run a single table

python3 runner.py single --table your_table_name --dry-run --threads 4
Live single-table migration (after backup)

python3 runner.py single --table your_table_name --threads 4
Live full migration (after verifying single-table)

python3 runner.py full --threads 8
Post-checks after live migration:

Row counts: compare SELECT COUNT(*) on both sides for a few tables.
Sample data integrity: spot-check key rows.
Indexes/sequences: ensure PK/unique/indexes exist and sequences updated.


Laku noć — sve je spremljeno. Kad budeš spreman, možeš pokrenuti:

Preview config (bez konekcija)


python3 runner.py --config-preview
Dry-run pune migracije


python3 runner.py full --dry-run --threads 8
Dry-run jedne tabele


python3 runner.py single --table your_table_name --dry-run --threads 4
Javi sutra šta želiš da radim dalje.