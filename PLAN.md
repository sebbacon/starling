# Plan: Migrate feed storage to Django ORM

## Stage 1: Model & Migration Setup
- Define Django models for feed data (`FeedItem`, `Category`, `SyncState`) mirroring the existing SQLite schema.
- Generate initial migrations to create these tables.
- Add a management command to ingest existing SQLite data into the new tables or re-run ingestion to populate them.
- Ensure the admin registers these models for inspection.

## Stage 2: Refactor Ingestion & Analytics
- Rewrite the ingestion pipeline (`sync_space_feeds` et al.) to use the ORM models, removing direct SQLite usage.
- Update analytics functions to operate via Django ORM queries.
- Adjust management commands to drop filesystem `--db` arguments and rely on the Django database.

## Stage 3: Testing & Tooling Updates
- Update fixtures/tests to use Django ORM setup instead of raw SQLite files.
- Add integration tests covering ingestion, analytics, admin registration, and reclassification with the new models.
- Refresh documentation (`README`, `justfile`, etc.) to reflect the ORM-backed setup and new commands.

## Stage 4: Data Migration & Cleanup
- Provide instructions/commands to migrate legacy data (e.g., re-run ingestion or execute the migration command).
- Remove obsolete SQLite-specific helper functions (`_ensure_schema`, etc.) and dead code paths.
- Verify coverage and linting remain green post-migration.
