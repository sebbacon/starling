# Starling Spaces Dashboard

A lightweight Django project that renders a live snapshot of Starling Spaces activity using the Django ORM for persistence. The repository also provides management commands for ingesting feed data, replaying legacy exports, and emitting JSON reports, replacing the previous CLI scripts.

## Prerequisites

- Python 3.12 (create the virtualenv with `uv venv`)
- [`uv`](https://github.com/astral-sh/uv) for dependency management
- Configuration provided through environment variables. Copy `.env.sample` to `.env` and adjust the values, especially `STARLING_PAT` (your Starling personal access token).

## Installing dependencies

```bash
uv sync
```

## Helper tasks

Common tasks are defined in the `justfile`:

- `just dev -- 0.0.0.0:8000` – run the Django development server
- `just ingest` – sync feed data into the Django database
- `just report` – emit the Spaces configuration as JSON
- `just average-spend` – compute spend averages per space and spending category
- `just reclassify-transactions` – re-run custom classification rules for stored feed items
- `just test` – run the pytest suite
- `just coverage` – run the test suite with coverage reporting
- `just clean` – auto-format the Python sources with Ruff and isort

## Usage

### Web dashboard

```bash
just dev -- 0.0.0.0:8000
```

Visit `http://localhost:8000/` to view the summary. The homepage uses htmx to refresh the metrics without a full page reload.

Navigate to `http://localhost:8000/spending/` for a stacked spending chart grouped by preferred categories (spaces first, falling back to transaction categories). By default this view shows the last 12 months of activity; append `?days=180` (or similar) to compare different windows.

Classification rules now live in the database (`ClassificationRule` entries) and are evaluated in ascending `position`. A default set ships with the migration; ongoing tweaks should happen through the admin UI so they apply immediately.

### Management commands

```bash
# ingest feed data from the Starling API
just ingest

# emit the Spaces configuration as JSON
just report

# calculate average spend (defaults to STARLING_SUMMARY_DAYS)
just average-spend

# re-run classification rules against existing feed items
just reclassify-transactions

```

If `STARLING_PAT` is missing, the commands fail fast with a clear error so secrets issues surface immediately.


## Salary automation

A daily automation handles allocating the monthly University of Oxford salary payment across Starling Spaces. It runs automatically via GitHub Actions at 07:00 UTC every day; you can also trigger it manually from the Actions tab.

When it detects a qualifying inbound payment (£5,000–£6,000 from University of Oxford) in the Joint account it:

1. Transfers fixed amounts to Mortgage, Groceries, and Holidays spaces
2. Tops up Bills and Kids spaces to their target balances
3. Moves ¾ of the remaining salary into a Salary drawdown space
4. Releases drawdown back to the main account in three equal tranches at days 8, 15, and 23 of the cycle

The command is **idempotent** — safe to re-run any number of times on the same day without creating duplicate transfers. If it missed a day it will catch up by executing any tranches that are now due.

```bash
# preview what would happen without moving any money
just salary-automation --dry-run

# run for real
just salary-automation
```

The GitHub Actions workflow requires a `STARLING_PAT` secret (with Savings Goals write access) and a `DJANGO_SECRET_KEY` secret configured in the repository's Actions settings.

## Tests

```bash
just test
```

For coverage details run:

```bash
just coverage
```
