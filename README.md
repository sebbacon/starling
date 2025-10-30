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

Classification rules live in `config/classification_rules.yaml` and are evaluated in order. Add overrides for specific spaces or counterparties to sit ahead of Starling’s fallback category.

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


## Tests

```bash
just test
```

For coverage details run:

```bash
just coverage
```
