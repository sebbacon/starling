# Starling Spaces Reporter

A small CLI that prints the current configuration and balances for the Spaces attached to your Starling Bank accounts.

## Prerequisites

- Python 3.12 (a `.venv` is created via `uv venv`)
- [`uv`](https://github.com/astral-sh/uv) for dependency management
- A Starling personal access token (PAT) stored as `STARLING_PAT` inside a local `.env` file in this directory

```ini
# .env
STARLING_PAT=replace-with-your-token
```

## Installing dependencies

```bash
uv sync
```

The repo already includes helper tasks via `just`:

- `just test` – run the pytest suite
- `just clean` – auto-format with Ruff and isort
- `just ingest -- --db data/starling_feeds.db` – pull feed history for every space into `data/starling_feeds.db`
- `just average-spend -- --db data/starling_feeds.db` – compute spend averages from the ingested data (supports `--days` and `--reference-time`)

## Usage

Run the CLI with `uv` so dependencies are resolved automatically:

```bash
uv run python -m starling_spaces.cli
```

Optional flags:

- `--account ACCOUNT_UID` – restrict output to one or more account UIDs (you can repeat the flag)
- `--base-url URL` – override the Starling API base URL (defaults to `https://api.starlingbank.com`)
- `--timeout SECONDS` – adjust the HTTP timeout (default `10`)

Example output (field values are illustrative):

```
Account Personal (acc-123) — GBP
  Space Rainy Day (space-1)
    Balance: GBP 1,200.00
    Target: GBP 2,000.00
    State: ACTIVE
    Settings:
      roundUpMultiplier: 2
      sweepEnabled: True
```

If the PAT is invalid or missing, the command exits with a non-zero status and prints a helpful error message to stderr.

## Tests

```bash
just test
```
