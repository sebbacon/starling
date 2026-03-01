# Plan: Daily Salary Allocation and Drawdown Automation

## Goal
Build a daily-run automation command that:
- Detects salary payment into the joint main account (`Kim & Sebastian`) when an inbound payment from `University of Oxford` is between `£5,000` and `£6,000`.
- On first detection for that salary cycle, allocates funds into named spaces with fixed transfers and top-ups.
- Moves `3/4` of the post-allocation remainder into `Salary drawdown`.
- Treats the remainder as `4` equal monthly quarters: the first quarter stays in the main account immediately, and the remaining `3` quarters are released from `Salary drawdown` before day 30, with catch-up if runs were missed.
- Is idempotent when run daily (safe to rerun, no duplicate transfers).

## Hardcoded Business Rules
- Salary detector:
- `counterparty` contains `University of Oxford` (case-insensitive)
- inbound amount between `500000` and `600000` minor units
- source account name exactly `Kim & Sebastian`
- Space allocations on cycle start:
- `Mortgage (monthly)`: transfer `£970`
- `Groceries (monthly)`: transfer `£800`
- `Holidays`: transfer `£400`
- `Bills (monthly)`: top up to `£1100`
- `Kids (monthly)`: top up to `£300`
- Remainder rule:
- `remainder = salary - (fixed transfers + topups)`
- `drawdown_funding = floor(remainder * 3 / 4)`
- immediate remainder stays in main account
- Drawdown release rule:
- split `drawdown_funding` into 3 deterministic tranche amounts summing exactly to the funded total
- tranche due thresholds by days since salary timestamp: `>=8`, `>=15`, `>=23`

## Idempotent Design
- No local state DB/file needed; derive state from Starling feed + deterministic transfer metadata.
- Cycle identity: salary `feedItemUid` (or a deterministic hash if needed).
- Every transfer uses:
- deterministic `transferUid` (UUID5 from `cycle_id + leg_name`)
- deterministic `reference` (e.g. `SALARY-AUTO:{cycle_id}:{leg}`)
- Before creating a transfer leg, query recent feed for existing transfer with same reference; skip if present.
- If the job misses days, it computes which tranches are now due and executes only missing ones.

## Execution Flow (Single Daily Run)
1. Resolve account and spaces by exact name; fail loudly if missing or duplicate.
2. Fetch candidate salary transactions from recent feed window and select the active cycle.
3. If no qualifying salary exists, exit with no-op.
4. Run cycle-start allocation legs (fixed + top-up + drawdown funding), skipping any already executed legs.
5. Compute due drawdown tranches from salary date and execute only missing due tranches back to main account.
6. Emit structured logs for each decision (`executed`, `skipped`, `already_done`, `not_due`, `error`).

## Safety and Failure Policy
- Fail fast on ambiguous account/space matches.
- Fail fast if computed `remainder < 0`.
- Fail fast if source space balance is insufficient for a due tranche.
- Never use fallback names or silent defaults.
- Treat Starling HTTP/schema issues as hard failures (non-zero exit), so GitHub Actions alerts.

## Implementation in This Repo
- Add a new module: `starling_spaces/salary_automation.py`
- Add a management command: `starling_web/spaces/management/commands/run_salary_automation.py`
- Add a `just` recipe: `salary-automation` (daily job entrypoint)
- Reuse existing Starling request helpers/error types where practical.

## Tests
- Unit tests for:
- salary detection filter logic
- top-up arithmetic
- drawdown funding and 3-tranche split
- due-tranche scheduling at day 8/15/23 boundaries
- idempotency checks (existing leg references cause skips)
- Integration-style command tests with mocked Starling API responses for:
- first run on salary day
- rerun same day (no duplicates)
- missed-run catch-up
- schema/API failure behavior
- Run `just coverage` and keep coverage at 100%.

## GitHub Actions Shape
- Daily `cron` + manual `workflow_dispatch`.
- Use repo secret `STARLING_PAT`.
- Add workflow `concurrency` to prevent overlapping runs.
- Command exit code drives alerting (non-zero on hard failure).

## Assumptions to Lock In
- "Other thirds over each quarter of month" is implemented as 4 equal quarters across a 30-day cycle: quarter 1 remains in the main account at cycle start, and quarters 2-4 are paid at approximately +8, +15, and +23 days (never at +30).
- Top-up spaces are topped up once per salary cycle (not re-topped up daily after spending).
- Cycle is anchored to salary transaction date rather than calendar month start/end.
