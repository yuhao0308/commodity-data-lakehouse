# CLAUDE.md — Project Gamma: Commodity Trading Data Lakehouse

## 1. Project Overview

Project Gamma is a FinTech-grade ETL system that ingests commodity futures data (Oil, Gold, Wheat) from yfinance, validates it through strict data quality gates, loads it into a cloud warehouse (Snowflake or Redshift), and transforms it into analytics-ready models via dbt. **Data correctness is a first-class citizen** — we never publish bad data.

The pipeline follows a **Write-Audit-Publish** circuit-breaker pattern: raw data is extracted, validated by Great Expectations checkpoints, and only promoted to published tables if all expectations pass. If validation fails, the pipeline halts and alerts via Slack. Apache Airflow orchestrates all steps, OpenLineage emits run metadata to Marquez for full lineage tracking, and GitHub Actions gates every PR with automated quality checks.

## 2. Architecture

```
                         ┌─────────────────────────────────────┐
                         │         Apache Airflow (DAGs)        │
                         │  orchestrates all tasks, emits OL    │
                         └──┬────────┬────────┬────────┬───────┘
                            │        │        │        │
                   ┌────────▼──┐ ┌───▼────┐ ┌─▼─────┐ │
                   │  Extract   │ │ Audit  │ │ Load  │ │
                   │  (yfinance)│ │  (GX)  │ │ (WH)  │ │
                   └────┬───────┘ └───┬────┘ └──┬────┘ │
                        │             │          │      │
  ┌──────────┐    ┌─────▼─────┐  ┌───▼────┐ ┌──▼──────▼──────┐
  │ yfinance │───►│ Raw JSON  │─►│ GX     │─►│  Snowflake /   │
  │   API    │    │ (S3/local)│  │ Check  │  │  Redshift      │
  └──────────┘    └───────────┘  └───┬────┘  └───────┬────────┘
                                     │               │
                              FAIL: stop +      PASS: publish
                              Slack alert            │
                                              ┌──────▼────────┐
                                              │  dbt models   │
                                              │  stg → int →  │
                                              │  fct snapshots │
                                              └──────┬────────┘
                                                     │
                         ┌───────────────────────────▼─────────┐
                         │  OpenLineage → Marquez               │
                         │  (lineage metadata per run/dataset)  │
                         └──────────────────────────────────────┘
```

**Flow:** yfinance → raw storage → GX validation (circuit breaker) → warehouse → dbt models → analytics. Airflow orchestrates every step. OpenLineage events flow to Marquez for lineage visualization and root-cause analysis.

## 3. Repo Layout (Proposed)

All directories below are **to be created** as the project is bootstrapped.

```
commodity-data-lakehouse/
├── airflow/
│   ├── dags/                    # Airflow DAG definitions
│   │   └── commodity_etl.py     # Main Write-Audit-Publish DAG
│   ├── plugins/                 # Custom operators, hooks, sensors
│   ├── include/                 # Shared Python modules used by DAGs
│   └── Dockerfile               # Airflow image with deps
├── great_expectations/
│   ├── expectations/            # Expectation suites (JSON/YAML)
│   │   └── commodities_raw.json
│   ├── checkpoints/             # Checkpoint configs
│   │   └── raw_validation.yml
│   └── great_expectations.yml   # GX project config
├── dbt/
│   ├── models/
│   │   ├── staging/             # stg_commodities.sql
│   │   ├── intermediate/        # int_volatility_metrics.sql
│   │   └── marts/               # fct_daily_snapshot.sql
│   ├── tests/                   # Custom dbt tests (e.g., volatility >= 0)
│   ├── macros/                  # Reusable SQL macros
│   ├── dbt_project.yml
│   └── profiles.yml.example     # Template — real profiles.yml is gitignored
├── lineage/
│   ├── marquez/                 # Marquez docker-compose overrides, seed data
│   └── openlineage.yml          # OpenLineage transport config
├── scripts/
│   ├── bootstrap.sh             # One-command local setup
│   ├── run_gx_checkpoint.sh     # Run GX validation locally
│   └── seed_sample_data.py      # Generate sample JSON for local dev
├── .github/
│   └── workflows/
│       ├── ci.yml               # PR gate: dbt + GX + lint
│       └── lint.yml             # Python + SQL linting
├── docs/
│   ├── design.md                # Architecture decisions
│   └── runbooks.md              # Incident response
├── docker-compose.yml           # Airflow + Postgres + Marquez + (local WH)
├── requirements.txt             # Python deps (pinned)
├── .env.example                 # Template for required env vars
├── .gitignore
├── CLAUDE.md                    # This file
└── README.md                    # Public-facing project readme (to be added)
```

### Naming Conventions

- **DAGs:** `commodity_etl`, `commodity_backfill` — snake_case, prefixed by domain.
- **dbt models:** `stg_` (staging), `int_` (intermediate), `fct_` (facts/marts). One model per file.
- **GX suites:** named after the dataset they validate, e.g., `commodities_raw`.
- **Python:** snake_case modules and functions. No single-letter variables outside loops.
- **SQL:** lowercase keywords, CTEs preferred over subqueries.

## 4. Local Development

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- dbt-core + adapter (`dbt-postgres` for local, `dbt-snowflake` or `dbt-redshift` for cloud)

### Setup

```bash
# Clone and enter repo
git clone <repo-url> && cd commodity-data-lakehouse

# Copy env template and fill in values
cp .env.example .env
# Edit .env — see "Required Environment Variables" below

# Start all services (Airflow, Postgres, Marquez)
docker compose up -d

# Install Python deps for local scripting
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### Common Commands

```bash
# Airflow UI
open http://localhost:8080        # default user: airflow / airflow

# Trigger the main DAG
docker compose exec airflow-webserver \
  airflow dags trigger commodity_etl

# Run GX checkpoint locally
python -m great_expectations checkpoint run raw_validation \
  --config great_expectations/great_expectations.yml

# dbt (against local Postgres or cloud warehouse)
cd dbt
dbt deps
dbt run --select stg_commodities+    # run staging and all downstream
dbt test                              # run all tests

# Marquez lineage UI
open http://localhost:3000

# Tear down
docker compose down -v
```

### Required Environment Variables

| Variable | Purpose | Example |
|---|---|---|
| `COMMODITY_SYMBOLS` | Comma-separated yfinance tickers | `CL=F,GC=F,ZW=F` |
| `WAREHOUSE_TYPE` | Target warehouse | `postgres` / `snowflake` / `redshift` |
| `WAREHOUSE_HOST` | Warehouse connection host | `localhost` |
| `WAREHOUSE_PORT` | Warehouse connection port | `5432` |
| `WAREHOUSE_DB` | Database name | `commodity_lakehouse` |
| `WAREHOUSE_USER` | DB username | `loader` |
| `WAREHOUSE_PASSWORD` | DB password | (secret) |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook for alerts | `https://hooks.slack.com/...` |
| `OPENLINEAGE_URL` | Marquez API endpoint | `http://localhost:5000` |

For local development, `WAREHOUSE_TYPE=postgres` uses the Postgres container from docker-compose as a warehouse substitute. Set Snowflake/Redshift vars when targeting cloud.

## 5. Data Quality Contracts

### Philosophy

**Fail fast, never publish bad data.** Every row that reaches a published table has passed explicit validation. Silent data corruption is treated as a P0 incident.

### Write-Audit-Publish Pattern

| Stage | Table Prefix | Description |
|---|---|---|
| **Write** | `raw_*` | Untrusted data as-extracted from yfinance. JSON stored as-is. |
| **Audit** | (no table) | GX checkpoint runs against raw data. Binary pass/fail. |
| **Publish** | `stg_*`, `int_*`, `fct_*` | Only populated if Audit passes. Considered trusted. |

If Audit fails, the pipeline **stops immediately** and fires a Slack alert with the expectation failure report. Raw data is preserved for inspection but never promoted.

### Example Great Expectations

```yaml
# great_expectations/expectations/commodities_raw.json (conceptual)
expectations:
  - expectation_type: expect_column_values_to_not_be_null
    kwargs: { column: timestamp }
  - expectation_type: expect_column_values_to_be_unique
    kwargs: { column: timestamp }
  - expectation_type: expect_column_values_to_not_be_null
    kwargs: { column: close_price }
  - expectation_type: expect_column_values_to_be_between
    kwargs: { column: close_price, min_value: 0.01 }
  - expectation_type: expect_column_values_to_not_be_null
    kwargs: { column: symbol }
  - expectation_type: expect_column_values_to_be_in_set
    kwargs: { column: symbol, value_set: ["CL=F", "GC=F", "ZW=F"] }
```

### dbt Tests

Located in `dbt/tests/` and inline in `schema.yml` files:

- **stg_commodities:** not_null on all columns, unique on `(symbol, date)`.
- **int_volatility_metrics:** `volatility >= 0` (custom test — volatility can never be negative).
- **fct_daily_snapshot:** Bollinger Band width > 0, relationships to staging.

Example custom test (`dbt/tests/assert_volatility_non_negative.sql`):

```sql
-- Fails if any row has negative volatility
select *
from {{ ref('int_volatility_metrics') }}
where volatility < 0
```

### Finance Metric: Bollinger Bands

The `int_volatility_metrics` model computes:
- 20-day simple moving average (SMA)
- 20-day rolling standard deviation
- Upper band = SMA + 2 * stddev
- Lower band = SMA - 2 * stddev
- `volatility` = rolling stddev (must be >= 0, enforced by dbt test)

## 6. Lineage & Observability

### OpenLineage Integration

OpenLineage is wired into Airflow via the `openlineage-airflow` provider package. Every task emits START, COMPLETE, and FAIL events automatically.

**Configuration** (`lineage/openlineage.yml`):

```yaml
transport:
  type: http
  url: ${OPENLINEAGE_URL}    # Marquez API
  endpoint: /api/v1/lineage
```

Set `AIRFLOW__OPENLINEAGE__TRANSPORT` to point at this config, or configure via `airflow.cfg`.

### Expected Metadata per Run

| Field | Example |
|---|---|
| Job namespace | `commodity_lakehouse` |
| Job name | `commodity_etl.extract_oil` |
| Input dataset | `yfinance://CL=F` |
| Output dataset | `snowflake://commodity_lakehouse.raw_commodities` |
| Run facets | `extractionComplete`, `rowCount`, `schemaVersion` |

Column-level lineage is a V1 goal via dbt-openlineage integration.

### Marquez UI

Accessible at `http://localhost:3000`. Use it to:

- Trace data flow from source to mart
- Inspect run history and durations
- Identify which downstream models are affected when a source schema changes

### RCA Workflow: Schema Change Breakage

1. A yfinance API change alters column names or types.
2. GX checkpoint fails (unexpected columns / missing columns).
3. Open Marquez → find the failed job → trace downstream datasets.
4. Identify all affected dbt models and expectations.
5. Patch the extractor, update GX suite, update dbt models, re-run.

## 7. CI/CD (GitHub Actions)

### PR Gate (`.github/workflows/ci.yml`)

Every PR must pass before merge:

```yaml
# High-level workflow structure (to be implemented)
name: PR Quality Gate
on: [pull_request]

jobs:
  quality-gate:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_DB: commodity_lakehouse_test
          POSTGRES_USER: test
          POSTGRES_PASSWORD: test
    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Lint (Python)
        run: ruff check . && ruff format --check .

      - name: Lint (SQL)
        run: sqlfluff lint dbt/models/

      - name: Seed sample data
        run: python scripts/seed_sample_data.py

      - name: Run GX validation
        run: |
          python -m great_expectations checkpoint run raw_validation

      - name: dbt run + test
        run: |
          cd dbt
          dbt deps
          dbt run --target ci
          dbt test --target ci

      - name: Upload artifacts
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: quality-reports
          path: |
            great_expectations/uncommitted/data_docs/
            dbt/target/
```

**Key behaviors:**
- PR fails if any lint, GX, or dbt check fails.
- Uses Postgres service container as the CI warehouse.
- `dbt/target/` and GX data docs uploaded as artifacts for inspection.
- Pip caching enabled to speed up runs.

### Lint Workflow (`.github/workflows/lint.yml`)

Runs `ruff` and `sqlfluff` on push to any branch. Fast feedback loop.

## 8. Engineering Standards

### Branching & PRs

- One feature or fix per branch. Branch from `main`.
- Branch naming: `feat/<topic>`, `fix/<topic>`, `chore/<topic>`.
- One PR per branch. PRs require passing CI and one review.

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(dbt): add int_volatility_metrics model
fix(gx): correct close_price min bound expectation
chore(ci): add sqlfluff to lint workflow
```

### Code Style

| Language | Tool | Config |
|---|---|---|
| Python | `ruff` (lint + format) | `pyproject.toml` |
| SQL | `sqlfluff` | `.sqlfluff` (dbt templater) |
| YAML | yamllint | `.yamllint.yml` |

### No Silent Failures

Every Airflow task must have:
- Explicit `on_failure_callback` that sends a Slack alert.
- Retries configured with backoff (default: 2 retries, 5-minute delay).
- Timeouts set (`execution_timeout`) — no task runs forever.

If a task can fail, it must fail loudly.

## 9. Runbooks (Failure Scenarios)

| Incident | Detection | Action |
|---|---|---|
| **GX checkpoint fails** | Airflow task fails, Slack alert | Inspect GX data docs report. Check raw data for anomalies. Fix expectation or data source. Re-trigger DAG. |
| **yfinance schema change** | GX fails on unexpected/missing columns | Open Marquez to trace downstream impact. Patch extractor in `airflow/include/`. Update GX suite. Update dbt staging model if needed. |
| **dbt test fails** | dbt task fails, Slack alert | Check `dbt/target/run_results.json`. Identify failing test. Fix transform logic or update test threshold. Never disable a test without a PR review. |
| **Warehouse load error** | Airflow task fails with DB error | Check connection env vars. Verify warehouse is reachable. All loads must be idempotent (upsert or delete+insert). Safe to retry. |
| **Airflow scheduler down** | No DAG runs, health check fails | `docker compose restart airflow-scheduler`. Check logs at `docker compose logs airflow-scheduler`. |
| **Marquez unreachable** | Lineage events fail (non-blocking) | Lineage emission is fire-and-forget. Pipeline continues. Restart Marquez: `docker compose restart marquez`. Backfill lineage from Airflow logs if needed. |

## 10. Roadmap

### MVP (Current Target)

- [ ] Single DAG (`commodity_etl`) extracting Oil, Gold, Wheat from yfinance
- [ ] Raw JSON storage (local filesystem or S3)
- [ ] GX checkpoint with core expectations (not null, unique timestamp, valid symbols)
- [ ] Load to warehouse (Postgres locally, Snowflake/Redshift in prod)
- [ ] dbt models: `stg_commodities` → `int_volatility_metrics` → `fct_daily_snapshot`
- [ ] Bollinger Bands calculation with volatility non-negative test
- [ ] OpenLineage → Marquez integration for basic lineage
- [ ] GitHub Actions CI with dbt + GX checks
- [ ] Docker Compose for local dev
- [ ] Slack alerting on pipeline failures

### V1

- [ ] Incremental loads (append-only with deduplication)
- [ ] Backfill DAG for historical data
- [ ] Additional GX contracts (distribution checks, freshness)
- [ ] Column-level lineage via dbt-openlineage
- [ ] dbt docs site auto-published to GitHub Pages
- [ ] Dashboard layer (Streamlit or Preset) for volatility metrics
- [ ] More commodities (Natural Gas, Corn, Silver)
- [ ] Scheduled alerting (daily data quality summary)
- [ ] Cost monitoring for warehouse queries

---

## Working with Claude Code

When asking Claude Code to make changes in this repo, use specific prompts like:

- **"Add a new commodity symbol end-to-end"** — Claude will update `COMMODITY_SYMBOLS`, add GX expectations, update dbt staging, and verify lineage.
- **"Create the initial Airflow DAG for commodity_etl"** — Claude will scaffold the DAG with extract, validate, load tasks and failure callbacks.
- **"Write the GX expectation suite for raw commodities"** — Claude will create the JSON suite in `great_expectations/expectations/`.
- **"Scaffold the dbt models for staging through marts"** — Claude will create SQL files with proper refs, CTEs, and schema tests.
- **"Set up docker-compose for local development"** — Claude will create the compose file with Airflow, Postgres, and Marquez services.
- **"Add a new dbt test to enforce volatility >= 0"** — Claude will create the custom test and register it in schema.yml.
- **"Wire OpenLineage into the Airflow DAG"** — Claude will configure the provider and transport settings.
- **"Create the GitHub Actions CI workflow"** — Claude will write the workflow YAML with all quality gates.

When modifying pipeline logic, always ask Claude to update the corresponding GX expectations and dbt tests in the same change. Data quality gates must stay in sync with the data they validate.
