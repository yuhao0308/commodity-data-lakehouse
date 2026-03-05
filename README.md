# Project Gamma — Commodity Trading Data Lakehouse

[![PR Quality Gate](https://github.com/yuhao0308/commodity-data-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/yuhao0308/commodity-data-lakehouse/actions/workflows/ci.yml)
[![Lint](https://github.com/yuhao0308/commodity-data-lakehouse/actions/workflows/lint.yml/badge.svg)](https://github.com/yuhao0308/commodity-data-lakehouse/actions/workflows/lint.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A FinTech-grade ETL system for commodity futures data (Oil, Gold, Wheat) with strict data quality enforcement via the **Write-Audit-Publish** pattern.

---

## Table of Contents

- [Architecture](#architecture)
- [Stack](#stack)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [How It Works](#how-it-works)
- [Environment Variables](#environment-variables)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

---

## Architecture

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

## Stack

| Component          | Technology                                       |
| ------------------ | ------------------------------------------------ |
| **Orchestration**  | Apache Airflow                                   |
| **Data Quality**   | Great Expectations                               |
| **Transformation** | dbt                                              |
| **Lineage**        | OpenLineage + Marquez                            |
| **Warehouse**      | Snowflake / Redshift (Postgres for local dev)    |
| **CI/CD**          | GitHub Actions                                   |
| **Linting**        | Ruff (Python) · SQLFluff (SQL)                   |
| **Containerization** | Docker & Docker Compose                        |

## Project Structure

```
commodity-data-lakehouse/
├── airflow/
│   ├── dags/                    # Airflow DAG definitions
│   ├── plugins/                 # Custom operators, hooks, sensors
│   ├── include/                 # Shared Python modules used by DAGs
│   └── Dockerfile               # Airflow image with deps
├── great_expectations/
│   ├── expectations/            # Expectation suites (JSON/YAML)
│   ├── checkpoints/             # Checkpoint configs
│   └── great_expectations.yml   # GX project config
├── dbt/
│   ├── models/
│   │   ├── staging/             # stg_commodities.sql
│   │   ├── intermediate/        # int_volatility_metrics.sql
│   │   └── marts/               # fct_daily_snapshot.sql
│   ├── tests/                   # Custom dbt tests
│   └── dbt_project.yml
├── lineage/                     # OpenLineage transport config
├── scripts/                     # Bootstrap, seeding, and utility scripts
├── tests/                       # Python tests
├── .github/workflows/           # CI/CD pipelines
├── docker-compose.yml           # Full local dev environment
├── requirements.txt             # Pinned Python dependencies
├── pyproject.toml               # Project metadata & Ruff config
└── CLAUDE.md                    # Detailed architecture & dev guide
```

## Quick Start

### Prerequisites

- **Docker & Docker Compose** (v2+)
- **Python 3.11+**
- **dbt-core** + adapter (`dbt-postgres` for local, `dbt-snowflake` / `dbt-redshift` for cloud)

### Setup

```bash
# 1. Clone and configure
git clone https://github.com/yuhao0308/commodity-data-lakehouse.git
cd commodity-data-lakehouse
cp .env.example .env   # edit as needed

# 2. Start services
docker compose up -d

# 3. Access UIs
open http://localhost:8080   # Airflow  (airflow / airflow)
open http://localhost:3000   # Marquez  lineage UI

# 4. Trigger the pipeline
docker compose exec airflow-webserver airflow dags trigger commodity_etl

# 5. Run dbt locally
cd dbt && dbt deps && dbt run && dbt test

# 6. Tear down
docker compose down -v
```

## How It Works

1. **Extract** — yfinance commodity futures data pulled into `raw_commodities`.
2. **Audit** — Great Expectations validates: not null, unique timestamps, numeric ranges.
3. **Publish** — If validation passes, rows promoted to `published_commodities`. If it fails, pipeline stops and Slack alerts fire.
4. **Transform** — dbt models: `stg_commodities` → `int_volatility_metrics` (Bollinger Bands) → `fct_daily_snapshot`.
5. **Lineage** — OpenLineage emits metadata to Marquez for full traceability.

> **Write-Audit-Publish:** Raw data is never consumed directly. Every row must pass through a Great Expectations checkpoint before being promoted to published tables. Failed validations halt the pipeline and fire a Slack alert — no bad data is ever silently published.

## Environment Variables

Copy `.env.example` to `.env` and fill in the values. Key variables:

| Variable | Purpose | Default |
| --- | --- | --- |
| `COMMODITY_SYMBOLS` | yfinance tickers (comma-separated) | `CL=F,GC=F,ZW=F` |
| `WAREHOUSE_TYPE` | Target warehouse | `postgres` |
| `WAREHOUSE_HOST` | Warehouse host | `localhost` |
| `WAREHOUSE_PORT` | Warehouse port | `5432` |
| `WAREHOUSE_DB` | Database name | `commodity_lakehouse` |
| `WAREHOUSE_USER` | DB username | `loader` |
| `WAREHOUSE_PASSWORD` | DB password | *(secret)* |
| `SLACK_WEBHOOK_URL` | Slack webhook for alerts | — |
| `OPENLINEAGE_URL` | Marquez API endpoint | `http://localhost:5000` |

## Development

### Running Linters

```bash
# Python
ruff check .
ruff format --check .

# SQL
sqlfluff lint dbt/models/ --ignore parsing
```

### Running Tests

```bash
# dbt tests
cd dbt && dbt test

# Great Expectations
cd great_expectations
python -c "
import great_expectations as gx
context = gx.get_context(context_root_dir='.')
result = context.run_checkpoint(checkpoint_name='raw_validation')
assert result.success
"
```

### CI/CD

Every pull request against `main` must pass the **PR Quality Gate** workflow:

- ✅ Python lint (Ruff)
- ✅ SQL lint (SQLFluff)
- ✅ Sample data seeding
- ✅ Great Expectations checkpoint
- ✅ dbt run + test

A separate **Lint** workflow runs on every push to any branch for fast feedback.

## Contributing

1. **Fork** the repository and create your branch from `main`.
2. Follow the branch naming convention: `feat/<topic>`, `fix/<topic>`, `chore/<topic>`.
3. Write [Conventional Commits](https://www.conventionalcommits.org/):
   ```
   feat(dbt): add int_volatility_metrics model
   fix(gx): correct close_price min bound expectation
   chore(ci): add sqlfluff to lint workflow
   ```
4. Ensure all linters pass and tests are green before opening a PR.
5. One feature or fix per branch, one PR per branch.
6. PRs require passing CI and at least one review.

## License

This project is licensed under the [MIT License](LICENSE).

---

<p align="center">
  For detailed architecture decisions, runbooks, and engineering standards, see <a href="CLAUDE.md">CLAUDE.md</a>.
</p>
