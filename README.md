# Project Gamma — Commodity Trading Data Lakehouse

A FinTech-grade ETL system for commodity futures data (Oil, Gold, Wheat) with strict data quality enforcement via the **Write-Audit-Publish** pattern.

## Stack

- **Orchestration:** Apache Airflow
- **Data Quality:** Great Expectations
- **Transformation:** dbt
- **Lineage:** OpenLineage + Marquez
- **Warehouse:** Snowflake / Redshift (Postgres for local dev)
- **CI/CD:** GitHub Actions

## Quick Start

```bash
# 1. Clone and configure
git clone <repo-url> && cd commodity-data-lakehouse
cp .env.example .env   # edit as needed

# 2. Start services
docker compose up -d

# 3. Access UIs
open http://localhost:8080   # Airflow (airflow/airflow)
open http://localhost:3000   # Marquez lineage

# 4. Trigger the pipeline
docker compose exec airflow-webserver airflow dags trigger commodity_etl

# 5. Run dbt locally
cd dbt && dbt deps && dbt run && dbt test

# 6. Tear down
docker compose down -v
```

## How It Works

1. **Extract** — yfinance commodity futures data pulled into `raw_commodities`
2. **Audit** — Great Expectations validates: not null, unique timestamps, numeric ranges
3. **Publish** — If validation passes, rows promoted to `published_commodities`. If it fails, pipeline stops and Slack alerts fire.
4. **Transform** — dbt models: `stg_commodities` → `int_volatility_metrics` (Bollinger Bands) → `fct_daily_snapshot`
5. **Lineage** — OpenLineage emits metadata to Marquez for full traceability

See [CLAUDE.md](CLAUDE.md) for detailed architecture, development guide, runbooks, and engineering standards.
