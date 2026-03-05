# Design Decisions (MVP)

This document captures the key architecture decisions for Project Gamma's MVP.

## ADR-001: Write-Audit-Publish data contract
- Status: Accepted
- Decision: Pipeline stages are `write` -> `audit` -> `publish`.
- Why:
  - Raw data stays untrusted until validation passes.
  - Bad data is prevented from entering analytics models.
- Consequence:
  - dbt models read from `published_commodities`, not `raw_commodities`.

## ADR-002: Raw storage in Postgres first
- Status: Accepted
- Decision: Store extracted rows in `raw_commodities` (JSONB payload included).
- Why:
  - Fastest path for CI/local development.
  - Easy to inspect/query with SQL.
- Consequence:
  - File-based lake storage can be added later without changing quality gates.

## ADR-003: Great Expectations validates database tables
- Status: Accepted
- Decision: GX checkpoint `raw_validation` runs against Postgres table `raw_commodities`.
- Why:
  - No file management needed in CI.
  - Same validation path for local and automated runs.
- Consequence:
  - GX datasource is SQLAlchemy-based.

## ADR-004: Per-symbol extraction tasks
- Status: Accepted
- Decision: Airflow uses one extract task per symbol (`oil`, `gold`, `wheat`) with fan-in.
- Why:
  - Better retry and fault isolation.
  - Better task-level lineage visibility.
- Consequence:
  - Slightly more DAG/task orchestration complexity.

## ADR-005: dbt triggered via subprocess in ETL DAG
- Status: Accepted (MVP)
- Decision: Airflow task `trigger_dbt` calls `dbt run --select stg_commodities+`.
- Why:
  - Minimal setup and operationally simple for MVP.
- Consequence:
  - Model-level observability in Airflow is limited compared with Cosmos.

## ADR-006: Materialization and incremental strategy
- Status: Accepted (MVP)
- Decision:
  - `staging` models: `view`
  - `intermediate` models: `view`
  - `marts` models: `table`
  - Volatility model remains full-refresh.
- Why:
  - Keeps complexity low while row counts are small.
- Consequence:
  - Incremental strategy with rolling lookback remains a V1 optimization.
