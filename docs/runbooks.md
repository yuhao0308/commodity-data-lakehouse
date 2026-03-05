# Runbooks

Operational response guide for common failure scenarios.

## Failure Scenarios

| Incident | Detection | Action |
|---|---|---|
| **GX checkpoint fails** | Airflow task fails, Slack alert | Inspect GX data docs report. Check raw data for anomalies. Fix expectation or data source. Re-trigger DAG. |
| **yfinance schema change** | GX fails on unexpected/missing columns | Open Marquez to trace downstream impact. Patch extractor in `airflow/include/`. Update GX suite. Update dbt staging model if needed. |
| **dbt test fails** | dbt task fails, Slack alert | Check `dbt/target/run_results.json`. Identify failing test. Fix transform logic or update test threshold. Never disable a test without a PR review. |
| **Warehouse load error** | Airflow task fails with DB error | Check connection env vars. Verify warehouse is reachable. All loads must be idempotent (upsert or delete+insert). Safe to retry. |
| **Airflow scheduler down** | No DAG runs, health check fails | `docker compose restart airflow-scheduler`. Check logs at `docker compose logs airflow-scheduler`. |
| **Marquez unreachable** | Lineage events fail (non-blocking) | Lineage emission is fire-and-forget. Pipeline continues. Restart Marquez: `docker compose restart marquez`. Backfill lineage from Airflow logs if needed. |

## Quick Commands

```bash
# start the local stack
scripts/bootstrap.sh

# seed deterministic test data
python3 scripts/seed_sample_data.py

# run GX checkpoint locally
scripts/run_gx_checkpoint.sh raw_validation
```
