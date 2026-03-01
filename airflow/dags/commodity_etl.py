from __future__ import annotations

import logging
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context

CURRENT_DIR = Path(__file__).resolve().parent
INCLUDE_DIR = CURRENT_DIR.parent / "include"
if str(INCLUDE_DIR) not in sys.path:
    sys.path.insert(0, str(INCLUDE_DIR))

from alerts.slack_alert import send_slack_failure_alert
from extractors.yfinance_extractor import (
    DEFAULT_COMMODITY_SYMBOLS,
    fetch_symbol_history,
    resolve_extract_window,
)
from loaders.warehouse_loader import load_raw_rows, publish_validated_rows
from validators.gx_validator import run_raw_table_checkpoint

LOGGER = logging.getLogger(__name__)


@dag(
    dag_id="commodity_etl",
    description="Write-Audit-Publish DAG for commodity futures data",
    schedule="0 6 * * 1-5",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    on_failure_callback=send_slack_failure_alert,
    tags=["commodities", "etl", "write-audit-publish"],
)
def commodity_etl_dag():
    symbol_map = _resolve_symbol_map()

    @task(task_id="extract_oil")
    def extract_oil() -> dict[str, Any]:
        return _extract_symbol("oil", symbol_map["oil"])

    @task(task_id="extract_gold")
    def extract_gold() -> dict[str, Any]:
        return _extract_symbol("gold", symbol_map["gold"])

    @task(task_id="extract_wheat")
    def extract_wheat() -> dict[str, Any]:
        return _extract_symbol("wheat", symbol_map["wheat"])

    @task(task_id="validate_raw")
    def validate_raw(extract_summaries: list[dict[str, Any]]) -> dict[str, Any]:
        if not extract_summaries:
            raise AirflowException("No extraction summaries were provided to validate_raw.")

        start_date, end_date = _extract_window_from_summaries(extract_summaries)
        symbols = [item["symbol"] for item in extract_summaries]

        run_raw_table_checkpoint(
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
        )

        return {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "symbols": symbols,
        }

    @task(task_id="load_to_warehouse")
    def load_to_warehouse(validation_summary: dict[str, Any]) -> dict[str, Any]:
        start_date = date.fromisoformat(validation_summary["start_date"])
        end_date = date.fromisoformat(validation_summary["end_date"])
        symbols = validation_summary.get("symbols", [])

        published_rows = publish_validated_rows(
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
        )
        return {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "symbols": symbols,
            "published_rows": published_rows,
        }

    @task(task_id="trigger_dbt")
    def trigger_dbt(load_summary: dict[str, Any]) -> dict[str, str]:
        import subprocess
        import sys

        published_rows = load_summary.get("published_rows", 0)
        if published_rows == 0:
            LOGGER.info("No rows published — skipping dbt run.")
            return {"status": "skipped", "reason": "no rows to transform"}

        dbt_project_dir = _resolve_dbt_project_dir()
        LOGGER.info("Running dbt from %s", dbt_project_dir)

        for cmd_name, cmd_args in [
            ("dbt deps", ["deps"]),
            ("dbt run", ["run", "--select", "stg_commodities+"]),
            ("dbt test", ["test", "--select", "stg_commodities+"]),
        ]:
            result = subprocess.run(
                [sys.executable, "-m", "dbt", *cmd_args, "--project-dir", str(dbt_project_dir)],
                check=False,
                capture_output=True,
                text=True,
                cwd=str(dbt_project_dir),
            )
            if result.returncode != 0:
                output = result.stderr.strip() or result.stdout.strip()
                raise AirflowException(f"{cmd_name} failed: {output}")
            LOGGER.info("%s succeeded.", cmd_name)

        return {"status": "completed"}

    extracted_oil = extract_oil()
    extracted_gold = extract_gold()
    extracted_wheat = extract_wheat()
    validated = validate_raw([extracted_oil, extracted_gold, extracted_wheat])
    loaded = load_to_warehouse(validated)
    trigger_dbt(loaded)


def _extract_symbol(commodity_name: str, symbol: str) -> dict[str, Any]:
    context = get_current_context()
    dag_run = context.get("dag_run")
    dag_run_conf = dag_run.conf if dag_run else {}

    start_date, end_date = resolve_extract_window(
        data_interval_start=context.get("data_interval_start"),
        data_interval_end=context.get("data_interval_end"),
        dag_run_conf=dag_run_conf,
    )

    rows = fetch_symbol_history(symbol=symbol, start_date=start_date, end_date=end_date)
    loaded_rows = load_raw_rows(
        symbol=symbol,
        records=rows,
        start_date=start_date,
        end_date=end_date,
    )

    LOGGER.info(
        "Extracted and staged %s rows for %s (%s) between %s and %s",
        loaded_rows,
        commodity_name,
        symbol,
        start_date.isoformat(),
        end_date.isoformat(),
    )
    return {
        "commodity": commodity_name,
        "symbol": symbol,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "loaded_rows": loaded_rows,
    }


def _extract_window_from_summaries(extract_summaries: list[dict[str, Any]]) -> tuple[date, date]:
    start_dates = [date.fromisoformat(item["start_date"]) for item in extract_summaries]
    end_dates = [date.fromisoformat(item["end_date"]) for item in extract_summaries]
    return min(start_dates), max(end_dates)


def _resolve_symbol_map() -> dict[str, str]:
    configured = os.getenv("COMMODITY_SYMBOLS", "")
    configured_symbols = [value.strip() for value in configured.split(",") if value.strip()]
    keys = list(DEFAULT_COMMODITY_SYMBOLS.keys())

    symbol_map = DEFAULT_COMMODITY_SYMBOLS.copy()
    for index, key in enumerate(keys):
        if index < len(configured_symbols):
            symbol_map[key] = configured_symbols[index]
    return symbol_map


def _resolve_dbt_project_dir() -> Path:
    """Resolve the dbt project directory."""
    airflow_default = Path("/opt/airflow/dbt")
    if airflow_default.exists():
        return airflow_default
    return CURRENT_DIR.parents[1] / "dbt"


commodity_etl = commodity_etl_dag()
