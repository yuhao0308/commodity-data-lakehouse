from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
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
        published_rows = load_summary.get("published_rows", 0)
        if published_rows == 0:
            LOGGER.info("No rows published — skipping dbt run.")
            return {"status": "skipped", "reason": "no rows to transform"}

        run_result = _run_dbt_models()
        LOGGER.info(
            "dbt run completed. command=%s target=%s project_dir=%s",
            run_result["command"],
            run_result["target"],
            run_result["project_dir"],
        )
        return {"status": "completed", **run_result}

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
    configured_dir = os.getenv("DBT_PROJECT_DIR")
    if configured_dir:
        configured_path = Path(configured_dir)
        if configured_path.exists():
            return configured_path

    airflow_default = Path("/opt/airflow/dbt")
    if airflow_default.exists():
        return airflow_default

    local_dir = CURRENT_DIR.parents[1] / "dbt"
    if local_dir.exists():
        return local_dir

    raise AirflowException("Could not find dbt project directory. Set DBT_PROJECT_DIR.")


def _run_dbt_models() -> dict[str, str]:
    dbt_project_dir = _resolve_dbt_project_dir()
    dbt_target = os.getenv("DBT_TARGET", "dev")
    dbt_select = os.getenv("DBT_SELECT", "stg_commodities+")
    profile_name = os.getenv("DBT_PROFILE", "commodity_lakehouse")

    with tempfile.TemporaryDirectory(prefix="dbt_profiles_") as profiles_dir:
        profiles_path = Path(profiles_dir) / "profiles.yml"
        profiles_path.write_text(
            _build_runtime_dbt_profile(profile_name=profile_name, target_name=dbt_target),
            encoding="utf-8",
        )

        command = [
            sys.executable,
            "-m",
            "dbt",
            "run",
            "--select",
            dbt_select,
            "--target",
            dbt_target,
            "--project-dir",
            str(dbt_project_dir),
            "--profiles-dir",
            profiles_dir,
            "--profile",
            profile_name,
        ]
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            cwd=str(dbt_project_dir),
        )
        if result.returncode != 0:
            output = (result.stderr or result.stdout).strip()
            raise AirflowException(f"dbt run failed for selection '{dbt_select}': {output}")

    return {
        "command": " ".join(command),
        "target": dbt_target,
        "project_dir": str(dbt_project_dir),
    }


def _build_runtime_dbt_profile(*, profile_name: str, target_name: str) -> str:
    host = os.getenv("WAREHOUSE_HOST", "postgres")
    port = int(os.getenv("WAREHOUSE_PORT", "5432"))
    user = os.getenv("WAREHOUSE_USER", "loader")
    password = os.getenv("WAREHOUSE_PASSWORD", "loader")
    db_name = os.getenv("WAREHOUSE_DB", "commodity_lakehouse")
    schema = os.getenv("DBT_SCHEMA", "public")

    # json.dumps keeps values safely quoted for YAML.
    return (
        f"{profile_name}:\n"
        f"  target: {target_name}\n"
        "  outputs:\n"
        f"    {target_name}:\n"
        "      type: postgres\n"
        f"      host: {json.dumps(host)}\n"
        f"      port: {port}\n"
        f"      user: {json.dumps(user)}\n"
        f"      password: {json.dumps(password)}\n"
        f"      dbname: {json.dumps(db_name)}\n"
        f"      schema: {json.dumps(schema)}\n"
        "      threads: 4\n"
    )


commodity_etl = commodity_etl_dag()
