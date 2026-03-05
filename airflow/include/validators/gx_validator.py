from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path
from typing import Sequence

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

LOGGER = logging.getLogger(__name__)


def run_raw_table_checkpoint(
    *,
    start_date: date,
    end_date: date,
    symbols: Sequence[str],
    checkpoint_name: str = "raw_validation",
) -> None:
    """Run Great Expectations validation against a date/symbol-scoped slice of raw_commodities.

    Uses the programmatic API (not CLI) so we can:
    1. Scope the batch to only the rows just extracted (by date range + symbols).
    2. Dynamically inject the allowed symbol set instead of hardcoding it in the suite.
    """
    gx_root = _resolve_gx_root()
    context = gx.get_context(context_root_dir=str(gx_root))

    # Build a SQL query scoped to the current extraction window
    query = _build_scoped_query(start_date=start_date, end_date=end_date, symbols=symbols)
    LOGGER.info(
        "Running GX validation scoped to %s - %s for symbols %s",
        start_date.isoformat(),
        end_date.isoformat(),
        symbols,
    )

    batch_request = RuntimeBatchRequest(
        datasource_name="warehouse_postgres",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="raw_commodities_scoped",
        runtime_parameters={"query": query},
        batch_identifiers={
            "default_identifier_name": f"validation_{start_date}_{end_date}",
        },
    )

    # Load the static suite (not_null, unique, numeric range checks)
    suite = context.get_expectation_suite("commodities_raw")

    # Get a validator for the scoped batch
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite,
    )

    # Dynamically add the symbol-in-set expectation using actual configured symbols
    validator.expect_column_values_to_be_in_set(
        column="symbol",
        value_set=list(symbols),
        meta={"notes": "Dynamically injected from COMMODITY_SYMBOLS"},
    )

    # Run all expectations
    results = validator.validate(run_name=f"{start_date}_{end_date}_raw_validation")

    if not results.success:
        failed = [
            r.expectation_config.expectation_type
            for r in results.results
            if not r.success
        ]
        raise RuntimeError(
            f"Great Expectations validation failed. "
            f"Failed expectations: {failed}. "
            f"See data docs for full report."
        )

    LOGGER.info(
        "GX validation passed: %d expectations evaluated, all succeeded.",
        len(results.results),
    )


def _build_scoped_query(
    *,
    start_date: date,
    end_date: date,
    symbols: Sequence[str],
) -> str:
    """Build a SQL query that scopes validation to the current extraction window.

    Symbols come from our own COMMODITY_SYMBOLS env var / DAG params (not user input),
    so inline formatting is acceptable here.
    """
    symbols_sql = ", ".join(f"'{_escape_sql_literal(s)}'" for s in symbols)
    return (
        f"select * from raw_commodities "
        f"where trade_date between '{start_date.isoformat()}' and '{end_date.isoformat()}' "
        f"and symbol in ({symbols_sql})"
    )


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve_gx_root() -> Path:
    configured_home = os.getenv("GREAT_EXPECTATIONS_HOME")
    if configured_home:
        return Path(configured_home)

    airflow_default = Path("/opt/airflow/great_expectations")
    if airflow_default.exists():
        return airflow_default

    return _project_root() / "great_expectations"
