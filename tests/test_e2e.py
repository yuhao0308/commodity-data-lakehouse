from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import date
from pathlib import Path

import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

from loaders.warehouse_loader import publish_validated_rows
from validators.gx_validator import run_raw_table_checkpoint


def test_end_to_end_seed_validate_publish_dbt_and_marquez_health(
    db_engine: Engine,
    expected_symbols: tuple[str, ...],
    dbt_project_dir: Path,
    run_command,
) -> None:
    _assert_airflow_health()
    _assert_marquez_health()

    raw_row_count, min_trade_date, max_trade_date = _assert_raw_seeded(db_engine, expected_symbols)

    run_raw_table_checkpoint(
        start_date=min_trade_date,
        end_date=max_trade_date,
        symbols=expected_symbols,
    )

    published_rows = publish_validated_rows(
        start_date=min_trade_date,
        end_date=max_trade_date,
        symbols=expected_symbols,
    )
    assert published_rows == raw_row_count

    published_count = _count_rows_by_date_and_symbols(
        db_engine,
        relation="published_commodities",
        start_date=min_trade_date,
        end_date=max_trade_date,
        symbols=expected_symbols,
    )
    assert published_count == raw_row_count

    _run_dbt_pipeline(run_command=run_command, dbt_project_dir=dbt_project_dir)

    stg_schema, stg_kind = _find_relation(db_engine, "stg_commodities")
    assert stg_kind == "v"
    assert _count_rows(db_engine, stg_schema, "stg_commodities") > 0

    int_schema, int_kind = _find_relation(db_engine, "int_volatility_metrics")
    assert int_kind == "v"
    assert _count_rows(db_engine, int_schema, "int_volatility_metrics") > 0

    fct_schema, fct_kind = _find_relation(db_engine, "fct_daily_snapshot")
    assert fct_kind == "r"
    assert _count_rows(db_engine, fct_schema, "fct_daily_snapshot") > 0

    columns = _get_columns(db_engine, fct_schema, "fct_daily_snapshot")
    for required in ("sma_20", "volatility", "upper_band", "lower_band", "percent_b"):
        assert required in columns


def _assert_airflow_health() -> None:
    airflow_url = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")
    response = requests.get(f"{airflow_url.rstrip('/')}/health", timeout=15)
    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, dict)
    assert "metadatabase" in payload


def _assert_marquez_health() -> None:
    marquez_url = os.getenv("MARQUEZ_URL", os.getenv("OPENLINEAGE_URL", "http://localhost:5000"))
    response = requests.get(f"{marquez_url.rstrip('/')}/api/v1/namespaces", timeout=15)
    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, dict)


def _assert_raw_seeded(db_engine: Engine, expected_symbols: tuple[str, ...]) -> tuple[int, date, date]:
    with db_engine.connect() as conn:
        symbol_rows = conn.execute(
            text(
                """
                select symbol, count(*)
                from raw_commodities
                group by symbol
                """
            )
        ).all()
        assert symbol_rows, "raw_commodities is empty; seed step likely failed."
        symbol_counts = {row[0]: int(row[1]) for row in symbol_rows}
        for symbol in expected_symbols:
            assert symbol in symbol_counts
            assert symbol_counts[symbol] > 0

        totals = conn.execute(
            text("select count(*), min(trade_date), max(trade_date) from raw_commodities")
        ).one()
        total_rows = int(totals[0])
        min_trade_date = totals[1]
        max_trade_date = totals[2]

    assert total_rows > 0
    assert isinstance(min_trade_date, date)
    assert isinstance(max_trade_date, date)
    return total_rows, min_trade_date, max_trade_date


def _count_rows_by_date_and_symbols(
    db_engine: Engine,
    *,
    relation: str,
    start_date: date,
    end_date: date,
    symbols: tuple[str, ...],
) -> int:
    in_clause = ", ".join(f":symbol_{index}" for index, _ in enumerate(symbols))
    params = {
        "start_date": start_date,
        "end_date": end_date,
    }
    params.update({f"symbol_{index}": symbol for index, symbol in enumerate(symbols)})

    sql = text(
        f"""
        select count(*)
        from {relation}
        where trade_date between :start_date and :end_date
          and symbol in ({in_clause})
        """
    )
    with db_engine.connect() as conn:
        return int(conn.execute(sql, params).scalar_one())


def _run_dbt_pipeline(*, run_command, dbt_project_dir: Path) -> None:
    target = os.getenv("DBT_TARGET", "dev")
    profile_name = os.getenv("DBT_PROFILE", "commodity_lakehouse")
    model_select = os.getenv("DBT_SELECT", "stg_commodities+")

    with tempfile.TemporaryDirectory(prefix="dbt_profiles_") as profiles_dir:
        profiles_path = Path(profiles_dir) / "profiles.yml"
        profiles_path.write_text(
            _build_runtime_profile(profile_name=profile_name, target=target),
            encoding="utf-8",
        )

        common_args = [
            "--project-dir",
            str(dbt_project_dir),
            "--profiles-dir",
            profiles_dir,
            "--profile",
            profile_name,
            "--target",
            target,
        ]

        run_command(
            [sys.executable, "-m", "dbt", "deps", *common_args],
            cwd=dbt_project_dir,
        )
        run_command(
            [sys.executable, "-m", "dbt", "run", "--select", model_select, *common_args],
            cwd=dbt_project_dir,
        )
        run_command(
            [sys.executable, "-m", "dbt", "test", "--select", model_select, *common_args],
            cwd=dbt_project_dir,
        )


def _build_runtime_profile(*, profile_name: str, target: str) -> str:
    host = os.getenv("WAREHOUSE_HOST", "postgres")
    port = int(os.getenv("WAREHOUSE_PORT", "5432"))
    user = os.getenv("WAREHOUSE_USER", "loader")
    password = os.getenv("WAREHOUSE_PASSWORD", "loader")
    db_name = os.getenv("WAREHOUSE_DB", "commodity_lakehouse")
    schema = os.getenv("DBT_SCHEMA", "public")

    return (
        f"{profile_name}:\n"
        f"  target: {target}\n"
        "  outputs:\n"
        f"    {target}:\n"
        "      type: postgres\n"
        f"      host: {json.dumps(host)}\n"
        f"      port: {port}\n"
        f"      user: {json.dumps(user)}\n"
        f"      password: {json.dumps(password)}\n"
        f"      dbname: {json.dumps(db_name)}\n"
        f"      schema: {json.dumps(schema)}\n"
        "      threads: 4\n"
    )


def _find_relation(db_engine: Engine, relation_name: str) -> tuple[str, str]:
    with db_engine.connect() as conn:
        row = conn.execute(
            text(
                """
                select n.nspname as schema_name, c.relkind
                from pg_class c
                join pg_namespace n on n.oid = c.relnamespace
                where c.relname = :relation_name
                  and c.relkind in ('r', 'v', 'm')
                order by
                  case when n.nspname like 'public%' then 0 else 1 end,
                  n.nspname
                limit 1
                """
            ),
            {"relation_name": relation_name},
        ).mappings().first()

    assert row is not None, f"Relation '{relation_name}' was not found."
    return str(row["schema_name"]), str(row["relkind"])


def _count_rows(db_engine: Engine, schema_name: str, relation_name: str) -> int:
    sql = text(f'select count(*) from "{schema_name}"."{relation_name}"')
    with db_engine.connect() as conn:
        return int(conn.execute(sql).scalar_one())


def _get_columns(db_engine: Engine, schema_name: str, relation_name: str) -> set[str]:
    with db_engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                select column_name
                from information_schema.columns
                where table_schema = :schema_name
                  and table_name = :relation_name
                """
            ),
            {"schema_name": schema_name, "relation_name": relation_name},
        ).all()

    return {str(row[0]) for row in rows}
