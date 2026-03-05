from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Sequence

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

REPO_ROOT = Path(__file__).resolve().parents[1]
AIRFLOW_INCLUDE_DIR = REPO_ROOT / "airflow" / "include"
if str(AIRFLOW_INCLUDE_DIR) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_INCLUDE_DIR))


@pytest.fixture(scope="session")
def expected_symbols() -> tuple[str, ...]:
    raw = os.getenv("COMMODITY_SYMBOLS", "CL=F,GC=F,ZW=F")
    parsed = tuple(item.strip() for item in raw.split(",") if item.strip())
    return parsed or ("CL=F", "GC=F", "ZW=F")


@pytest.fixture(scope="session")
def warehouse_sqlalchemy_url() -> str:
    return os.getenv(
        "WAREHOUSE_SQLALCHEMY_URL",
        "postgresql+psycopg2://loader:loader@localhost:5432/commodity_lakehouse",
    )


@pytest.fixture(scope="session")
def db_engine(warehouse_sqlalchemy_url: str) -> Engine:
    engine = create_engine(warehouse_sqlalchemy_url, future=True, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("select 1"))
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def dbt_project_dir() -> Path:
    configured = os.getenv("DBT_PROJECT_DIR")
    if configured:
        configured_path = Path(configured)
        if configured_path.exists():
            return configured_path

    airflow_default = Path("/opt/airflow/dbt")
    if airflow_default.exists():
        return airflow_default

    local_path = REPO_ROOT / "dbt"
    if local_path.exists():
        return local_path

    raise RuntimeError("Could not resolve dbt project directory.")


@pytest.fixture(scope="session", autouse=True)
def ensure_gx_home() -> None:
    os.environ.setdefault("GREAT_EXPECTATIONS_HOME", str(REPO_ROOT / "great_expectations"))


@pytest.fixture
def run_command():
    def _run(
        command: Sequence[str],
        *,
        cwd: str | Path | None = None,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            cwd=str(cwd) if cwd else None,
            env=env,
        )
        if result.returncode != 0:
            raise AssertionError(
                "Command failed:\n"
                f"cmd: {' '.join(command)}\n"
                f"exit: {result.returncode}\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )
        return result

    return _run
