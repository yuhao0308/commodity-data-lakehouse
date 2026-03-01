from __future__ import annotations

import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Sequence


def run_raw_table_checkpoint(
    *,
    start_date: date,
    end_date: date,
    symbols: Sequence[str],
    checkpoint_name: str = "raw_validation",
) -> None:
    """Run a Great Expectations checkpoint against raw_commodities."""
    gx_root = _resolve_gx_root()
    project_root = gx_root.parent
    gx_config = gx_root / "great_expectations.yml"
    checkpoint_file = gx_root / "checkpoints" / f"{checkpoint_name}.yml"

    if not gx_config.exists():
        raise FileNotFoundError(f"Missing Great Expectations config: {gx_config}")
    if not checkpoint_file.exists():
        raise FileNotFoundError(f"Missing checkpoint config: {checkpoint_file}")

    env = os.environ.copy()
    env["GX_VALIDATION_START_DATE"] = start_date.isoformat()
    env["GX_VALIDATION_END_DATE"] = end_date.isoformat()
    env["GX_VALIDATION_SYMBOLS"] = ",".join(symbols)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "great_expectations",
            "checkpoint",
            "run",
            checkpoint_name,
            "--config",
            str(gx_config),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=env,
        cwd=str(project_root),
    )

    if result.returncode != 0:
        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        output = stderr or stdout or "No output captured."
        raise RuntimeError(
            f"Great Expectations checkpoint '{checkpoint_name}' failed. Output: {output}"
        )


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
