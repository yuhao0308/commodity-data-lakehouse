from __future__ import annotations

import logging
import os
from typing import Any

import requests

LOGGER = logging.getLogger(__name__)


def send_slack_failure_alert(context: dict[str, Any]) -> None:
    """Post a concise failure message to Slack via incoming webhook."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url or "replace/me" in webhook_url:
        LOGGER.warning("Skipping Slack alert because SLACK_WEBHOOK_URL is not configured.")
        return

    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown_task"
    run_id = context.get("run_id") or (
        context.get("dag_run").run_id if context.get("dag_run") else "unknown_run"
    )
    logical_date = context.get("logical_date")
    exception = context.get("exception")
    log_url = task_instance.log_url if task_instance else ""

    text = (
        f":red_circle: *Airflow task failed*\n"
        f"*DAG*: `{dag_id}`\n"
        f"*Task*: `{task_id}`\n"
        f"*Run ID*: `{run_id}`\n"
        f"*Logical Date*: `{logical_date}`\n"
        f"*Exception*: `{exception}`\n"
        f"*Logs*: {log_url}"
    )

    try:
        response = requests.post(webhook_url, json={"text": text}, timeout=10)
        response.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to send Slack failure alert: %s", exc)
