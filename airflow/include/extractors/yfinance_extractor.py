from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Any, Mapping

import pandas as pd
import yfinance as yf

DEFAULT_COMMODITY_SYMBOLS: dict[str, str] = {
    "oil": "CL=F",
    "gold": "GC=F",
    "wheat": "ZW=F",
}


def resolve_extract_window(
    *,
    data_interval_start: datetime | None,
    data_interval_end: datetime | None,
    dag_run_conf: Mapping[str, Any] | None = None,
) -> tuple[date, date]:
    """Resolve an inclusive [start_date, end_date] window for extraction."""
    today = datetime.utcnow().date()
    default_day = today - timedelta(days=1)
    start_date = default_day
    end_date = default_day

    if data_interval_start is not None:
        start_date = data_interval_start.date()
    if data_interval_end is not None:
        end_date = (data_interval_end - timedelta(days=1)).date()

    conf = dag_run_conf or {}
    conf_start = _parse_iso_date(conf.get("start_date"), "start_date")
    conf_end = _parse_iso_date(conf.get("end_date"), "end_date")

    if conf_start is not None:
        start_date = conf_start
    if conf_end is not None:
        end_date = conf_end
    if conf_start is not None and conf_end is None:
        end_date = conf_start
    if conf_end is not None and conf_start is None:
        start_date = conf_end

    if end_date < start_date:
        raise ValueError(
            f"Invalid extraction window: end_date={end_date.isoformat()} is before "
            f"start_date={start_date.isoformat()}"
        )

    return start_date, end_date


def fetch_symbol_history(symbol: str, start_date: date, end_date: date) -> list[dict[str, Any]]:
    """Fetch daily OHLCV rows from yfinance and normalize records."""
    history = yf.download(
        tickers=symbol,
        start=start_date.isoformat(),
        end=(end_date + timedelta(days=1)).isoformat(),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    if history.empty:
        return []

    if isinstance(history.columns, pd.MultiIndex):
        history.columns = history.columns.get_level_values(0)

    history = history.reset_index()
    date_col = "Date" if "Date" in history.columns else "Datetime"
    history = history.rename(
        columns={
            date_col: "trade_date",
            "Open": "open_price",
            "High": "high_price",
            "Low": "low_price",
            "Close": "close_price",
            "Volume": "volume",
        }
    )

    records: list[dict[str, Any]] = []
    for row in history.to_dict(orient="records"):
        trade_date = _parse_trade_date(row.get("trade_date"))
        if trade_date is None:
            continue

        open_price = _as_optional_float(row.get("open_price"))
        high_price = _as_optional_float(row.get("high_price"))
        low_price = _as_optional_float(row.get("low_price"))
        close_price = _as_optional_float(row.get("close_price"))
        volume = _as_optional_int(row.get("volume"))

        raw_payload = {
            "symbol": symbol,
            "trade_date": trade_date.isoformat(),
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": volume,
        }

        records.append(
            {
                "symbol": symbol,
                "trade_date": trade_date,
                "open_price": open_price,
                "high_price": high_price,
                "low_price": low_price,
                "close_price": close_price,
                "volume": volume,
                "source": "yfinance",
                "raw_payload": raw_payload,
            }
        )

    return records


def _parse_iso_date(raw_value: Any, field_name: str) -> date | None:
    if raw_value is None:
        return None
    if isinstance(raw_value, datetime):
        return raw_value.date()
    if isinstance(raw_value, date):
        return raw_value
    if isinstance(raw_value, str):
        try:
            return date.fromisoformat(raw_value.strip())
        except ValueError as exc:
            raise ValueError(f"Invalid {field_name}: {raw_value}") from exc
    raise TypeError(f"{field_name} must be a date or ISO date string, got {type(raw_value)!r}")


def _parse_trade_date(raw_value: Any) -> date | None:
    if raw_value is None:
        return None
    if isinstance(raw_value, datetime):
        return raw_value.date()
    if isinstance(raw_value, date):
        return raw_value
    if isinstance(raw_value, pd.Timestamp):
        return raw_value.date()
    return None


def _as_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return float(value)


def _as_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return int(value)
