#!/usr/bin/env python3
"""Generate deterministic synthetic commodity data for CI and local testing."""

from __future__ import annotations

import argparse
import logging
import os
import random
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
AIRFLOW_INCLUDE_DIR = REPO_ROOT / "airflow" / "include"
if str(AIRFLOW_INCLUDE_DIR) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_INCLUDE_DIR))

LOGGER = logging.getLogger(__name__)

DEFAULT_SYMBOLS: tuple[str, ...] = ("CL=F", "GC=F", "ZW=F")
DEFAULT_END_DATE = date(2025, 1, 31)
DEFAULT_DAYS = 45
DEFAULT_SEED = 42

PRICE_BASELINES: dict[str, float] = {
    "CL=F": 76.0,   # crude oil futures
    "GC=F": 2040.0, # gold futures
    "ZW=F": 595.0,  # wheat futures
}
DAILY_VOLATILITY: dict[str, float] = {
    "CL=F": 0.025,
    "GC=F": 0.011,
    "ZW=F": 0.019,
}
VOLUME_BASELINES: dict[str, int] = {
    "CL=F": 360_000,
    "GC=F": 220_000,
    "ZW=F": 130_000,
}


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    symbols = parse_symbols(args.symbols)
    if args.days < 20:
        raise ValueError("--days must be at least 20 so Bollinger window models are populated.")

    end_date = resolve_end_date(args.end_date)
    trading_days = recent_trading_days(end_date=end_date, count=args.days)
    start_date = trading_days[0]
    final_date = trading_days[-1]

    LOGGER.info(
        "Seeding synthetic data for symbols=%s window=%s..%s days=%s seed=%s",
        ",".join(symbols),
        start_date.isoformat(),
        final_date.isoformat(),
        len(trading_days),
        args.seed,
    )

    all_records: dict[str, list[dict[str, object]]] = {}
    for index, symbol in enumerate(symbols):
        symbol_rng = random.Random(args.seed + (index * 9973))
        all_records[symbol] = generate_symbol_rows(symbol=symbol, days=trading_days, rng=symbol_rng)

    if args.dry_run:
        total_rows = sum(len(rows) for rows in all_records.values())
        LOGGER.info("Dry run only. Generated %s rows (not written).", total_rows)
        sample_symbol = symbols[0]
        LOGGER.info("Sample row (%s): %s", sample_symbol, all_records[sample_symbol][0])
        return 0

    load_raw_rows, publish_validated_rows = get_loader_functions()

    total_loaded = 0
    for symbol in symbols:
        rows = all_records[symbol]
        inserted_rows = load_raw_rows(
            symbol=symbol,
            records=rows,
            start_date=start_date,
            end_date=final_date,
        )
        total_loaded += inserted_rows
        LOGGER.info("Inserted %s raw rows for %s.", inserted_rows, symbol)

    if args.raw_only:
        LOGGER.info("Skipping publish step because --raw-only was set.")
    else:
        published_rows = publish_validated_rows(
            start_date=start_date,
            end_date=final_date,
            symbols=symbols,
        )
        LOGGER.info("Published %s rows to published_commodities.", published_rows)
    LOGGER.info("Seeding complete. Raw rows inserted: %s", total_loaded)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate deterministic synthetic commodity futures rows and load them."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of recent trading days to generate (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Random seed for deterministic output (default: {DEFAULT_SEED}).",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Comma-separated symbols. Defaults to COMMODITY_SYMBOLS or CL=F,GC=F,ZW=F.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help=(
            "Inclusive end date (YYYY-MM-DD). Defaults to SEED_END_DATE if set, "
            f"otherwise {DEFAULT_END_DATE.isoformat()}."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate rows and print summary without writing to the warehouse.",
    )
    parser.add_argument(
        "--raw-only",
        action="store_true",
        help="Load only raw_commodities and skip publish promotion.",
    )
    return parser.parse_args()


def get_loader_functions():
    from loaders.warehouse_loader import load_raw_rows, publish_validated_rows

    return load_raw_rows, publish_validated_rows


def parse_symbols(raw_symbols: str | None) -> list[str]:
    raw = raw_symbols or os.getenv("COMMODITY_SYMBOLS", "")
    parsed = [value.strip() for value in raw.split(",") if value.strip()]
    return parsed or list(DEFAULT_SYMBOLS)


def resolve_end_date(raw_end_date: str | None) -> date:
    value = raw_end_date or os.getenv("SEED_END_DATE")
    if value:
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"Invalid end date '{value}'. Expected YYYY-MM-DD.") from exc
    return DEFAULT_END_DATE


def recent_trading_days(*, end_date: date, count: int) -> list[date]:
    if count < 1:
        raise ValueError("count must be >= 1")

    days: list[date] = []
    current = end_date
    while len(days) < count:
        if current.weekday() < 5:
            days.append(current)
        current -= timedelta(days=1)
    days.reverse()
    return days


def generate_symbol_rows(
    *,
    symbol: str,
    days: Sequence[date],
    rng: random.Random,
) -> list[dict[str, object]]:
    base_price = PRICE_BASELINES.get(symbol, 100.0)
    volatility = DAILY_VOLATILITY.get(symbol, 0.02)
    base_volume = VOLUME_BASELINES.get(symbol, 100_000)

    previous_close = base_price
    rows: list[dict[str, object]] = []
    for trade_day in days:
        daily_return = rng.gauss(0.0004, volatility)
        open_price = max(0.01, previous_close * (1 + rng.gauss(0.0, volatility / 2)))
        close_price = max(0.01, previous_close * (1 + daily_return))

        span = abs(rng.gauss(volatility * 0.7, volatility * 0.2))
        high_price = max(open_price, close_price) * (1 + span)
        low_price = min(open_price, close_price) * (1 - span)
        low_price = max(0.01, low_price)
        high_price = max(high_price, low_price)

        volume = int(max(1, base_volume * (1 + rng.uniform(-0.30, 0.30))))

        row = {
            "symbol": symbol,
            "trade_date": trade_day,
            "open_price": round(open_price, 4),
            "high_price": round(high_price, 4),
            "low_price": round(low_price, 4),
            "close_price": round(close_price, 4),
            "volume": volume,
            "source": "seed_sample_data",
            "raw_payload": {
                "symbol": symbol,
                "trade_date": trade_day.isoformat(),
                "open": round(open_price, 4),
                "high": round(high_price, 4),
                "low": round(low_price, 4),
                "close": round(close_price, 4),
                "volume": volume,
                "generated_at": f"{trade_day.isoformat()}T00:00:00Z",
            },
        }
        rows.append(row)
        previous_close = close_price

    return rows


if __name__ == "__main__":
    raise SystemExit(main())
