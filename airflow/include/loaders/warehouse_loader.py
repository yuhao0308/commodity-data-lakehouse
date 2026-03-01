from __future__ import annotations

import json
import os
from datetime import date
from functools import lru_cache
from typing import Any, Sequence

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Connection, Engine


def load_raw_rows(
    *,
    symbol: str,
    records: Sequence[dict[str, Any]],
    start_date: date,
    end_date: date,
    warehouse_type: str | None = None,
) -> int:
    """Idempotently replace raw rows for a symbol/date window."""
    engine = get_warehouse_engine(warehouse_type=warehouse_type)

    with engine.begin() as conn:
        ensure_raw_table(conn)
        conn.execute(
            text(
                """
                delete from raw_commodities
                where symbol = :symbol
                  and trade_date between :start_date and :end_date
                """
            ),
            {
                "symbol": symbol,
                "start_date": start_date,
                "end_date": end_date,
            },
        )

        if not records:
            return 0

        prepared_rows = [
            {
                "symbol": row["symbol"],
                "trade_date": row["trade_date"],
                "open_price": row.get("open_price"),
                "high_price": row.get("high_price"),
                "low_price": row.get("low_price"),
                "close_price": row.get("close_price"),
                "volume": row.get("volume"),
                "source": row.get("source", "yfinance"),
                "raw_payload": json.dumps(row.get("raw_payload", {})),
            }
            for row in records
        ]

        conn.execute(
            text(
                """
                insert into raw_commodities (
                    symbol,
                    trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    source,
                    raw_payload,
                    ingested_at
                ) values (
                    :symbol,
                    :trade_date,
                    :open_price,
                    :high_price,
                    :low_price,
                    :close_price,
                    :volume,
                    :source,
                    cast(:raw_payload as jsonb),
                    now()
                )
                """
            ),
            prepared_rows,
        )

    return len(prepared_rows)


def publish_validated_rows(
    *,
    start_date: date,
    end_date: date,
    symbols: Sequence[str] | None = None,
    warehouse_type: str | None = None,
) -> int:
    """Promote validated raw rows to published_commodities."""
    engine = get_warehouse_engine(warehouse_type=warehouse_type)
    symbol_list = list(symbols or [])

    with engine.begin() as conn:
        ensure_raw_table(conn)
        ensure_published_table(conn)

        if symbol_list:
            count_stmt = text(
                """
                select count(*)
                from raw_commodities
                where trade_date between :start_date and :end_date
                  and symbol in :symbols
                """
            ).bindparams(bindparam("symbols", expanding=True))

            delete_stmt = text(
                """
                delete from published_commodities
                where trade_date between :start_date and :end_date
                  and symbol in :symbols
                """
            ).bindparams(bindparam("symbols", expanding=True))

            insert_stmt = text(
                """
                insert into published_commodities (
                    symbol,
                    trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    source,
                    published_at
                )
                select
                    symbol,
                    trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    source,
                    now()
                from raw_commodities
                where trade_date between :start_date and :end_date
                  and symbol in :symbols
                """
            ).bindparams(bindparam("symbols", expanding=True))

            params = {
                "start_date": start_date,
                "end_date": end_date,
                "symbols": symbol_list,
            }
            rows_to_publish = int(conn.execute(count_stmt, params).scalar_one())
            conn.execute(delete_stmt, params)
            conn.execute(insert_stmt, params)
            return rows_to_publish

        rows_to_publish = int(
            conn.execute(
                text(
                    """
                    select count(*)
                    from raw_commodities
                    where trade_date between :start_date and :end_date
                    """
                ),
                {"start_date": start_date, "end_date": end_date},
            ).scalar_one()
        )

        conn.execute(
            text(
                """
                delete from published_commodities
                where trade_date between :start_date and :end_date
                """
            ),
            {"start_date": start_date, "end_date": end_date},
        )
        conn.execute(
            text(
                """
                insert into published_commodities (
                    symbol,
                    trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    source,
                    published_at
                )
                select
                    symbol,
                    trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    source,
                    now()
                from raw_commodities
                where trade_date between :start_date and :end_date
                """
            ),
            {"start_date": start_date, "end_date": end_date},
        )

    return rows_to_publish


@lru_cache(maxsize=3)
def get_warehouse_engine(*, warehouse_type: str | None = None) -> Engine:
    resolved_warehouse = (warehouse_type or os.getenv("WAREHOUSE_TYPE", "postgres")).lower()
    if resolved_warehouse != "postgres":
        if resolved_warehouse == "snowflake":
            raise NotImplementedError("Snowflake loading is not implemented yet.")
        raise ValueError(f"Unsupported warehouse type: {resolved_warehouse}")

    return create_engine(_postgres_connection_url(), future=True, pool_pre_ping=True)


def ensure_raw_table(conn: Connection) -> None:
    conn.execute(
        text(
            """
            create table if not exists raw_commodities (
                symbol text not null,
                trade_date date not null,
                open_price double precision,
                high_price double precision,
                low_price double precision,
                close_price double precision,
                volume bigint,
                source text not null default 'yfinance',
                raw_payload jsonb not null,
                ingested_at timestamptz not null default now(),
                primary key (symbol, trade_date)
            )
            """
        )
    )


def ensure_published_table(conn: Connection) -> None:
    conn.execute(
        text(
            """
            create table if not exists published_commodities (
                symbol text not null,
                trade_date date not null,
                open_price double precision,
                high_price double precision,
                low_price double precision,
                close_price double precision,
                volume bigint,
                source text not null default 'yfinance',
                published_at timestamptz not null default now(),
                primary key (symbol, trade_date)
            )
            """
        )
    )


def _postgres_connection_url() -> str:
    explicit_url = os.getenv("WAREHOUSE_SQLALCHEMY_URL")
    if explicit_url:
        return explicit_url

    host = os.getenv("WAREHOUSE_HOST", "localhost")
    port = os.getenv("WAREHOUSE_PORT", "5432")
    db_name = os.getenv("WAREHOUSE_DB", "commodity_lakehouse")
    user = os.getenv("WAREHOUSE_USER", "loader")
    password = os.getenv("WAREHOUSE_PASSWORD", "loader")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
