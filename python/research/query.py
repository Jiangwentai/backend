from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date, datetime
from pathlib import Path
import re

import duckdb
import pyarrow as pa

SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
SUPPORTED_INTERVALS = {
    "1m": "1 minute",
    "5m": "5 minutes",
    "1h": "1 hour",
    "1d": None,
}


def _validate_component(name: str, value: str) -> str:
    if not SAFE_COMPONENT.fullmatch(value):
        raise ValueError(f"invalid {name}")
    return value


def _completed_files(
    root: Path, exchange: str, instrument: str, start_day: str | None, end_day: str | None
) -> list[str]:
    exchange = _validate_component("exchange", exchange)
    instrument = _validate_component("instrument", instrument)
    base = root / "ctp" / f"exchange={exchange}" / f"instrument={instrument}"
    files: list[str] = []
    for success in sorted(base.glob("trading_day=*/_SUCCESS.json")):
        day = success.parent.name.removeprefix("trading_day=")
        compact_day = day.replace("-", "")
        if start_day and compact_day < start_day:
            continue
        if end_day and compact_day > end_day:
            continue
        parquet = success.parent / "part-0000.parquet"
        if parquet.is_file():
            files.append(str(parquet))
    if not files:
        raise FileNotFoundError(f"no completed archive for {exchange}.{instrument}")
    return files


def _validate_day(value: str | None) -> str | None:
    if value is not None and not re.fullmatch(r"[0-9]{8}", value):
        raise ValueError("trading day must use YYYYMMDD")
    return value


def _mapping_day(value: str | date) -> str:
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    return _validate_day(value) or ""


def _read_view(connection: duckdb.DuckDBPyConnection, files: list[str]) -> None:
    connection.from_parquet(files, hive_partitioning=False, union_by_name=True).create_view("raw_ticks")
    columns={row[0] for row in connection.execute("DESCRIBE raw_ticks").fetchall()};additions=[]
    if "provider" not in columns:additions.append("'ctp'::VARCHAR AS provider")
    if "event_type" not in columns:additions.append("'quote_snapshot'::VARCHAR AS event_type")
    if "instrument_id" not in columns:additions.append("exchange || '.' || instrument AS instrument_id")
    if "quality" not in columns:additions.append("'UNKNOWN'::VARCHAR AS quality")
    connection.execute(f"CREATE VIEW ticks AS SELECT *, {', '.join(additions)} FROM raw_ticks" if additions else "CREATE VIEW ticks AS SELECT * FROM raw_ticks")


def _time_predicate(start: datetime | None, end: datetime | None) -> tuple[str, list[datetime]]:
    if start is not None and end is not None and start >= end:
        raise ValueError("start must be before end")
    clauses: list[str] = []
    parameters: list[datetime] = []
    if start is not None:
        clauses.append("event_ts >= ?")
        parameters.append(start)
    if end is not None:
        clauses.append("event_ts < ?")
        parameters.append(end)
    return (" AND ".join(clauses) or "TRUE"), parameters


def load_ticks(
    root: str | Path,
    exchange: str,
    instrument: str,
    *,
    start_day: str | None = None,
    end_day: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    provider: str | None = None,
) -> pa.Table:
    """Load immutable raw snapshots from completed archive partitions."""
    files = _completed_files(
        Path(root), exchange, instrument, _validate_day(start_day), _validate_day(end_day)
    )
    predicate, parameters = _time_predicate(start, end)
    if provider is not None:
        if provider not in {"ctp","synthetic","ibkr","akshare"}:raise ValueError("invalid provider")
        predicate=f"({predicate}) AND provider = ?";parameters.append(provider)
    with duckdb.connect() as connection:
        _read_view(connection, files)
        return connection.execute(
            f"SELECT * FROM ticks WHERE {predicate} ORDER BY event_ts, producer_id, seq",
            parameters,
        ).to_arrow_table()


def load_bars(
    root: str | Path,
    exchange: str,
    instrument: str,
    interval: str,
    *,
    start_day: str | None = None,
    end_day: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> pa.Table:
    """Build OHLCV bars without mutating or replacing raw snapshots."""
    if interval not in SUPPORTED_INTERVALS:
        raise ValueError("interval must be one of 1m, 5m, 1h, 1d")
    start_day = _validate_day(start_day)
    end_day = _validate_day(end_day)
    if start_day and end_day and start_day > end_day:
        raise ValueError("start_day must not follow end_day")
    files = _completed_files(Path(root), exchange, instrument, start_day, end_day)
    predicate, parameters = _time_predicate(start, end)
    bucket = (
        "trading_day"
        if interval == "1d"
        else f"time_bucket(INTERVAL '{SUPPORTED_INTERVALS[interval]}', event_ts)"
    )
    bar_start = "min(event_ts)" if interval == "1d" else "bar_key"
    with duckdb.connect() as connection:
        _read_view(connection, files)
        return connection.execute(
            f"""
            WITH ordered AS (
                SELECT *, lag(volume) OVER (
                    PARTITION BY exchange, instrument, trading_day
                    ORDER BY event_ts, producer_id, seq
                ) AS previous_volume
                FROM ticks
            ), selected AS (
                SELECT *, CASE
                    WHEN previous_volume IS NULL THEN volume
                    WHEN volume < previous_volume THEN 0
                    ELSE volume - previous_volume
                END AS delta_volume
                FROM ordered
                WHERE {predicate}
            ), bucketed AS (
                SELECT *, {bucket} AS bar_key,
                    CASE WHEN last_price IS NOT NULL AND isfinite(last_price)
                         THEN last_price END AS valid_price
                FROM selected
            )
            SELECT
                exchange,
                instrument,
                trading_day,
                ? AS interval,
                {bar_start} AS bar_start,
                arg_min(valid_price, struct_pack(event_ts, producer_id, seq)) AS open,
                max(valid_price) AS high,
                min(valid_price) AS low,
                arg_max(valid_price, struct_pack(event_ts, producer_id, seq)) AS close,
                sum(delta_volume)::UBIGINT AS volume,
                arg_max(open_interest, struct_pack(event_ts, producer_id, seq)) AS open_interest,
                count(*)::UBIGINT AS snapshot_count
            FROM bucketed
            GROUP BY exchange, instrument, trading_day, bar_key
            HAVING count(valid_price) > 0
            ORDER BY trading_day, bar_key
            """,
            [*parameters, interval],
        ).to_arrow_table()


def load_continuous_ticks(
    root: str | Path,
    continuous_symbol: str,
    mappings: Iterable[Mapping[str, str | date]],
) -> pa.Table:
    """Resolve an explicit metadata mapping without rewriting physical-contract rows."""
    _validate_component("continuous symbol", continuous_symbol)
    tables: list[pa.Table] = []
    for mapping in mappings:
        day = _mapping_day(mapping["trading_day"])
        table = load_ticks(
            root,
            mapping["exchange_code"],
            mapping["instrument_id"],
            start_day=day,
            end_day=day,
        )
        table = table.append_column(
            "continuous_symbol", pa.array([continuous_symbol] * table.num_rows, pa.string())
        )
        tables.append(table)
    if not tables:
        raise ValueError("continuous mapping is empty")
    return pa.concat_tables(tables).sort_by(
        [("event_ts", "ascending"), ("producer_id", "ascending"), ("seq", "ascending")]
    )
