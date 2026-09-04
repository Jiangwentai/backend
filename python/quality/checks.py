from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb

from archive.writer import verify_archive
from research.query import _completed_files, _read_view, _validate_day


@dataclass(frozen=True)
class QualityCheck:
    name: str
    severity: str
    violations: int


@dataclass(frozen=True)
class QualityReport:
    exchange: str
    instrument: str
    row_count: int
    status: str
    checks: tuple[QualityCheck, ...]

    def as_dict(self) -> dict:
        return {
            "exchange": self.exchange,
            "instrument": self.instrument,
            "row_count": self.row_count,
            "status": self.status,
            "checks": [asdict(check) for check in self.checks],
        }


def check_archive(
    root: str | Path,
    exchange: str,
    instrument: str,
    *,
    start_day: str | None = None,
    end_day: str | None = None,
) -> QualityReport:
    start_day = _validate_day(start_day)
    end_day = _validate_day(end_day)
    if start_day and end_day and start_day > end_day:
        raise ValueError("start_day must not follow end_day")
    files = _completed_files(Path(root), exchange, instrument, start_day, end_day)
    for filename in files:
        verify_archive(Path(filename).parent)
    with duckdb.connect() as connection:
        _read_view(connection,files)
        values = connection.execute("""
            WITH ordered AS (
                SELECT *, lag(volume) OVER (
                    PARTITION BY provider, exchange, instrument, trading_day
                    ORDER BY event_ts, producer_id, seq
                ) AS previous_volume
                FROM ticks
            )
            SELECT
                count(*) AS row_count,
                count(*) - count(DISTINCT (event_ts, provider, producer_id, seq)) AS duplicate_identity,
                count(*) FILTER (WHERE provider IS NULL OR provider = '' OR producer_id IS NULL OR producer_id = '' OR seq IS NULL) AS missing_identity,
                count(*) FILTER (WHERE NOT regexp_full_match(trading_day, '[0-9]{8}')) AS invalid_trading_day,
                count(*) FILTER (WHERE volume < 0) AS negative_volume,
                count(*) FILTER (WHERE last_price IS NOT NULL AND NOT isfinite(last_price)) AS nonfinite_price,
                count(*) FILTER (WHERE recv_ts < event_ts) AS receive_before_event,
                count(*) FILTER (WHERE previous_volume IS NOT NULL AND volume < previous_volume) AS volume_decrease,
                count(*) FILTER (WHERE bid_price1 IS NOT NULL AND ask_price1 IS NOT NULL AND bid_price1 > ask_price1) AS crossed_book
            FROM ordered WHERE event_type='quote_snapshot'
        """).fetchone()
    names = (
        ("duplicate_identity", "ERROR"),
        ("missing_identity", "ERROR"),
        ("invalid_trading_day", "ERROR"),
        ("negative_volume", "ERROR"),
        ("nonfinite_price", "ERROR"),
        ("receive_before_event", "ERROR"),
        ("cumulative_volume_decrease", "WARNING"),
        ("crossed_top_of_book", "WARNING"),
    )
    checks = tuple(QualityCheck(name, severity, int(values[index + 1])) for index, (name, severity) in enumerate(names))
    status = "FAIL" if any(check.severity == "ERROR" and check.violations for check in checks) else "PASS"
    return QualityReport(exchange, instrument, int(values[0]), status, checks)
