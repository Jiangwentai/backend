from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys

import pytest

from archive.writer import archive_partition
from research import load_bars, load_continuous_ticks, load_ticks
from test_archive import Source, row


def archived(root: Path, instrument: str, day: str, rows: list[dict]) -> None:
    for value in rows:
        value["instrument"] = instrument
        value["trading_day"] = day
    archive_partition(Source(rows), root, "SHFE", instrument, day)


def tick(seq: int, timestamp: str, volume: int, price: float, day: str = "20260904") -> dict:
    value = row(seq)
    value["event_ts"] = int(datetime.fromisoformat(timestamp).timestamp() * 1_000_000)
    value["recv_ts"] = value["event_ts"] * 1_000
    value["volume"] = volume
    value["last_price"] = price
    value["trading_day"] = day
    return value


def test_load_ticks_reads_only_completed_partitions_and_filters_time(tmp_path: Path):
    archived(tmp_path, "zn2610", "20260904", [
        tick(1, "2026-09-03T13:00:00+00:00", 10, 100),
        tick(2, "2026-09-03T13:01:00+00:00", 12, 101),
    ])
    table = load_ticks(
        tmp_path, "SHFE", "zn2610", start=datetime(2026, 9, 3, 13, 1, tzinfo=timezone.utc)
    )
    assert table.num_rows == 1
    assert table.column("seq").to_pylist() == [2]
    with pytest.raises(ValueError):
        load_ticks(tmp_path, "../SHFE", "zn2610")


def test_intraday_bars_use_cumulative_volume_differences_and_resets(tmp_path: Path):
    archived(tmp_path, "zn2610", "20260904", [
        tick(1, "2026-09-03T13:00:01+00:00", 100, 10),
        tick(2, "2026-09-03T13:00:30+00:00", 105, 12),
        tick(3, "2026-09-03T13:01:00+00:00", 2, 11),
        tick(4, "2026-09-03T13:01:30+00:00", 6, 13),
    ])
    bars = load_bars(tmp_path, "SHFE", "zn2610", "1m").to_pylist()
    assert [(bar["open"], bar["high"], bar["low"], bar["close"]) for bar in bars] == [
        (10.0, 12.0, 10.0, 12.0), (11.0, 13.0, 11.0, 13.0)
    ]
    assert [bar["volume"] for bar in bars] == [105, 4]
    assert bars[0]["bar_start"] == datetime(2026, 9, 3, 13, 0, tzinfo=timezone.utc)


def test_invalid_price_does_not_hide_volume_delta(tmp_path: Path):
    values = [
        tick(1, "2026-09-03T13:00:01+00:00", 10, 10),
        tick(2, "2026-09-03T13:00:20+00:00", 15, 11),
        tick(3, "2026-09-03T13:00:40+00:00", 18, 12),
    ]
    values[1]["last_price"] = None
    archived(tmp_path, "zn2610", "20260904", values)
    bar = load_bars(tmp_path, "SHFE", "zn2610", "1m").to_pylist()[0]
    assert (bar["open"], bar["close"], bar["volume"], bar["snapshot_count"]) == (10.0, 12.0, 18, 3)


def test_daily_bar_groups_night_and_day_sessions_by_trading_day(tmp_path: Path):
    archived(tmp_path, "zn2610", "20260904", [
        tick(1, "2026-09-03T13:00:00+00:00", 10, 100),
        tick(2, "2026-09-04T01:00:00+00:00", 16, 102),
    ])
    bars = load_bars(tmp_path, "SHFE", "zn2610", "1d").to_pylist()
    assert len(bars) == 1
    assert bars[0]["trading_day"] == "20260904"
    assert bars[0]["volume"] == 16


def test_continuous_contract_uses_explicit_daily_mapping(tmp_path: Path):
    archived(tmp_path, "zn2610", "20260904", [tick(1, "2026-09-03T13:00:00+00:00", 1, 100)])
    archived(tmp_path, "zn2611", "20260905", [tick(2, "2026-09-04T13:00:00+00:00", 1, 101, "20260905")])
    table = load_continuous_ticks(tmp_path, "ZN_MAIN", [
        {"trading_day": "20260904", "exchange_code": "SHFE", "instrument_id": "zn2610"},
        {"trading_day": "20260905", "exchange_code": "SHFE", "instrument_id": "zn2611"},
    ])
    assert table.column("instrument").to_pylist() == ["zn2610", "zn2611"]
    assert table.column("continuous_symbol").to_pylist() == ["ZN_MAIN", "ZN_MAIN"]


def test_rejects_unsupported_interval_or_missing_archive(tmp_path: Path):
    with pytest.raises(ValueError):
        load_bars(tmp_path, "SHFE", "zn2610", "30s")
    with pytest.raises(FileNotFoundError):
        load_ticks(tmp_path, "SHFE", "zn2610")


def test_research_cli_emits_json_lines(tmp_path: Path):
    archived(tmp_path, "zn2610", "20260904", [
        tick(1, "2026-09-03T13:00:00+00:00", 4, 100),
        tick(2, "2026-09-03T13:00:30+00:00", 7, 101),
    ])
    result = subprocess.run(
        [sys.executable, "-m", "research.cli", "bars", "--archive-root", str(tmp_path),
         "--exchange", "SHFE", "--instrument", "zn2610", "--start-day", "20260904",
         "--interval", "1m"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert '"interval":"1m"' in result.stdout
    assert '"volume":7' in result.stdout
