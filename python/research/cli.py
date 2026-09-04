from __future__ import annotations

import argparse
from datetime import datetime
import json
import os

from .query import load_bars, load_ticks


def _json_default(value):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    raise TypeError(f"cannot serialize {type(value).__name__}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Query completed Parquet market-data archives")
    parser.add_argument("command", choices=["ticks", "bars"])
    parser.add_argument("--archive-root", default=os.getenv("ARCHIVE_ROOT", "data/market"))
    parser.add_argument("--exchange", required=True)
    parser.add_argument("--instrument", required=True)
    parser.add_argument("--start-day")
    parser.add_argument("--end-day")
    parser.add_argument("--start", type=datetime.fromisoformat)
    parser.add_argument("--end", type=datetime.fromisoformat)
    parser.add_argument("--interval", choices=["1m", "5m", "1h", "1d"], default="1m")
    arguments = parser.parse_args()
    common = {
        "start_day": arguments.start_day,
        "end_day": arguments.end_day,
        "start": arguments.start,
        "end": arguments.end,
    }
    if arguments.command == "ticks":
        table = load_ticks(arguments.archive_root, arguments.exchange, arguments.instrument, **common)
    else:
        table = load_bars(
            arguments.archive_root,
            arguments.exchange,
            arguments.instrument,
            arguments.interval,
            **common,
        )
    for record in table.to_pylist():
        print(json.dumps(record, default=_json_default, separators=(",", ":")))


if __name__ == "__main__":
    main()
