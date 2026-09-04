from __future__ import annotations

import argparse
import json
import os

from .checks import check_archive


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit completed Parquet market-data archives")
    parser.add_argument("--archive-root", default=os.getenv("ARCHIVE_ROOT", "data/market"))
    parser.add_argument("--exchange", required=True)
    parser.add_argument("--instrument", required=True)
    parser.add_argument("--start-day")
    parser.add_argument("--end-day")
    arguments = parser.parse_args()
    report = check_archive(
        arguments.archive_root,
        arguments.exchange,
        arguments.instrument,
        start_day=arguments.start_day,
        end_day=arguments.end_day,
    )
    print(json.dumps(report.as_dict(), sort_keys=True, separators=(",", ":")))
    raise SystemExit(0 if report.status == "PASS" else 1)


if __name__ == "__main__":
    main()
