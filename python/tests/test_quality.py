from pathlib import Path
import json
import subprocess
import sys

from archive.writer import archive_partition
from quality import check_archive
from test_archive import Source, row


def check(report, name):
    return next(value for value in report.checks if value.name == name)


def valid_row(seq: int):
    value = row(seq)
    value["recv_ts"] = value["event_ts"] * 1_000 + 1_000
    return value


def test_quality_passes_valid_archive_and_reports_warnings(tmp_path: Path):
    first = valid_row(1)
    second = valid_row(2)
    second["volume"] = 50
    archive_partition(Source([first, second]), tmp_path, "SHFE", "zn2610", "20260904")
    report = check_archive(tmp_path, "SHFE", "zn2610")
    assert report.status == "PASS" and report.row_count == 2
    assert check(report, "cumulative_volume_decrease").violations == 1


def test_quality_fails_duplicate_identity_and_negative_volume(tmp_path: Path):
    first = valid_row(1)
    duplicate = valid_row(1)
    duplicate["event_ts"] = first["event_ts"]
    duplicate["volume"] = -1
    archive_partition(Source([first, duplicate]), tmp_path, "SHFE", "zn2610", "20260904")
    report = check_archive(tmp_path, "SHFE", "zn2610")
    assert report.status == "FAIL"
    assert check(report, "duplicate_identity").violations == 1
    assert check(report, "negative_volume").violations == 1


def test_quality_cli_has_ci_friendly_json_and_exit_code(tmp_path: Path):
    archive_partition(Source([valid_row(1)]), tmp_path, "SHFE", "zn2610", "20260904")
    result = subprocess.run(
        [sys.executable, "-m", "quality.cli", "--archive-root", str(tmp_path),
         "--exchange", "SHFE", "--instrument", "zn2610"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert json.loads(result.stdout)["status"] == "PASS"
