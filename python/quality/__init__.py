"""Offline quality checks for immutable market-data archives."""

from .checks import QualityCheck, QualityReport, check_archive

__all__ = ["QualityCheck", "QualityReport", "check_archive"]
