from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from .models import CoverageStatus, HistoricalCoverage

UTC = timezone.utc
SHANGHAI = ZoneInfo("Asia/Shanghai")
INTERVALS = {"1m": timedelta(minutes=1), "5m": timedelta(minutes=5),
             "1h": timedelta(hours=1)}


def _utc(value) -> datetime:
    if isinstance(value, datetime):
        return (value.replace(tzinfo=UTC) if value.tzinfo is None else value).astimezone(UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return (parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed).astimezone(UTC)


class ExpectedBarGenerator:
    """Generate canonical bar starts from a preloaded calendar/session snapshot."""
    def generate(self, interval: str, start: datetime, end: datetime,
                 calendar: list[dict], sessions: list[dict]) -> tuple[datetime, ...]:
        start, end = _utc(start), _utc(end)
        if start >= end:
            raise ValueError("start must be before end")
        if interval == "1d":
            values = [datetime.combine(row["trading_day"], time(), SHANGHAI).astimezone(UTC)
                      for row in calendar if row["is_trading_day"]]
            return tuple(value for value in values if start <= value < end)
        step = INTERVALS.get(interval)
        if step is None:
            raise ValueError("interval must be one of 1m, 5m, 1h, 1d")
        values = set()
        for row in calendar:
            if not row["is_trading_day"]:
                continue
            trading_day = row["trading_day"]
            night_open = row.get("night_session_open")
            for session in sessions:
                if session.get("effective_from") and trading_day < session["effective_from"]:
                    continue
                if session.get("effective_to") and trading_day > session["effective_to"]:
                    continue
                session_start = session["start_time"]
                session_end = session["end_time"]
                is_night = night_open is not None and (session_start >= time(18) or session.get("crosses_midnight"))
                start_date = night_open if is_night else trading_day
                end_date = start_date + timedelta(days=1) if session.get("crosses_midnight") or session_end <= session_start else start_date
                cursor = datetime.combine(start_date, session_start, SHANGHAI).astimezone(UTC)
                boundary = datetime.combine(end_date, session_end, SHANGHAI).astimezone(UTC)
                while cursor < boundary:
                    if start <= cursor < end:
                        values.add(cursor)
                    cursor += step
        return tuple(sorted(values))


class CoverageEngine:
    def calculate(self, provider: str, instrument_id: str, interval: str,
                  start: datetime, end: datetime, expected: tuple[datetime, ...],
                  bars: list[dict]) -> HistoricalCoverage:
        expected_set = set(expected)
        observed_all = {_utc(bar["bar_start"]) for bar in bars}
        observed = sorted(observed_all & expected_set)
        unexpected = len(observed_all - expected_set)
        total = len(expected_set)
        ratio = len(observed) / total if total else 0.0
        status = (CoverageStatus.UNKNOWN if not total else CoverageStatus.EMPTY if not observed else
                  CoverageStatus.COMPLETE if len(observed) == total else CoverageStatus.PARTIAL)
        return HistoricalCoverage(provider, instrument_id, interval, _utc(start), _utc(end), total,
                                  len(observed), total - len(observed), ratio,
                                  observed[0] if observed else None, observed[-1] if observed else None,
                                  unexpected, status)
