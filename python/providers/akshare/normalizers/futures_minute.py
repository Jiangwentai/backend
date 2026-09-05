from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from ..errors import SchemaError, ValidationError
from ..models import HistoricalBar, ProviderId
from ..registry import EndpointDefinition
from .futures_daily import _nullable_float, _nullable_int, normalize_symbol, validate_schema

SHANGHAI = ZoneInfo("Asia/Shanghai")


def trading_day_for(local_time: datetime) -> date:
    """Chinese futures night sessions belong to the next weekday trading day.

    Exchange holidays require the metadata calendar; this conservative fallback handles
    the ordinary weekday/weekend rule and deliberately does not claim holiday knowledge.
    """
    day = local_time.date()
    if local_time.hour >= 21:
        day += timedelta(days=1)
        while day.weekday() >= 5:
            day += timedelta(days=1)
    return day


def _timestamp(value: Any) -> datetime:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"invalid minute timestamp: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI)
    return parsed.astimezone(timezone.utc)


def normalize_futures_minute(rows: list[dict[str, Any]], *, definition: EndpointDefinition,
                             raw_symbol: str, exchange: str, instrument_id: str,
                             fetch_id: str, fetched_at: datetime) -> tuple[HistoricalBar, ...]:
    validate_schema(rows, definition)
    symbol = normalize_symbol(raw_symbol)
    result: list[HistoricalBar] = []
    seen: set[datetime] = set()
    for row in rows:
        bar_start = _timestamp(row["datetime"])
        if bar_start.second or bar_start.microsecond:
            raise ValidationError(f"1m bar timestamp is not minute aligned: {row['datetime']}")
        if bar_start in seen:
            raise SchemaError(f"duplicate 1m bar timestamp: {row['datetime']}")
        seen.add(bar_start)
        open_, high, low, close = (_nullable_float(row.get(name)) for name in ("open", "high", "low", "close"))
        if None in (open_, high, low, close):
            raise ValidationError("1m bar requires complete OHLC")
        if high < max(open_, close, low) or low > min(open_, close, high):
            raise ValidationError("invalid 1m OHLC range")
        local = bar_start.astimezone(SHANGHAI)
        result.append(HistoricalBar(
            ProviderId.AKSHARE, instrument_id, exchange.upper(), symbol, raw_symbol, "1m",
            bar_start, trading_day_for(local), open_, high, low, close,
            _nullable_int(row.get("volume")), _nullable_int(row.get("hold")), None, None,
            fetched_at, definition.function_name, definition.upstream_source, fetch_id,
        ))
    return tuple(sorted(result, key=lambda value: value.bar_start))
