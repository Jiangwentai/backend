from __future__ import annotations

from datetime import date, datetime, time, timezone
import math
import re
from typing import Any
from zoneinfo import ZoneInfo

from ..errors import EmptyDatasetError, SchemaError, ValidationError
from ..models import HistoricalBar, ProviderId
from ..registry import EndpointDefinition


def normalize_symbol(value: str) -> str:
    # Comparison/provenance only; identity validation belongs to the resolver.
    from instruments.normalization import provider_symbol_key
    try:
        return provider_symbol_key("akshare", value)
    except ValueError as exc:
        raise ValidationError(str(exc)) from exc


def _nullable_float(value: Any) -> float | None:
    if value is None or value == "": return None
    result = float(value)
    return result if math.isfinite(result) else None


def _nullable_int(value: Any) -> int | None:
    number = _nullable_float(value)
    if number is None: return None
    if number < 0 or not number.is_integer(): raise ValidationError(f"invalid non-negative integer: {value}")
    return int(number)


def _date(value: Any) -> date:
    if isinstance(value, datetime): return value.date()
    if isinstance(value, date): return value
    try: return date.fromisoformat(str(value).strip()[:10])
    except ValueError as exc: raise ValidationError(f"invalid trading date: {value}") from exc


def validate_schema(rows: list[dict[str, Any]], definition: EndpointDefinition) -> None:
    if not rows:
        if definition.empty_is_error: raise EmptyDatasetError(f"empty response from {definition.name}")
        return
    columns = set(rows[0])
    missing = definition.required_columns - columns
    if missing: raise SchemaError(f"{definition.name} missing required columns: {sorted(missing)}")
    known = definition.required_columns | definition.optional_columns
    unexpected = columns - known
    if unexpected: raise SchemaError(f"{definition.name} unexpected columns: {sorted(unexpected)}")
    if any(set(row) != columns for row in rows): raise SchemaError(f"{definition.name} returned inconsistent row schemas")


def normalize_futures_daily(rows: list[dict[str, Any]], *, definition: EndpointDefinition,
                            raw_symbol: str, exchange: str, instrument_id: str,
                            fetch_id: str, fetched_at: datetime) -> tuple[HistoricalBar, ...]:
    validate_schema(rows, definition); symbol = normalize_symbol(raw_symbol); result = []
    seen: set[date] = set()
    for row in rows:
        trading_day = _date(row["date"])
        if trading_day in seen: raise SchemaError(f"duplicate trading day: {trading_day}")
        seen.add(trading_day)
        open_, high, low, close = (_nullable_float(row.get(name)) for name in ("open", "high", "low", "close"))
        if high is not None and low is not None and high < low: raise ValidationError("bar high is below low")
        if high is not None and low is not None:
            if open_ is not None and not low <= open_ <= high: raise ValidationError("bar open outside range")
            if close is not None and not low <= close <= high: raise ValidationError("bar close outside range")
        result.append(HistoricalBar(
            ProviderId.AKSHARE, instrument_id, exchange.upper(), symbol, raw_symbol, "1d",
            datetime.combine(trading_day,time(),ZoneInfo("Asia/Shanghai")).astimezone(timezone.utc), trading_day,
            open_, high, low, close, _nullable_int(row.get("volume")),
            _nullable_int(row.get("hold")), None, _nullable_float(row.get("settle")),
            fetched_at, definition.function_name, definition.upstream_source, fetch_id,
        ))
    return tuple(result)


def normalize_eastmoney_foreign_daily(rows: list[dict[str, Any]], *, definition: EndpointDefinition,
                                      raw_symbol: str, exchange: str, instrument_id: str,
                                      fetch_id: str, fetched_at: datetime) -> tuple[HistoricalBar, ...]:
    validate_schema(rows,definition);result=[];seen=set()
    for row in rows:
        trading_day=_date(row["日期"])
        if trading_day in seen:raise SchemaError(f"duplicate trading day: {trading_day}")
        seen.add(trading_day)
        open_,close,high,low=(_nullable_float(row.get(name)) for name in ("开盘","最新价","最高","最低"))
        if high is not None and low is not None and high<low:raise ValidationError("bar high is below low")
        if high is not None and low is not None:
            if open_ is not None and not low<=open_<=high:raise ValidationError("bar open outside range")
            if close is not None and not low<=close<=high:raise ValidationError("bar close outside range")
        result.append(HistoricalBar(ProviderId.AKSHARE,instrument_id,exchange.upper(),raw_symbol,raw_symbol,"1d",
          datetime.combine(trading_day,time(),ZoneInfo("Asia/Shanghai")).astimezone(timezone.utc),trading_day,
          open_,high,low,close,_nullable_int(row.get("总量")),_nullable_int(row.get("持仓")),None,None,
          fetched_at,definition.function_name,definition.upstream_source,fetch_id))
    return tuple(result)
