from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from ..errors import EmptyDatasetError, SchemaError, ValidationError
from ..symbols import quote_symbol_key
from ..models import ProviderId, QuoteSnapshot, QuoteSubscription
from ..registry import EndpointDefinition
from .futures_daily import _nullable_float, _nullable_int

SHANGHAI = ZoneInfo("Asia/Shanghai")


def _event_time(row: dict[str, Any], recv_at: datetime) -> tuple[datetime, str]:
    raw_time = str(row.get("time") or "").strip()
    raw_date = str(row.get("date") or "").strip()
    if raw_time and raw_date:
        try:
            return datetime.fromisoformat(f"{raw_date} {raw_time}").replace(tzinfo=SHANGHAI).astimezone(timezone.utc), "UPSTREAM"
        except ValueError:
            pass
    return recv_at, "RECEIVE_TIME"


def normalize_quotes(rows: list[dict[str, Any]], *, definition: EndpointDefinition,
                     subscriptions: list[QuoteSubscription], recv_at: datetime) -> tuple[QuoteSnapshot, ...]:
    if not rows:
        raise EmptyDatasetError(f"empty response from {definition.name}")
    required = {"symbol", "current_price"}
    requested = {}
    for subscription in subscriptions:
        key = quote_symbol_key(subscription.provider_symbol, subscription.exchange)
        if key in requested:
            raise SchemaError(f"AMBIGUOUS_REQUEST_SYMBOL: {key}")
        requested[key] = subscription
    returned = {}
    for row in rows:
        missing = required - row.keys()
        if missing:
            raise SchemaError(f"{definition.name} missing required columns: {sorted(missing)}")
        if not isinstance(row["symbol"], str) or not row["symbol"].strip():
            raise SchemaError("MISSING_QUOTE_SYMBOL")
        try:
            key = quote_symbol_key(row["symbol"])
        except ValueError as exc:
            raise SchemaError(str(exc)) from exc
        if key in returned:
            raise SchemaError(f"DUPLICATE_QUOTE_SYMBOL: {key}")
        if key not in requested:
            raise SchemaError(f"UNEXPECTED_QUOTE_SYMBOL: {key}")
        try:
            quote_symbol_key(row["symbol"], requested[key].exchange)
        except ValueError as exc:
            raise SchemaError(str(exc)) from exc
        returned[key] = row
    missing_symbols = requested.keys() - returned.keys()
    if missing_symbols:
        raise SchemaError(f"MISSING_QUOTE_SYMBOLS: {sorted(missing_symbols)}")
    result = []
    for key, subscription in requested.items():
        row = returned[key]
        event_ts, timestamp_source = _event_time(row, recv_at)
        last_price = _nullable_float(row.get("current_price"))
        if last_price is None:
            raise ValidationError(f"missing current price for {subscription.provider_symbol}")
        result.append(QuoteSnapshot(
            ProviderId.AKSHARE, subscription.instrument_id, subscription.exchange.upper(),
            subscription.instrument_id.split(".", 1)[-1], "BEST_EFFORT", event_ts, recv_at,
            timestamp_source, last_price, _nullable_int(row.get("volume")),
            _nullable_float(row.get("amount")), _nullable_float(row.get("hold")), None, None,
            _nullable_float(row.get("bid_price")), _nullable_int(row.get("buy_vol")),
            _nullable_float(row.get("ask_price")), _nullable_int(row.get("sell_vol")),
            definition.function_name, definition.upstream_source,
            provider_symbol=subscription.provider_symbol, raw_provider_symbol=row["symbol"],
            instrument_kind=subscription.instrument_kind,
        ))
    return tuple(result)
