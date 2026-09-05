from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from ..errors import EmptyDatasetError, SchemaError, ValidationError
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
    if len(rows) != len(subscriptions):
        raise SchemaError(f"{definition.name} returned {len(rows)} rows for {len(subscriptions)} symbols")
    required = {"symbol", "current_price"}
    result = []
    for row, subscription in zip(rows, subscriptions, strict=True):
        missing = required - row.keys()
        if missing:
            raise SchemaError(f"{definition.name} missing required columns: {sorted(missing)}")
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
        ))
    return tuple(result)
