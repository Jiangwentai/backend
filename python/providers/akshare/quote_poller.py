from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from dataclasses import replace
from instruments import ProviderInstrumentResolver, InstrumentKind
from instruments.registry import DOMESTIC_EXCHANGES
import logging
import time
import uuid
from zoneinfo import ZoneInfo

from live.ingress import LiveEventIngress

from .endpoints import RealtimeQuoteAdapter
from .errors import EmptyDatasetError, SchemaError, MappingError
from .health import HealthTracker
from .models import QuoteSnapshot, QuoteSubscription
from .normalizers.futures_minute import trading_day_for

logger = logging.getLogger(__name__)
MINIMUM_POLL_INTERVAL_SECONDS = 3.0


def snapshot_to_live_event(snapshot: QuoteSnapshot, *, producer_id: str, seq: int) -> dict:
    local_time = snapshot.event_ts.astimezone(ZoneInfo("Asia/Shanghai"))
    trading_day = trading_day_for(local_time).strftime("%Y%m%d")
    action_day = local_time.date().strftime("%Y%m%d")
    return {
        "schema_version": 2, "provider": "AKSHARE", "event_type": "quote_snapshot",
        "instrument_id": snapshot.instrument_id, "quality": "BEST_EFFORT",
        "exchange": snapshot.exchange, "instrument": snapshot.instrument,
        "event_ts": int(snapshot.event_ts.timestamp()*1_000_000),
        "recv_ts": int(snapshot.recv_ts.timestamp()*1_000_000_000),
        "timestamp_source": snapshot.timestamp_source,
        "producer_id": producer_id, "seq": seq, "trading_day": trading_day, "action_day": action_day,
        "last_price": snapshot.last_price, "volume": snapshot.volume,
        "turnover": snapshot.turnover, "open_interest": snapshot.open_interest,
        "upper_limit_price": snapshot.upper_limit_price, "lower_limit_price": snapshot.lower_limit_price,
        "bid_price": [snapshot.bid_price1, None, None, None, None],
        "bid_volume": [snapshot.bid_volume1, None, None, None, None],
        "ask_price": [snapshot.ask_price1, None, None, None, None],
        "ask_volume": [snapshot.ask_volume1, None, None, None, None],
        "source": snapshot.source, "upstream_source": snapshot.upstream_source,
        "provider_symbol": snapshot.provider_symbol, "raw_provider_symbol": snapshot.raw_provider_symbol,
        "instrument_kind": snapshot.instrument_kind,
    }


class AkshareQuotePoller:
    def __init__(self, client, ingress: LiveEventIngress, subscriptions: list[QuoteSubscription], *,
                 poll_interval_seconds: float = 5.0, request_timeout_seconds: float = 10.0,
                 batch_size: int = 20, resolver: ProviderInstrumentResolver | None = None):
        if poll_interval_seconds < MINIMUM_POLL_INTERVAL_SECONDS:
            raise ValueError(f"quote polling interval must be >= {MINIMUM_POLL_INTERVAL_SECONDS}s")
        if request_timeout_seconds <= 0 or batch_size < 1:
            raise ValueError("invalid quote poller limits")
        self.resolver = resolver or ProviderInstrumentResolver()
        client.metrics.instrument_resolution = self.resolver.metrics
        self.client=client;self.ingress=ingress;self.subscriptions=list(subscriptions)
        self.poll_interval=poll_interval_seconds;self.request_timeout=request_timeout_seconds;self.batch_size=batch_size
        self.adapter=RealtimeQuoteAdapter(client);self.producer_id=str(uuid.uuid4());self.seq=0
        self.health=HealthTracker();self._stop=asyncio.Event();self.events_emitted=0;self.poll_lag_seconds=0.0

    async def poll_once(self) -> int:
        started=time.monotonic();emitted=0
        groups: dict[str,list[QuoteSubscription]]={}
        for subscription in self.subscriptions:groups.setdefault(subscription.market,[]).append(subscription)
        try:
            for market,subscriptions in groups.items():
                for offset in range(0,len(subscriptions),self.batch_size):
                    batch=subscriptions[offset:offset+self.batch_size]
                    verified = []
                    for subscription in batch:
                        resolution = await self.resolver.resolve_raw("akshare", subscription.provider_symbol,
                            exchange_hint=subscription.exchange, as_of=datetime.now(ZoneInfo("Asia/Shanghai")).date())
                        if not resolution.resolved or resolution.canonical_instrument != subscription.instrument_id:
                            raise MappingError(f"SUBSCRIPTION_IDENTITY_CONFLICT: {subscription.provider_symbol}: {resolution.reason}")
                        if resolution.exchange not in DOMESTIC_EXCHANGES or resolution.kind not in {InstrumentKind.PHYSICAL_FUTURE, InstrumentKind.CONTINUOUS_FUTURE}:
                            raise MappingError("UNSUPPORTED_ENDPOINT_INSTRUMENT: Sina domestic quote endpoint")
                        verified.append(replace(subscription, instrument_kind=resolution.kind.value))
                    batch = verified
                    recv_at=datetime.now(timezone.utc)
                    self.client.metrics.quote_requests_total+=1
                    rows=await asyncio.wait_for(self.adapter.fetch_native([item.provider_symbol for item in batch],market),self.request_timeout)
                    snapshots=self.adapter.normalize(rows,batch,recv_at=recv_at)
                    self.client.metrics.quote_rows_received_total += len(rows)
                    for snapshot in snapshots:
                        self.seq+=1
                        await self.ingress.emit(snapshot_to_live_event(snapshot,producer_id=self.producer_id,seq=self.seq))
                        emitted+=1
            self.events_emitted+=emitted;self.client.metrics.quote_events_emitted_total+=emitted
            self.client.metrics.quote_last_success_timestamp=time.time();self.health.success()
            return emitted
        except Exception as exc:
            self.client.metrics.quote_request_failures_total+=1;self.health.failure(exc)
            if isinstance(exc,MappingError):
                self.client.metrics.mapping_errors_total+=1
                self.client.metrics.quote_mapping_errors_total+=1
            if isinstance(exc,SchemaError):self.client.metrics.quote_schema_errors_total+=1
            if isinstance(exc,EmptyDatasetError):self.client.metrics.quote_empty_responses_total+=1
            logger.warning("provider=akshare component=quote_poller endpoint=%s subscription_count=%d error_class=%s error=%s",
                           self.adapter.definition.function_name,len(self.subscriptions),type(exc).__name__,exc)
            raise
        finally:self.client.metrics.quote_poll_duration_seconds+=time.monotonic()-started

    async def run(self) -> None:
        self.client.metrics.quote_active_subscriptions=len(self.subscriptions)
        while not self._stop.is_set():
            started=time.monotonic()
            try:await self.poll_once()
            except Exception:pass
            delay=max(0.0,self.poll_interval-(time.monotonic()-started));self.poll_lag_seconds=max(0.0,-delay)
            try:await asyncio.wait_for(self._stop.wait(),delay)
            except TimeoutError:pass
        self.client.metrics.quote_active_subscriptions=0

    def stop(self) -> None:self._stop.set()
