from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from typing import Any, Protocol


class ProviderId(StrEnum):
    AKSHARE = "AKSHARE"


class DatasetType(StrEnum):
    HISTORICAL_BARS = "historical_bars"
    REFERENCE = "reference"
    INVENTORY = "inventory"
    POSITION = "position"
    BEST_EFFORT_QUOTES = "best_effort_quotes"


class EndpointStability(StrEnum):
    STABLE = "stable"
    EXPERIMENTAL = "experimental"
    DISABLED = "disabled"


class ProviderHealthState(StrEnum):
    AVAILABLE = "AVAILABLE"
    DEGRADED = "DEGRADED"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True)
class ProviderCapabilities:
    realtime_quotes: bool = False
    tick_by_tick: bool = False
    market_depth: bool = False
    historical_ticks: bool = False
    historical_bars: bool = True
    intraday_bars: bool = True
    reference_data: bool = True
    best_effort_quotes: bool = False


@dataclass(frozen=True)
class QuoteSubscription:
    instrument_id: str
    provider_symbol: str
    exchange: str
    market: str = "CF"


@dataclass(frozen=True)
class QuoteSnapshot:
    provider: ProviderId
    instrument_id: str
    exchange: str
    instrument: str
    quality: str
    event_ts: datetime
    recv_ts: datetime
    timestamp_source: str
    last_price: float | None
    volume: int | None
    turnover: float | None
    open_interest: float | None
    upper_limit_price: float | None
    lower_limit_price: float | None
    bid_price1: float | None
    bid_volume1: int | None
    ask_price1: float | None
    ask_volume1: int | None
    source: str
    upstream_source: str | None


@dataclass(frozen=True)
class ProviderHealth:
    provider: ProviderId
    state: ProviderHealthState
    last_success: datetime | None = None
    last_error: str | None = None
    recent_failures: int = 0


@dataclass(frozen=True)
class HistoricalBarRequest:
    provider_symbol: str
    start: date | None = None
    end: date | None = None
    endpoint: str = "futures_daily_sina"
    exchange: str | None = None


@dataclass(frozen=True)
class ReferenceDataRequest:
    endpoint: str = "futures_contracts_qihuo"
    parameters: dict[str, Any] = field(default_factory=lambda:{"symbol":"所有"})


@dataclass(frozen=True)
class HistoricalBar:
    provider: ProviderId
    instrument_id: str
    exchange: str
    provider_symbol: str
    raw_provider_symbol: str
    interval: str
    bar_start: datetime
    trading_day: date
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: int | None
    open_interest: int | None
    turnover: float | None
    settlement: float | None
    fetched_at: datetime
    source: str
    upstream_source: str | None
    fetch_id: str

    @property
    def identity(self) -> tuple[str, str, str, datetime]:
        return self.provider.value, self.instrument_id, self.interval, self.bar_start


@dataclass(frozen=True)
class HistoricalDataBatch:
    fetch_id: str
    endpoint: str
    rows: tuple[HistoricalBar, ...]
    rows_received: int
    rows_rejected: int
    raw_archive: str
    lineage: dict[str, Any]
    unresolved_symbols: tuple[str, ...] = ()


@dataclass(frozen=True)
class ReferenceRecord:
    provider: ProviderId
    dataset: str
    provider_key: str
    values: dict[str, Any]
    source: str
    upstream_source: str | None
    fetch_id: str
    fetched_at: datetime


@dataclass(frozen=True)
class ReferenceDataBatch:
    fetch_id: str
    endpoint: str
    rows: tuple[ReferenceRecord, ...]
    raw_archive: str
    lineage: dict[str, Any]


class HistoricalDataProvider(Protocol):
    async def fetch_bars(self, request: HistoricalBarRequest) -> HistoricalDataBatch: ...


class ReferenceDataProvider(Protocol):
    async def fetch_reference(self, request: ReferenceDataRequest) -> ReferenceDataBatch: ...
