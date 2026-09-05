from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

DOMESTIC_FUTURES_MARKETS = frozenset({"CFFEX", "CZCE", "DCE", "GFEX", "INE", "SHFE"})


def historical_market(instrument_id: str) -> str:
    exchange = instrument_id.split(".", 1)[0].upper()
    return "DOMESTIC" if exchange in DOMESTIC_FUTURES_MARKETS else "FOREIGN"


@dataclass(frozen=True)
class HistoricalCapabilities:
    provider: str
    upstream_source: str
    supported_intervals: tuple[str, ...]
    supported_markets: tuple[str, ...] = ("DOMESTIC", "FOREIGN")
    supported_instrument_kinds: tuple[str, ...] = ()
    supports_arbitrary_range: bool = True
    bounded_recent_history: bool = False
    supports_latest_bars: bool = True
    supports_realtime_snapshot: bool = False

    def eligibility(self, instrument_id: str, interval: str,
                    instrument_kind: str | None = None) -> tuple[bool, str | None]:
        if interval not in self.supported_intervals:
            return False, "INTERVAL_NOT_SUPPORTED"
        if historical_market(instrument_id) not in self.supported_markets:
            return False, "MARKET_NOT_SUPPORTED"
        if self.supported_instrument_kinds and instrument_kind not in self.supported_instrument_kinds:
            return False, "INSTRUMENT_KIND_NOT_SUPPORTED"
        return True, None


def default_historical_capabilities(provider: str) -> tuple[HistoricalCapabilities, ...]:
    if provider.upper() == "AKSHARE":
        return (
            HistoricalCapabilities("AKSHARE", "SINA_DOMESTIC", ("1d", "1m"), ("DOMESTIC",),
                                   supports_arbitrary_range=False, bounded_recent_history=True,
                                   supports_realtime_snapshot=True),
            HistoricalCapabilities("AKSHARE", "SINA_FOREIGN", ("1d",), ("FOREIGN",),
                                   supports_arbitrary_range=True, supports_realtime_snapshot=True),
            HistoricalCapabilities("AKSHARE", "EASTMONEY_FOREIGN", ("1d",), ("FOREIGN",),
                                   supports_arbitrary_range=False, supports_realtime_snapshot=True),
        )
    return ()


class HistoricalQuality(StrEnum):
    AUTHORITATIVE = "AUTHORITATIVE"
    EXCHANGE_DIRECT = "EXCHANGE_DIRECT"
    BROKER = "BROKER"
    PUBLIC = "PUBLIC"
    BEST_EFFORT = "BEST_EFFORT"
    DERIVED = "DERIVED"
    UNKNOWN = "UNKNOWN"


class CoverageStatus(StrEnum):
    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"
    EMPTY = "EMPTY"
    UNKNOWN = "UNKNOWN"


class SelectionMode(StrEnum):
    EXPLICIT = "EXPLICIT"
    SINGLE = "SINGLE"
    COMPOSITE = "COMPOSITE"


@dataclass(frozen=True)
class HistoricalProviderPolicy:
    provider: str
    priority: int
    quality: HistoricalQuality = HistoricalQuality.UNKNOWN
    enabled: bool = True
    supported_intervals: tuple[str, ...] = ("1m", "5m", "1h", "1d")
    capabilities: tuple[HistoricalCapabilities, ...] = ()

    def eligibility(self, instrument_id: str, interval: str) -> tuple[bool, str | None]:
        capabilities = self.capabilities or default_historical_capabilities(self.provider)
        if capabilities:
            market = historical_market(instrument_id)
            market_capabilities = [capability for capability in capabilities if market in capability.supported_markets]
            if not market_capabilities:
                return False, "MARKET_NOT_SUPPORTED"
            reasons = [capability.eligibility(instrument_id, interval) for capability in market_capabilities]
            if any(eligible for eligible, _ in reasons):
                return True, None
            if all(reason == "INTERVAL_NOT_SUPPORTED" for _, reason in reasons):
                return False, "INTERVAL_NOT_SUPPORTED"
            return False, next(reason for _, reason in reasons if reason)
        if interval not in self.supported_intervals:
            return False, "INTERVAL_NOT_SUPPORTED"
        return True, None


@dataclass(frozen=True)
class HistoricalCoverage:
    provider: str
    instrument_id: str
    interval: str
    range_start: datetime
    range_end: datetime
    expected_bars: int
    observed_bars: int
    missing_bars: int
    coverage_ratio: float
    first_observed: datetime | None
    last_observed: datetime | None
    unexpected_bars: int = 0
    status: CoverageStatus = CoverageStatus.UNKNOWN


class HistoricalIncompleteError(RuntimeError):
    def __init__(self, details: dict):
        super().__init__("requested historical range is incomplete")
        self.details = details
