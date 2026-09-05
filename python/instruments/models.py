from dataclasses import dataclass, field
from datetime import date
from typing import Any

from .kinds import InstrumentKind


@dataclass(frozen=True)
class ParsedInstrument:
    provider: str
    raw_symbol: str
    normalized_symbol: str
    kind: InstrumentKind = InstrumentKind.UNKNOWN
    exchange: str | None = None
    product: str | None = None
    canonical_instrument: str | None = None
    contract_code: str | None = None
    delivery_month: str | None = None
    tenor: str | None = None
    method: str = "UNRESOLVED"
    reason: str | None = None


@dataclass(frozen=True)
class InstrumentResolution:
    resolved: bool
    canonical_instrument: str | None
    kind: InstrumentKind
    exchange: str | None
    product: str | None
    delivery_month: str | None
    tenor: str | None
    raw_symbol: str
    normalized_symbol: str | None
    method: str
    explicit_mapping: bool
    metadata_registered: bool | None
    reason: str | None = None
    provider: str = ""
    contract_code: str | None = None
    conflict: bool = False


@dataclass(frozen=True)
class ProviderSymbolResolution:
    resolved: bool
    provider: str
    canonical_instrument: str
    provider_symbol: str | None = None
    method: str = "UNRESOLVED"
    reason: str | None = None
    provider_source: str | None = None


@dataclass(frozen=True)
class ExplicitMapping:
    provider: str
    provider_symbol: str
    canonical_instrument: str
    kind: InstrumentKind = InstrumentKind.PHYSICAL_FUTURE
    product: str | None = None
    delivery_month: str | None = None
    tenor: str | None = None
    valid_from: date | None = None
    valid_to: date | None = None
    provider_source: str | None = None

    def valid_at(self, as_of: date) -> bool:
        return (self.valid_from is None or self.valid_from <= as_of) and (self.valid_to is None or self.valid_to >= as_of)


@dataclass(frozen=True)
class ProviderProductDefinition:
    provider: str
    provider_symbol: str
    exchange: str | None
    product: str | None
    kind: InstrumentKind
    tenor: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    provider_source: str | None = None


@dataclass(frozen=True)
class ForeignProductDefinition:
    provider: str
    provider_root: str
    canonical_exchange: str
    canonical_product: str
