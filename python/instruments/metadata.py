"""Read-side metadata boundary, independent of provider SDKs and price storage."""
from datetime import date
from typing import Protocol

from .models import ExplicitMapping, ProviderProductDefinition
from .normalization import normalize_provider_symbol, provider_symbol_key


class InstrumentMetadata(Protocol):
    async def list_explicit_mappings(self, provider: str, as_of: date) -> list[ExplicitMapping]: ...
    async def lookup_explicit_mapping(self, provider: str, raw_symbol: str, as_of: date) -> list[ExplicitMapping]: ...
    async def lookup_normalized_mapping(self, provider: str, symbol: str, exchange: str | None, as_of: date) -> list[ExplicitMapping]: ...
    async def lookup_explicit_provider_symbol(self, provider: str, canonical: str, as_of: date) -> list[ExplicitMapping]: ...
    async def lookup_metadata_registration(self, canonical: str, kind) -> bool | None: ...
    async def product_definitions(self, provider: str) -> list[ProviderProductDefinition]: ...


class MappingLookups:
    async def lookup_explicit_mapping(self, provider, raw_symbol, as_of):
        return [row for row in await self.list_explicit_mappings(provider, as_of) if row.provider_symbol == raw_symbol]

    async def lookup_normalized_mapping(self, provider, symbol, exchange, as_of):
        matches = []
        for row in await self.list_explicit_mappings(provider, as_of):
            try:
                normalized = normalize_provider_symbol(provider, row.provider_symbol)
            except ValueError:
                continue
            if normalized.symbol == symbol and (not exchange or row.canonical_instrument.partition(".")[0] == exchange):
                matches.append(row)
        return matches

    async def lookup_explicit_provider_symbol(self, provider, canonical, as_of):
        return [row for row in await self.list_explicit_mappings(provider, as_of) if row.canonical_instrument == canonical]


class MemoryInstrumentMetadata(MappingLookups):
    """Injectable snapshot, useful for offline resolution and deterministic tests."""
    def __init__(self, mappings=(), registered=(), definitions=(), *, registration_known=True):
        self.mappings = list(mappings)
        self.registered = set(registered)
        self.definitions = list(definitions)
        self.registration_known = registration_known

    async def list_explicit_mappings(self, provider, as_of):
        return [row for row in self.mappings if row.provider == provider and row.valid_at(as_of)]

    async def lookup_metadata_registration(self, canonical, kind):
        return canonical in self.registered if self.registration_known else None

    async def product_definitions(self, provider):
        return [row for row in self.definitions if row.provider == provider]
