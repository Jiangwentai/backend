from dataclasses import replace
from datetime import date
import re

from .chinese_futures import parse_chinese_future
from .foreign_futures import alias_canonical, parse_foreign_future
from .kinds import InstrumentKind as Kind
from .metadata import MemoryInstrumentMetadata
from .metrics import ResolutionMetrics
from .models import InstrumentResolution, ParsedInstrument, ProviderSymbolResolution
from .month_codes import month_to_month_code, expand_yymm
from .normalization import normalize_provider_symbol, provider_symbol_key
from .registry import DOMESTIC_EXCHANGES, DOMESTIC_PRODUCTS, FOREIGN_ROOTS, PRODUCT_ALIASES


class ProviderInstrumentResolver:
    def __init__(self, metadata=None, *, roots=FOREIGN_ROOTS, aliases=PRODUCT_ALIASES, metrics=None):
        self.metadata = metadata if metadata is not None else MemoryInstrumentMetadata(registration_known=False)
        self.roots = tuple(roots)
        self.aliases = tuple(aliases)
        self.metrics = metrics if metrics is not None else ResolutionMetrics()

    async def _aliases(self, provider, provider_source=None):
        definitions = await self.metadata.product_definitions(provider)
        keys = {(row.provider_symbol,row.provider_source) for row in definitions}
        values=tuple(row for row in self.aliases if row.provider != provider or
                     (row.provider_symbol,row.provider_source) not in keys) + tuple(definitions)
        source=(provider_source or ("SINA" if provider=="akshare" else None))
        return tuple(row for row in values if row.provider!=provider or row.provider_source is None or row.provider_source==source)

    def parse(self, provider, raw_symbol, *, exchange_hint=None, as_of=None, aliases=None):
        provider = provider.lower()
        try:
            normalized = normalize_provider_symbol(provider, raw_symbol, exchange_hint)
        except ValueError as exc:
            return ParsedInstrument(provider, raw_symbol, raw_symbol, reason=str(exc))
        aliases = self.aliases if aliases is None else aliases
        if normalized.exchange in DOMESTIC_EXCHANGES or re.fullmatch(r"[A-Za-z]+[0-9]{1,4}", normalized.symbol):
            return parse_chinese_future(raw_symbol, exchange_hint, as_of=as_of, provider=provider)
        return parse_foreign_future(provider, raw_symbol, exchange_hint=exchange_hint, as_of=as_of,
                                    roots=self.roots, aliases=aliases)

    def parse_canonical(self, provider, canonical, *, as_of=None, aliases=None):
        aliases = self.aliases if aliases is None else aliases
        exchange, _, instrument = canonical.partition(".")
        if exchange in DOMESTIC_EXCHANGES:
            return self.parse(provider, canonical, as_of=as_of, aliases=aliases)
        for alias in aliases:
            if alias.provider == provider and alias_canonical(alias) == canonical:
                return self.parse(provider, alias.provider_symbol, exchange_hint=exchange, as_of=as_of, aliases=aliases)
        match = re.fullmatch(r"([a-z]+)([0-9]{4})", instrument)
        roots = [root for root in self.roots if root.provider == provider and root.canonical_exchange == exchange
                 and match and root.canonical_product == match[1]]
        if len(roots) == 1:
            try:
                native = roots[0].provider_root + match[2][:2] + month_to_month_code(int(match[2][2:]))
                return self.parse(provider, native, exchange_hint=exchange, as_of=as_of, aliases=aliases)
            except ValueError:
                pass
        return ParsedInstrument(provider, canonical, canonical, reason="UNRECOGNIZED_CANONICAL_TARGET")

    async def resolve_raw(self, provider: str, raw_symbol: str, *, exchange_hint: str | None = None,
                          as_of: date | None = None, provider_source: str | None = None) -> InstrumentResolution:
        provider = provider.lower()
        effective_source=provider_source or ("SINA" if provider=="akshare" else None)
        effective_date = as_of or date.today()  # mapping validity only; no hidden decade inference
        exact = await self.metadata.lookup_explicit_mapping(provider, raw_symbol, effective_date)
        exact=[row for row in exact if row.provider_source is None or row.provider_source==effective_source]
        try:
            normalized = normalize_provider_symbol(provider, raw_symbol, exchange_hint)
        except ValueError as exc:
            return self._finish(ParsedInstrument(provider, raw_symbol, raw_symbol, reason=str(exc)), exchange_hint=exchange_hint)
        mappings = exact
        method = "EXPLICIT_MAPPING"
        if not mappings:
            mappings = await self.metadata.lookup_normalized_mapping(provider, normalized.symbol, normalized.exchange, effective_date)
            mappings=[row for row in mappings if row.provider_source is None or row.provider_source==effective_source]
            method = "NORMALIZED_EXPLICIT_MAPPING"
        aliases = await self._aliases(provider,provider_source)
        parsed = self.parse(provider, raw_symbol, exchange_hint=exchange_hint, as_of=as_of, aliases=aliases)
        if mappings:
            identities = {(m.canonical_instrument, m.kind, m.product, m.delivery_month, m.tenor) for m in mappings}
            if len(identities) != 1:
                return self._finish(replace(parsed, canonical_instrument=None, method="UNRESOLVED", reason="AMBIGUOUS_EXPLICIT_MAPPING"),
                                    conflict=True, exchange_hint=exchange_hint)
            mapping = mappings[0]
            exchange = mapping.canonical_instrument.partition(".")[0]
            if normalized.exchange and normalized.exchange != exchange:
                return self._finish(replace(parsed, canonical_instrument=None, method="UNRESOLVED", reason="EXCHANGE_HINT_CONFLICT"),
                                    conflict=True, exchange_hint=exchange_hint)
            conflict = bool(parsed.canonical_instrument and parsed.canonical_instrument != mapping.canonical_instrument)
            # Enrichment must describe the explicit target, never the raw parser's target.
            target = self.parse_canonical(provider, mapping.canonical_instrument, as_of=as_of, aliases=aliases)
            parsed = ParsedInstrument(provider, raw_symbol, normalized.symbol, mapping.kind, exchange,
                                      mapping.product or (target.product if target.canonical_instrument == mapping.canonical_instrument else None),
                                      mapping.canonical_instrument, target.contract_code if target.canonical_instrument == mapping.canonical_instrument else None,
                                      mapping.delivery_month or (target.delivery_month if target.canonical_instrument == mapping.canonical_instrument else None),
                                      mapping.tenor, method, "provider_mapping_parser_conflict" if conflict else None)
            registered = await self.metadata.lookup_metadata_registration(parsed.canonical_instrument, parsed.kind)
            return self._finish(parsed, explicit=True, registered=registered, conflict=conflict, exchange_hint=exchange_hint)
        registered = await self.metadata.lookup_metadata_registration(parsed.canonical_instrument, parsed.kind) if parsed.canonical_instrument else None
        return self._finish(parsed, registered=registered, exchange_hint=exchange_hint)

    def _finish(self, parsed, *, explicit=False, registered=None, conflict=False, exchange_hint=None):
        value = InstrumentResolution(bool(parsed.canonical_instrument), parsed.canonical_instrument, parsed.kind,
                                     parsed.exchange, parsed.product, parsed.delivery_month, parsed.tenor,
                                     parsed.raw_symbol, parsed.normalized_symbol, parsed.method, explicit, registered,
                                     parsed.reason, parsed.provider, parsed.contract_code, conflict)
        self.metrics.record(value, exchange_hint)
        return value

    async def format_provider_symbol(self, provider: str, canonical: str, *, as_of: date | None = None,
                                     provider_source: str | None = None) -> ProviderSymbolResolution:
        provider = provider.lower()
        mappings = await self.metadata.lookup_explicit_provider_symbol(provider, canonical, as_of or date.today())
        effective_source=provider_source or ("SINA" if provider=="akshare" else None)
        mappings=[row for row in mappings if row.provider_source is None or row.provider_source==effective_source]
        symbols = {row.provider_symbol for row in mappings}
        if len(symbols) > 1:
            return ProviderSymbolResolution(False, provider, canonical, reason="AMBIGUOUS_REVERSE_MAPPING")
        explicit = bool(symbols)
        symbol = next(iter(symbols)) if symbols else None
        exchange, separator, instrument = canonical.partition(".")
        if not separator:
            return ProviderSymbolResolution(False, provider, canonical, reason="INVALID_CANONICAL_INSTRUMENT")
        aliases = await self._aliases(provider,provider_source)
        if symbol is None:
            alias_symbols = {row.provider_symbol for row in aliases if row.provider == provider and alias_canonical(row) == canonical}
            if len(alias_symbols) > 1:
                return ProviderSymbolResolution(False, provider, canonical, reason="AMBIGUOUS_REVERSE_ALIAS")
            if alias_symbols:
                symbol = next(iter(alias_symbols))
            elif provider == "akshare":
                match = re.fullmatch(r"([a-z]+)([0-9]{4})", instrument)
                if match:
                    try:
                        expand_yymm(match[2], as_of=as_of)
                    except ValueError as exc:
                        return ProviderSymbolResolution(False, provider, canonical, reason=str(exc))
                    if exchange in DOMESTIC_EXCHANGES and match[1].upper() in DOMESTIC_PRODUCTS[exchange]:
                        # Sina daily/minute endpoints accept YYMM including CZCE.
                        symbol = match[1].upper() + match[2]
                    else:
                        roots = [root for root in self.roots if root.provider == provider and
                                 root.canonical_exchange == exchange and root.canonical_product == match[1]]
                        if len(roots) == 1:
                            symbol = roots[0].provider_root + match[2][:2] + month_to_month_code(int(match[2][2:]))
                elif instrument.endswith(".continuous") and exchange in DOMESTIC_EXCHANGES:
                    root = instrument.removesuffix(".continuous").upper()
                    if root in DOMESTIC_PRODUCTS[exchange]:
                        symbol = root + "0"
        if symbol is None:
            return ProviderSymbolResolution(False, provider, canonical, reason="NO_PROVIDER_FORMATTER")
        # A forward override can invalidate otherwise deterministic reverse formatting.
        forward = await self.resolve_raw(provider, symbol, exchange_hint=exchange, as_of=as_of,provider_source=provider_source)
        if not forward.resolved or forward.canonical_instrument != canonical:
            return ProviderSymbolResolution(False, provider, canonical, reason="ROUND_TRIP_CONFLICT")
        return ProviderSymbolResolution(True, provider, canonical, symbol,
                                        "EXPLICIT_MAPPING" if explicit else "DETERMINISTIC_FORMATTER",
                                        provider_source=(provider_source or ("SINA" if provider=="akshare" else None)))

    async def audit_mappings(self, provider: str, *, as_of: date | None = None):
        rows = await self.metadata.list_explicit_mappings(provider.lower(), as_of or date.today())
        aliases = await self._aliases(provider.lower())
        result = []
        normalized_targets = {}
        for row in rows:
            key = provider_symbol_key(provider, row.provider_symbol)
            normalized_targets.setdefault(key, set()).add(row.canonical_instrument)
        for row in rows:
            parsed = self.parse(provider, row.provider_symbol, exchange_hint=row.canonical_instrument.partition(".")[0], as_of=as_of, aliases=aliases)
            resolved = await self.resolve_raw(provider, row.provider_symbol, as_of=as_of)
            conflict = bool(parsed.canonical_instrument and parsed.canonical_instrument != row.canonical_instrument) or not resolved.resolved or len(normalized_targets[provider_symbol_key(provider, row.provider_symbol)]) > 1
            result.append({"status": "CONFLICT" if conflict else "OK" if parsed.canonical_instrument else "UNPARSEABLE",
                           "provider_symbol": row.provider_symbol, "explicit": row.canonical_instrument,
                           "parser": parsed.canonical_instrument, "reason": resolved.reason or parsed.reason})
        return result
