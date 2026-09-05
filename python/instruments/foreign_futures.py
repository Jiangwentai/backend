from dataclasses import replace
from datetime import date
import re

from .kinds import InstrumentKind as Kind
from .models import ParsedInstrument
from .month_codes import expand_yymm, month_code_to_month
from .normalization import normalize_provider_symbol
from .registry import FOREIGN_ROOTS, PRODUCT_ALIASES


def alias_canonical(alias) -> str | None:
    if not alias.exchange or not alias.product:
        return None
    suffix = {Kind.CONTINUOUS_FUTURE: "continuous", Kind.INDEX: "index", Kind.SPOT: "spot",
              Kind.CFD: "cfd", Kind.SYNTHETIC: "synthetic"}.get(alias.kind)
    if alias.kind == Kind.ROLLING_TENOR and alias.tenor and re.fullmatch(r"P[1-9][0-9]*[DMY]", alias.tenor):
        suffix = alias.tenor[1:].lower()
    return f"{alias.exchange}.{alias.product}.{suffix}" if suffix else None


def parse_foreign_future(provider: str, raw_symbol: str, *, exchange_hint: str | None = None,
                         as_of: date | None = None, roots=FOREIGN_ROOTS, aliases=PRODUCT_ALIASES) -> ParsedInstrument:
    try:
        normalized = normalize_provider_symbol(provider, raw_symbol, exchange_hint)
    except ValueError as exc:
        return ParsedInstrument(provider, raw_symbol, raw_symbol, reason=str(exc))
    base = ParsedInstrument(provider, raw_symbol, normalized.symbol, exchange=normalized.exchange)
    matches = [alias for alias in aliases if alias.provider == provider.lower() and alias.provider_symbol == normalized.symbol]
    if len(matches) > 1:
        return replace(base, reason="AMBIGUOUS_PRODUCT_ALIAS")
    if matches:
        alias = matches[0]
        if normalized.exchange and alias.exchange != normalized.exchange:
            return replace(base, reason="PRODUCT_EXCHANGE_MISMATCH")
        canonical = alias_canonical(alias)
        return replace(base, kind=alias.kind, exchange=alias.exchange, product=alias.product,
                       tenor=alias.tenor, canonical_instrument=canonical,
                       method="AKSHARE_FOREIGN_ALIAS" if canonical and provider.lower() == "akshare" else "ROLLING_TENOR_RULE" if canonical else "UNRESOLVED",
                       reason=None if canonical else "UNDEFINED_PRODUCT_SEMANTICS")
    match = re.fullmatch(r"([A-Z]+)([0-9]{2})([FGHJKMNQUVXZ])", normalized.symbol)
    if not match:
        return replace(base, reason="UNRECOGNIZED_FOREIGN_SYMBOL")
    definitions = [root for root in roots if root.provider == provider.lower() and root.provider_root == match[1]]
    if normalized.exchange:
        definitions = [root for root in definitions if root.canonical_exchange == normalized.exchange]
    if len(definitions) != 1:
        return replace(base, product=match[1].lower(), reason="UNKNOWN_OR_AMBIGUOUS_FOREIGN_ROOT")
    root = definitions[0]
    try:
        delivery = expand_yymm(match[2] + f"{month_code_to_month(match[3]):02d}", as_of=as_of)
    except ValueError as exc:
        return replace(base, reason=str(exc))
    return replace(base, kind=Kind.PHYSICAL_FUTURE, exchange=root.canonical_exchange,
                   product=root.canonical_product, delivery_month=delivery, contract_code=match[2] + match[3],
                   canonical_instrument=f"{root.canonical_exchange}.{root.canonical_product}{delivery[2:]}",
                   method="FOREIGN_PHYSICAL_MONTH_CODE")
