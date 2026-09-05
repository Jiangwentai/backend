from dataclasses import replace
from datetime import date
import re

from .kinds import InstrumentKind as Kind
from .models import ParsedInstrument
from .month_codes import expand_czce_ymm, expand_yymm
from .normalization import normalize_provider_symbol
from .registry import DOMESTIC_PRODUCTS


def parse_chinese_future(raw_symbol: str, exchange: str | None = None, *,
                         as_of: date | None = None, provider: str = "akshare") -> ParsedInstrument:
    try:
        normalized = normalize_provider_symbol(provider, raw_symbol, exchange)
    except ValueError as exc:
        return ParsedInstrument(provider, raw_symbol, raw_symbol, reason=str(exc))
    base = ParsedInstrument(provider, raw_symbol, normalized.symbol, exchange=normalized.exchange)
    match = re.fullmatch(r"([A-Za-z]+)([0-9]{1,4})", normalized.symbol)
    if not match:
        return replace(base, reason="UNRECOGNIZED_DOMESTIC_SYMBOL")
    product, code = match[1].upper(), match[2]
    venues = [venue for venue, roots in DOMESTIC_PRODUCTS.items() if product in roots]
    base = replace(base, product=product.lower(), contract_code=code)
    if not venues:
        return replace(base, reason="UNKNOWN_PRODUCT")
    exchange = normalized.exchange or (venues[0] if len(venues) == 1 else None)
    if exchange not in venues:
        return replace(base, reason="PRODUCT_EXCHANGE_MISMATCH" if exchange else "EXCHANGE_REQUIRED")
    base = replace(base, exchange=exchange)
    if code in {"0", "00", "888", "999"}:
        # Only Sina's documented zero main-series suffix is canonicalized.
        if provider.lower() == "akshare" and code == "0":
            return replace(base, kind=Kind.CONTINUOUS_FUTURE, contract_code=None,
                           canonical_instrument=f"{exchange}.{product.lower()}.continuous", method="CONTINUOUS_RULE")
        return replace(base, kind=Kind.SYNTHETIC, reason="UNDEFINED_CONTINUOUS_CONVENTION")
    try:
        delivery = expand_czce_ymm(code, as_of=as_of) if exchange == "CZCE" and len(code) == 3 else expand_yymm(code, as_of=as_of)
    except ValueError as exc:
        return replace(base, reason=str(exc))
    return replace(base, kind=Kind.PHYSICAL_FUTURE, delivery_month=delivery,
                   canonical_instrument=f"{exchange}.{product.lower()}{delivery[2:]}", method="CHINESE_PHYSICAL_RULE")
