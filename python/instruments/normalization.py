from dataclasses import dataclass
import re

from .registry import EXCHANGES


@dataclass(frozen=True)
class NormalizedSymbol:
    symbol: str
    exchange: str | None


def normalize_provider_symbol(provider: str, raw_symbol: str, exchange_hint: str | None = None) -> NormalizedSymbol:
    if not isinstance(raw_symbol, str) or not raw_symbol.strip() or len(raw_symbol) > 128:
        raise ValueError("INVALID_SYMBOL")
    provider = provider.lower()
    symbol = raw_symbol.strip()
    # Case-folding is a documented rule only for these providers. Unknown
    # providers retain exact identity until a dialect/explicit mapping is supplied.
    if provider in {"akshare", "ctp", "synthetic"}:
        symbol = symbol.upper()
    hint = exchange_hint.strip().upper() if exchange_hint else None
    if provider in {"akshare", "ctp", "synthetic"}:
        for exchange in sorted(EXCHANGES, key=len, reverse=True):
            if any(symbol.startswith(exchange + separator) for separator in "._-"):
                if hint and hint != exchange:
                    raise ValueError("EXCHANGE_HINT_CONFLICT")
                hint, symbol = exchange, symbol[len(exchange) + 1:]
                break
        # Existing AKShare suffix convention remains accepted.
        for exchange in EXCHANGES:
            if symbol.endswith("." + exchange):
                if hint and hint != exchange:
                    raise ValueError("EXCHANGE_HINT_CONFLICT")
                hint, symbol = exchange, symbol[:-len(exchange) - 1]
                break
    if provider == "akshare":
        match = re.fullmatch(r"([A-Z]+)[._-]?([0-9]{1,4})", symbol)
        reverse = re.fullmatch(r"([0-9]{3,4})([A-Z]+)", symbol)
        if match:
            symbol = "".join(match.groups())
        elif reverse:
            symbol = reverse[2] + reverse[1]
    return NormalizedSymbol(symbol, hint)


def provider_symbol_key(provider: str, raw_symbol: str) -> str:
    return normalize_provider_symbol(provider, raw_symbol).symbol
