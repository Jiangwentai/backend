from __future__ import annotations

from datetime import datetime
from typing import Any

from ..models import ProviderId, ReferenceRecord
from ..normalizers.futures_daily import validate_schema
from ..registry import EndpointDefinition


def normalize_contract_reference(rows: list[dict[str, Any]], *, definition: EndpointDefinition,
                                 fetch_id: str, fetched_at: datetime) -> tuple[ReferenceRecord, ...]:
    validate_schema(rows, definition); result = []
    for row in rows:
        symbol = str(row.get("symbol",row.get("合约代码",""))).strip().upper()
        values = {str(key): None if value is None else str(value) for key, value in row.items()}
        result.append(ReferenceRecord(ProviderId.AKSHARE, definition.name, symbol, values,
                                      definition.function_name, definition.upstream_source, fetch_id, fetched_at))
    return tuple(result)


def normalize_foreign_product_reference(rows, *, definition, fetch_id, fetched_at):
    from dataclasses import asdict
    from instruments.kinds import InstrumentKind
    from instruments.registry import PRODUCT_ALIASES, AKSHARE_REFERENCE_NAMES
    from ..errors import SchemaError
    validate_schema(rows, definition)
    result = []
    seen = set()
    aliases = {value.provider_symbol: value for value in PRODUCT_ALIASES if value.provider == "akshare"}
    for row in rows:
        code, name = row["code"], row["symbol"]
        if not isinstance(code, str) or not code.strip() or not isinstance(name, str):
            raise SchemaError("INVALID_FOREIGN_REFERENCE_IDENTITY")
        key = code.strip().upper()
        if key in seen:
            raise SchemaError(f"DUPLICATE_FOREIGN_REFERENCE_CODE: {key}")
        seen.add(key)
        alias = aliases.get(key)
        if alias and name.strip() != AKSHARE_REFERENCE_NAMES[key]:
            raise SchemaError(f"FOREIGN_REFERENCE_SEMANTICS_CHANGED: {key}")
        # Preserve unclassified rows for research; never promote an unfamiliar
        # product name into a delivery-month contract or a known alias.
        values = {"raw_provider_symbol": code, "name": name,
                  "instrument_kind": alias.kind.value if alias else InstrumentKind.UNKNOWN.value,
                  "definition": asdict(alias) if alias else None}
        result.append(ReferenceRecord(ProviderId.AKSHARE, definition.name, key, values,
                                     definition.function_name, definition.upstream_source, fetch_id, fetched_at))
    return tuple(result)
