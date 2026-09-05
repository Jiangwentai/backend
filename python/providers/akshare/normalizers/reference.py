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
