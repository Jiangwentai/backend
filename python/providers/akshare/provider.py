from __future__ import annotations

from datetime import datetime, timezone
import logging
import uuid

from .client import AkshareClient
from .errors import MappingError, SchemaError, ValidationError
from .endpoints import FuturesContractReferenceAdapter, FuturesDailyAdapter, FuturesMinuteBarAdapter
from .health import HealthTracker
from .models import (HistoricalBarRequest, HistoricalDataBatch, ProviderCapabilities,
                     ReferenceDataBatch, ReferenceDataRequest)
from .normalizers import normalize_symbol
from .raw_archive import RawArchiveRepository
from .registry import endpoint
from .repositories import InstrumentResolver

logger = logging.getLogger(__name__)


class AkshareProvider:
    def __init__(self, client: AkshareClient, resolver: InstrumentResolver,
                 raw_archive: RawArchiveRepository):
        self.client = client; self.resolver = resolver; self.raw_archive = raw_archive
        self.health_tracker = HealthTracker()
        self._daily=FuturesDailyAdapter(client)
        self._minute=FuturesMinuteBarAdapter(client)

    @property
    def capabilities(self) -> ProviderCapabilities: return ProviderCapabilities(best_effort_quotes=True)
    @property
    def health(self): return self.health_tracker.snapshot()

    def _lineage(self, definition, fetch_id: str, fetched_at: datetime, parameters: dict) -> dict:
        return {"provider": "AKSHARE", "function_name": definition.function_name,
                "endpoint": definition.name, "upstream_source": definition.upstream_source,
                "request_parameters": parameters, "fetch_id": fetch_id,
                "fetched_at": fetched_at.isoformat(), "provider_version": self.client.version,
                "normalizer_schema_version": 1}

    async def fetch_bars(self, request: HistoricalBarRequest, *, fetch_id: str | None = None) -> HistoricalDataBatch:
        definition = endpoint(request.endpoint); fetch_id = fetch_id or str(uuid.uuid4()); fetched_at = datetime.now(timezone.utc)
        parameters = {"symbol": request.provider_symbol}
        adapter = self._minute if request.endpoint == "futures_1m_sina" else self._daily
        if request.endpoint == "futures_1m_sina": parameters["period"] = "1"
        lineage = self._lineage(definition, fetch_id, fetched_at, parameters)
        try:
            rows = await adapter.fetch_native(request.provider_symbol)
            archive = self.raw_archive.write(dataset=definition.name, fetch_id=fetch_id, rows=rows, lineage=lineage)
            self.client.metrics.rows_received_total[definition.name] += len(rows)
            normalized_symbol = normalize_symbol(request.provider_symbol)
            try: exchange, instrument_id = await self.resolver.resolve(normalized_symbol, request.exchange, request.end or request.start)
            except MappingError:
                self.client.metrics.mapping_errors_total += 1; self.client.metrics.rows_rejected_total[definition.name] += len(rows); self.health_tracker.failure(MappingError(normalized_symbol))
                return HistoricalDataBatch(fetch_id, definition.name, (), len(rows), len(rows), str(archive), lineage, (normalized_symbol,))
            bars = adapter.normalize(rows,raw_symbol=request.provider_symbol,exchange=exchange,
                                         instrument_id=instrument_id,fetch_id=fetch_id,fetched_at=fetched_at)
            bars = tuple(bar for bar in bars if (request.start is None or bar.trading_day >= request.start)
                         and (request.end is None or bar.trading_day <= request.end))
            if request.endpoint == "futures_1m_sina":
                lineage["upstream_range_supported"] = False
                lineage["coverage_complete"] = request.start is None and request.end is None
                lineage["known_limit"] = "Sina endpoint returns a bounded recent window and accepts no date range"
            self.client.metrics.rows_normalized_total[definition.name] += len(bars); self.health_tracker.success()
            logger.info("provider=AKSHARE dataset=%s endpoint=%s fetch_id=%s row_count=%d status=SUCCESS",
                        definition.name, definition.function_name, fetch_id, len(bars))
            return HistoricalDataBatch(fetch_id, definition.name, bars, len(rows), 0, str(archive), lineage)
        except (SchemaError,ValidationError) as exc:
            if isinstance(exc,SchemaError):self.client.metrics.schema_errors_total += 1
            self.client.metrics.rows_rejected_total[definition.name] += len(rows) if "rows" in locals() else 0
            self.health_tracker.failure(exc);raise
        except Exception as exc:
            self.health_tracker.failure(exc); raise

    async def fetch_reference(self, request: ReferenceDataRequest, *, fetch_id: str | None = None) -> ReferenceDataBatch:
        definition = endpoint(request.endpoint); fetch_id = fetch_id or str(uuid.uuid4()); fetched_at = datetime.now(timezone.utc)
        reference=FuturesContractReferenceAdapter(self.client,definition)
        lineage = self._lineage(definition, fetch_id, fetched_at, request.parameters)
        try:
            rows = await reference.fetch_native(request.parameters)
            archive = self.raw_archive.write(dataset=definition.name, fetch_id=fetch_id, rows=rows, lineage=lineage)
            records = reference.normalize(rows,fetch_id=fetch_id,fetched_at=fetched_at)
            self.client.metrics.rows_received_total[definition.name] += len(rows)
            self.client.metrics.rows_normalized_total[definition.name] += len(records); self.health_tracker.success()
            return ReferenceDataBatch(fetch_id, definition.name, records, str(archive), lineage)
        except (SchemaError,ValidationError) as exc:
            if isinstance(exc,SchemaError):self.client.metrics.schema_errors_total += 1
            self.client.metrics.rows_rejected_total[definition.name] += len(rows) if "rows" in locals() else 0
            self.health_tracker.failure(exc);raise
        except Exception as exc:
            self.health_tracker.failure(exc); raise
