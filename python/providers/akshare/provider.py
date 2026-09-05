from __future__ import annotations

from datetime import datetime, timezone
import logging
import uuid

from .client import AkshareClient
from .errors import MappingError, SchemaError, ValidationError
from .endpoints import (FuturesContractReferenceAdapter, FuturesDailyAdapter,
                        FuturesForeignDailyAdapter, FuturesForeignDailyEastmoneyAdapter,
                        FuturesMinuteBarAdapter)
from .health import HealthTracker
from .models import (HistoricalBarRequest, HistoricalDataBatch, ProviderCapabilities,
                     ReferenceDataBatch, ReferenceDataRequest)
from .raw_archive import RawArchiveRepository
from .registry import endpoint
from instruments import ProviderInstrumentResolver, InstrumentKind
from instruments.registry import DOMESTIC_EXCHANGES
from instruments.normalization import provider_symbol_key

logger = logging.getLogger(__name__)


class AkshareProvider:
    def __init__(self, client: AkshareClient, resolver: ProviderInstrumentResolver,
                 raw_archive: RawArchiveRepository):
        self.client = client; self.resolver = resolver; self.raw_archive = raw_archive
        self.client.metrics.instrument_resolution = resolver.metrics
        self.health_tracker = HealthTracker()
        self._daily=FuturesDailyAdapter(client)
        self._foreign_daily=FuturesForeignDailyAdapter(client)
        self._foreign_daily_eastmoney=FuturesForeignDailyEastmoneyAdapter(client)
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
        parameters = {"symbol": request.provider_symbol,"trigger":request.trigger,
                      "acquisition_request_id":request.acquisition_request_id}
        adapter = (self._minute if request.endpoint == "futures_1m_sina" else
                   self._foreign_daily_eastmoney if request.endpoint == "futures_foreign_daily_eastmoney" else
                   self._foreign_daily if request.endpoint == "futures_foreign_daily_sina" else self._daily)
        if request.endpoint == "futures_1m_sina": parameters["period"] = "1"
        lineage = self._lineage(definition, fetch_id, fetched_at, parameters)
        try:
            resolution = await self.resolver.resolve_raw("akshare", request.provider_symbol,
                exchange_hint=request.exchange, as_of=request.end or request.start,
                provider_source=definition.upstream_source)
            native_symbol = request.provider_symbol
            if resolution.resolved:
                if request.endpoint in {"futures_foreign_daily_sina","futures_foreign_daily_eastmoney"}:
                    if resolution.exchange in DOMESTIC_EXCHANGES:
                        raise MappingError("UNSUPPORTED_ENDPOINT_INSTRUMENT: foreign Sina endpoint only")
                elif resolution.exchange not in DOMESTIC_EXCHANGES or resolution.kind not in {InstrumentKind.PHYSICAL_FUTURE, InstrumentKind.CONTINUOUS_FUTURE}:
                    raise MappingError("UNSUPPORTED_ENDPOINT_INSTRUMENT: domestic Sina bar endpoints only")
                if resolution.explicit_mapping:
                    # Exact spelling is retained; normalized aliases resolve to
                    # the configured native spelling rather than a guessed one.
                    if resolution.method == "NORMALIZED_EXPLICIT_MAPPING":
                        reverse = await self.resolver.format_provider_symbol("akshare", resolution.canonical_instrument,
                            as_of=request.end or request.start,provider_source=definition.upstream_source)
                        if not reverse.resolved:
                            raise MappingError(reverse.reason)
                        native_symbol = reverse.provider_symbol
                else:
                    reverse = await self.resolver.format_provider_symbol("akshare", resolution.canonical_instrument,
                        as_of=request.end or request.start,provider_source=definition.upstream_source)
                    if not reverse.resolved:
                        raise MappingError(reverse.reason)
                    native_symbol = reverse.provider_symbol
            parameters["symbol"] = native_symbol
            lineage["raw_provider_symbol"] = request.provider_symbol
            lineage["instrument_resolution"] = {
                "canonical_instrument": resolution.canonical_instrument,
                "raw_symbol": resolution.raw_symbol, "normalized_symbol": resolution.normalized_symbol,
                "method": resolution.method, "kind": resolution.kind.value,
                "metadata_registered": resolution.metadata_registered, "reason": resolution.reason,
                "exchange_hint": request.exchange, "candidate_product": resolution.product,
                "candidate_contract_code": resolution.contract_code,
            }
            rows = await adapter.fetch_native(native_symbol)
            archive = self.raw_archive.write(dataset=definition.name, fetch_id=fetch_id, rows=rows, lineage=lineage)
            self.client.metrics.rows_received_total[definition.name] += len(rows)
            normalized_symbol = provider_symbol_key("akshare", request.provider_symbol)
            if not resolution.resolved:
                self.client.metrics.mapping_errors_total += 1
                self.client.metrics.rows_rejected_total[definition.name] += len(rows)
                self.health_tracker.failure(MappingError(resolution.reason))
                return HistoricalDataBatch(fetch_id, definition.name, (), len(rows), len(rows), str(archive), lineage, (normalized_symbol,))
            exchange, instrument_id = resolution.exchange, resolution.canonical_instrument
            bars = adapter.normalize(rows,raw_symbol=request.provider_symbol,exchange=exchange,
                                         instrument_id=instrument_id,fetch_id=fetch_id,fetched_at=fetched_at)
            from dataclasses import replace
            bars = tuple(replace(bar, provider_symbol=native_symbol, instrument_kind=resolution.kind.value) for bar in bars)
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
