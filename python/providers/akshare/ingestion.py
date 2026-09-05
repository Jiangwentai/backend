from __future__ import annotations

import uuid

from .models import HistoricalBarRequest, ReferenceDataRequest
from .registry import endpoint


class AkshareIngestionService:
    """Owns storage routing; the provider only acquires and normalizes batches."""
    def __init__(self, provider, metadata, bars): self.provider=provider; self.metadata=metadata; self.bars=bars

    async def ingest_bars(self, request: HistoricalBarRequest):
        definition=endpoint(request.endpoint);fetch_id=str(uuid.uuid4());metrics=self.provider.client.metrics
        await self.metadata.begin_run(fetch_id,definition.name,definition.function_name,
                                      {"symbol":request.provider_symbol,"trigger":request.trigger,
                                       "acquisition_request_id":request.acquisition_request_id},self.provider.client.version)
        metrics.ingestion_runs_total += 1
        try:
            batch = await self.provider.fetch_bars(request,fetch_id=fetch_id)
            for symbol in batch.unresolved_symbols:
                await self.metadata.unresolved(batch.fetch_id, request.provider_symbol, symbol, batch.endpoint,
                                               batch.lineage.get("instrument_resolution"))
            if batch.unresolved_symbols:
                await self.metadata.finish_run(batch.fetch_id,status="PARTIAL",received=batch.rows_received,
                    normalized=0,rejected=batch.rows_rejected,written=0)
                return batch, 0, 0
            revisions = await self.metadata.record_versions(batch.rows)
            written = await self.bars.upsert(batch.rows)
            metrics.historical_revisions_total += revisions;metrics.historical_rows_written_total += written
            await self.metadata.finish_run(batch.fetch_id,status="SUCCESS",received=batch.rows_received,
                normalized=len(batch.rows),rejected=batch.rows_rejected,written=written)
            return batch, written, revisions
        except Exception as exc:
            metrics.ingestion_failures_total += 1
            received=batch.rows_received if "batch" in locals() else 0;normalized=len(batch.rows) if "batch" in locals() else 0;rejected=batch.rows_rejected if "batch" in locals() else 0
            await self.metadata.finish_run(fetch_id,status="FAILED",received=received,
                normalized=normalized,rejected=rejected,written=0,error=exc)
            raise

    async def ingest_reference(self, request: ReferenceDataRequest):
        definition=endpoint(request.endpoint);fetch_id=str(uuid.uuid4());metrics=self.provider.client.metrics
        await self.metadata.begin_run(fetch_id,definition.name,definition.function_name,
                                      request.parameters,self.provider.client.version)
        metrics.ingestion_runs_total += 1
        try:
            batch = await self.provider.fetch_reference(request,fetch_id=fetch_id)
            written = await self.metadata.upsert_reference(batch.rows)
            await self.metadata.finish_run(batch.fetch_id,status="SUCCESS",received=len(batch.rows),
                normalized=len(batch.rows),rejected=0,written=written)
            return batch, written
        except Exception as exc:
            metrics.ingestion_failures_total += 1
            count=len(batch.rows) if "batch" in locals() else 0
            await self.metadata.finish_run(fetch_id,status="FAILED",received=count,
                normalized=count,rejected=0,written=0,error=exc)
            raise
