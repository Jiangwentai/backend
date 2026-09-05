from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import json
from typing import Any, Protocol

import asyncpg
import httpx

from instruments import ProviderInstrumentResolver
from instruments.postgres import PostgresInstrumentMetadata

from .errors import MappingError
from .models import HistoricalBar, ReferenceRecord


class InstrumentResolver(Protocol):
    async def resolve(self, provider_symbol: str, exchange: str | None = None, as_of=None) -> tuple[str, str]: ...
    async def provider_symbol(self, instrument_id: str) -> tuple[str, str, str]: ...


class HistoricalBarRepository(Protocol):
    async def upsert(self, bars: tuple[HistoricalBar, ...]) -> int: ...


class AkshareMetadataRepository:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: asyncpg.Pool | None = None
        self.instrument_metadata = PostgresInstrumentMetadata(self._ready)
        self.instrument_resolver = ProviderInstrumentResolver(self.instrument_metadata)
    async def start(self) -> None:
        if self.pool is None: self.pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=4)
    def _ready(self):
        if self.pool is None: raise RuntimeError("AKShare PostgreSQL repository is not started")
        return self.pool

    async def resolve_provider_instrument(self, provider_symbol: str, exchange: str | None = None, as_of=None, provider_source=None):
        return await self.instrument_resolver.resolve_raw("akshare", provider_symbol, exchange_hint=exchange, as_of=as_of,
                                                          provider_source=provider_source)

    async def resolve(self, provider_symbol: str, exchange: str | None = None, as_of=None) -> tuple[str, str]:
        """Compatibility wrapper returning the legacy (exchange, local code) tuple."""
        result = await self.resolve_provider_instrument(provider_symbol, exchange, as_of)
        if not result.resolved:
            raise MappingError(f"{result.reason}: {provider_symbol}")
        return result.exchange, result.canonical_instrument.partition(".")[2]

    async def provider_symbol(self, instrument_id: str, as_of=None, provider_source=None) -> tuple[str, str, str]:
        """Compatibility wrapper over explicit mapping then deterministic formatter."""
        result = await self.instrument_resolver.format_provider_symbol("akshare", instrument_id, as_of=as_of,
                                                                        provider_source=provider_source)
        if not result.resolved:
            raise MappingError(f"{result.reason}: {instrument_id}")
        exchange, _, instrument = result.canonical_instrument.partition(".")
        return exchange, instrument, result.provider_symbol

    async def begin_run(self, fetch_id: str, dataset: str, endpoint: str, parameters: dict[str, Any], version: str) -> None:
        await self._ready().execute("""INSERT INTO provider_ingestion_runs
          (id,provider_code,dataset,endpoint,status,request_parameters,provider_version,schema_version)
          VALUES($1,'akshare',$2,$3,'RUNNING',$4::jsonb,$5,1)""",
          fetch_id, dataset, endpoint, json.dumps(parameters, default=str), version)

    async def finish_run(self, fetch_id: str, *, status: str, received: int, normalized: int,
                         rejected: int, written: int, error: Exception | None = None) -> None:
        await self._ready().execute("""UPDATE provider_ingestion_runs SET completed_at=now(),status=$2,
          rows_received=$3,rows_normalized=$4,rows_rejected=$5,rows_written=$6,
          error_code=$7,error_message=$8 WHERE id=$1""", fetch_id, status, received,
          normalized, rejected, written, type(error).__name__ if error else None, str(error)[:2000] if error else None)

    async def unresolved(self, fetch_id: str, raw_symbol: str, normalized_symbol: str, endpoint: str, diagnostics=None) -> None:
        await self._ready().execute("""INSERT INTO provider_unresolved_instruments
          (provider_code,fetch_id,endpoint,raw_provider_symbol,normalized_provider_symbol,resolution_note)
          VALUES('akshare',$1,$2,$3,$4,$5) ON CONFLICT DO NOTHING""",
          fetch_id, endpoint, raw_symbol, normalized_symbol, json.dumps(diagnostics, default=str) if diagnostics else None)

    async def list_unresolved(self) -> list[dict[str, Any]]:
        rows = await self._ready().fetch("""SELECT fetch_id::text,endpoint,raw_provider_symbol,
          normalized_provider_symbol,resolution_note,first_seen_at FROM provider_unresolved_instruments
          WHERE provider_code='akshare' AND resolved_at IS NULL ORDER BY first_seen_at""")
        return [dict(row) for row in rows]

    async def upsert_reference(self, rows: tuple[ReferenceRecord, ...]) -> int:
        async with self._ready().acquire() as connection:
            async with connection.transaction():
                for row in rows:
                    await connection.execute("""INSERT INTO provider_reference_records
                      (provider_code,dataset,provider_key,payload,source,upstream_source,fetch_id,fetched_at)
                      VALUES('akshare',$1,$2,$3::jsonb,$4,$5,$6,$7)
                      ON CONFLICT(provider_code,dataset,provider_key) DO UPDATE SET
                      payload=EXCLUDED.payload,source=EXCLUDED.source,upstream_source=EXCLUDED.upstream_source,
                      fetch_id=EXCLUDED.fetch_id,fetched_at=EXCLUDED.fetched_at,updated_at=now()""",
                      row.dataset, row.provider_key, json.dumps(row.values), row.source,
                      row.upstream_source, row.fetch_id, row.fetched_at)
        return len(rows)

    async def record_versions(self, bars: tuple[HistoricalBar, ...]) -> int:
        revisions = 0
        async with self._ready().acquire() as connection:
            async with connection.transaction():
                for bar in bars:
                    values = {key: value for key, value in asdict(bar).items()
                              if key not in {"provider", "bar_start", "trading_day", "fetched_at", "fetch_id", "raw_provider_symbol", "instrument_kind"}}
                    values["provider"] = bar.provider.value
                    payload = json.dumps(values, sort_keys=True, default=str)
                    previous = await connection.fetchrow("""SELECT payload,fetch_id FROM historical_bar_versions
                      WHERE provider_code='akshare' AND provider_source=$1 AND instrument_id=$2 AND interval=$3 AND bar_start=$4""",
                      bar.upstream_source or "unknown",bar.instrument_id, bar.interval, bar.bar_start)
                    previous_payload = json.loads(previous["payload"]) if previous and isinstance(previous["payload"],str) else (previous["payload"] if previous else None)
                    if previous and previous_payload != json.loads(payload):
                        revisions += 1
                        await connection.execute("""INSERT INTO historical_bar_revisions
                          (provider_code,provider_source,instrument_id,interval,bar_start,previous_payload,new_payload,
                           previous_fetch_id,new_fetch_id) VALUES('akshare',$1,$2,$3,$4,$5,$6::jsonb,$7,$8)""",
                          bar.upstream_source or "unknown",bar.instrument_id, bar.interval, bar.bar_start, json.dumps(previous_payload), payload,
                          previous["fetch_id"], bar.fetch_id)
                    await connection.execute("""INSERT INTO historical_bar_versions
                      (provider_code,provider_source,instrument_id,interval,bar_start,payload,fetch_id,fetched_at)
                      VALUES('akshare',$1,$2,$3,$4,$5::jsonb,$6,$7)
                      ON CONFLICT(provider_code,provider_source,instrument_id,interval,bar_start) DO UPDATE SET
                      payload=EXCLUDED.payload,fetch_id=EXCLUDED.fetch_id,fetched_at=EXCLUDED.fetched_at,updated_at=now()""",
                      bar.upstream_source or "unknown",bar.instrument_id, bar.interval, bar.bar_start, payload, bar.fetch_id, bar.fetched_at)
        return revisions

    async def close(self) -> None:
        if self.pool: await self.pool.close(); self.pool = None


def _symbol(value: str) -> str:
    return value.replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")


def _field_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


class QuestDbHistoricalBarRepository:
    def __init__(self, base_url: str, timeout: float = 15.0): self.base_url = base_url.rstrip("/"); self.timeout = timeout
    async def upsert(self, bars: tuple[HistoricalBar, ...]) -> int:
        if not bars: return 0
        lines = []
        for bar in bars:
            tags = f"provider=AKSHARE,instrument_id={_symbol(bar.instrument_id)},interval={bar.interval},upstream_source={_symbol(bar.upstream_source or 'unknown')}"
            fields = [f"exchange={_field_string(bar.exchange)}", f"provider_symbol={_field_string(bar.provider_symbol)}",
                      f"raw_provider_symbol={_field_string(bar.raw_provider_symbol)}", f"instrument_kind={_field_string(bar.instrument_kind)}",
                      f"quality={_field_string(bar.quality)}",
                      f"trading_day={_field_string(bar.trading_day.isoformat())}", f"source={_field_string(bar.source)}",
                      f"fetch_id={_field_string(bar.fetch_id)}", f"fetched_at={int(bar.fetched_at.timestamp()*1_000_000)}t"]
            for name in ("open", "high", "low", "close", "settlement", "turnover"):
                value = getattr(bar, name)
                if value is not None: fields.append(f"{name}={value}")
            for name in ("volume", "open_interest"):
                value = getattr(bar, name)
                if value is not None: fields.append(f"{name}={value}i")
            lines.append(f"historical_bars,{tags} {','.join(fields)} {int(bar.bar_start.timestamp()*1_000_000)}t")
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(f"{self.base_url}/write", params={"precision": "us"}, content="\n".join(lines))
            response.raise_for_status()
        return len(bars)
