# Canonical market-data model

## Identity and provenance

Every canonical event has an `EventHeader` containing provider, event type, quality, canonical instrument ID, exchange/instrument symbols, event and receive timestamps, producer UUID, and process-global sequence. The pair `(producer_id, seq)` is stable across local retries; provider is included in downstream keys and queries.

`QuoteSnapshot` is the compatible evolution of the former `MarketTick`; `MarketTick` remains a type alias so established quote code and fields keep compiling. Reserved `MarketEvent` variants are:

- `QuoteSnapshot`
- `TradeTick`
- `BidAskTick`
- `DepthUpdate`
- `BarEvent`

Only quote snapshots are accepted by the current persistence and live sinks.

## QuestDB

The existing `ctp_market_data` table is retained for operational compatibility. Phase 9 adds `provider`, `event_type`, `instrument_id`, and `quality` symbol columns. Replay identity is:

```text
(event_ts, provider, producer_id, seq)
```

Migrations `002` through `006` add these fields and the provider-aware DEDUP key to existing installations.

## Live schema

MessagePack schema version 2 adds the four canonical metadata fields. C++ and Python decoders accept v1 and supply `CTP`, `QUOTE_SNAPSHOT`, `<exchange>.<instrument>`, and `UNKNOWN` defaults. The ZeroMQ topic remains `<exchange>.<instrument>`; provider is part of the payload and cache identity.

## PostgreSQL metadata

`providers` stores the provider registry and enabled defaults. `provider_instruments` maps provider-native symbols to physical futures contracts with validity bounds and JSON metadata. It references the Phase 5 contract catalog rather than duplicating instrument definitions.

## Archive compatibility

New Parquet partitions use schema version 2 and contain the canonical metadata. Readers union columns by name and supply defaults when reading schema-v1 archives. Existing `ctp/` path layout is preserved; the manifest records represented providers.
