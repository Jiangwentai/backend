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

## Historical provider bars

AKShare daily and 1-minute bars use `(provider, instrument_id, interval, bar_start)` as canonical identity. They preserve provider symbol, exchange, trading day, OHLC, nullable optional values, upstream source, source function, fetch time, and fetch ID. This is intentionally separate from realtime `(provider, producer_id, seq, event_ts)` identity. A `QuoteSnapshot` is a polled point-in-time observation; it is neither a completed `HistoricalBar` nor proof of a `TradeTick`.

The latest accepted version is stored in QuestDB `historical_bars` and PostgreSQL `historical_bar_versions`. Changed repeats create immutable `historical_bar_revisions` records. Every fetched source version remains in raw Parquet.

Historical observations also carry a nullable/configured quality classification. Coverage is range-specific derived state, not stored truth. SINGLE and COMPOSITE results are read projections; their provider-aware source rows and DEDUP identity are unchanged.

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


## Shared instrument resolution

Canonical identity and metadata registration are separate. The provider-independent `python/instruments` package resolves exact explicit mappings, normalized explicit mappings, and deterministic provider/exchange rules, then enriches metadata. Physical futures, provider continuous series, and rolling tenors retain distinct kinds; no delivery month is fabricated for LME 3M. AKShare historical and quote workers use this boundary, and quote association is by symbol identity. See [instrument rules and compatibility](instruments.md).

Existing PostgreSQL mappings remain valid; typed aliases use existing provider/reference JSON metadata, without new PostgreSQL tables. QuestDB migrations 008–014 add nullable provenance fields and leave DEDUP keys unchanged. Historical canonical writes use full IDs; read compatibility accepts legacy local IDs. New Parquet archives use schema v3 and preserve provider/native/raw identity; existing completed archives remain immutable. The optional Python live writer preserves nanosecond receive time with the correct ILP field suffix. CTP callbacks and C++ data planes are unchanged.
