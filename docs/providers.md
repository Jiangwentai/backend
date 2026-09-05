# Provider architecture

Phase 9 makes CTP one implementation of a market-data provider rather than a system-wide special case.

## Boundaries

- `IRealtimeMarketDataProvider` defines lifecycle, subscriptions, capabilities, health, and an event-sink boundary.
- `IHistoricalDataProvider` and `IReferenceDataProvider` are separate interfaces so a provider does not advertise unsupported behavior.
- `ProviderManager` rejects duplicate provider IDs, starts providers with rollback on failure, stops them in reverse order, and routes subscriptions explicitly.
- `IMarketEventSink` accepts canonical `MarketEvent` values. The current queue sink accepts quote snapshots and visibly rejects unsupported future variants.

`ProviderId`, `ProviderState`, `ProviderCapabilities`, and `ProviderHealth` are strong provider-level concepts. CTP and Synthetic implement the authoritative realtime interface. Phase 11B gives AKShare historical, intraday, reference, and a distinct `best_effort_quotes` capability; `realtime_quotes`, market depth, and trade ticks remain false. IBKR remains reserved for its own phase.

## Ingress and threading

Each realtime provider owns a producer identity and a bounded SPSC ingress queue. The Dispatcher is the single consumer of every ingress queue and visits them round-robin before feeding the existing persistence and live planes. This preserves these invariants:

- provider callbacks do not perform database, filesystem, or live-network work;
- CTP callbacks do not acquire a shared MPSC lock;
- persistence backpressure cannot be hidden as silent loss;
- live congestion cannot block persistence;
- a provider cannot overwrite unread data from another provider.

The CTP SDK remains isolated under `cpp/src/ctp`; only its adapter includes proprietary structures. The adapter normalizes SDK callbacks and publishes canonical events. Synthetic follows the same lifecycle and sink path.

## Subscriptions and instruments

A `Subscription` names a provider, canonical instrument, provider symbol, and optional exchange. `InstrumentMappingRegistry` maps provider symbols to canonical IDs explicitly; implicit cross-provider symbol equality is not assumed. PostgreSQL `providers` and `provider_instruments` persist registry and time-bounded mapping metadata.

CTP configured symbols may include an exchange prefix. Because some fronts return an empty `ExchangeID`, that configured mapping is used as a fallback during normalization.

## Failure and health

Provider health is independent. A provider start failure rolls back already-started providers; a runtime provider failure is reported without inventing automatic failover or price arbitration. The API reports provider observations, while collector lifecycle logs and metrics remain authoritative for adapter state.

## Downstream compatibility

QuestDB, live cache, WebSocket coalescing, archive, DuckDB research, and quality checks include provider identity. Existing table names, CTP archive paths, quote fields, legacy `source`, and WebSocket subscription control remain compatible. MessagePack v1 is readable; writers emit v2.

Non-quote canonical event variants are defined to prevent another core redesign, but Phase 9 realtime persistence and live publication support quote snapshots only. AKShare historical bars use a separate semantic identity and `historical_bars` repository; polled AKShare observations are `QuoteSnapshot`, never `BarEvent` or reconstructed `TradeTick`.

See [AKShare historical/reference provider](providers/akshare.md) for endpoints, lineage, scheduling, revisions, and operations.

## Selection and failover

`PROVIDER_SELECTION_MODE=explicit` is the safe default: multiple observations require an explicit `provider=` query. `preferred` walks `PROVIDER_PREFERENCE`; `ranked` first compares declared quality and then provider preference. Both reject stale observations unless `PROVIDER_ALLOW_STALE=true`. Moving away from the first preferred provider additionally requires `PROVIDER_FALLBACK_ENABLED=true`. Responses identify `selection_reason`, `preferred_provider`, and `fallback`; no hidden fallback is permitted.

`GET /v1/provider-selection/{symbol}` reports current provider observations, age/staleness, selected provider, and maximum price discrepancy in basis points. It is diagnostic only: the system does not average prices or alter upstream events.

Historical selection is a separate read-side policy. Provider priorities and quality are configured independently from realtime selection. SINGLE keeps one provider for an entire range; COMPOSITE falls back only at complete-bar boundaries and preserves source provenance. See [historical coverage](historical-data.md).


## Shared instrument resolution

Canonical identity and metadata registration are separate. The provider-independent `python/instruments` package resolves exact explicit mappings, normalized explicit mappings, and deterministic provider/exchange rules, then enriches metadata. Physical futures, provider continuous series, and rolling tenors retain distinct kinds; no delivery month is fabricated for LME 3M. AKShare historical and quote workers use this boundary, and quote association is by symbol identity. See [instrument rules and compatibility](instruments.md).

Existing PostgreSQL mappings remain valid; typed aliases use existing provider/reference JSON metadata, without new PostgreSQL tables. QuestDB migrations 008–014 add nullable provenance fields and leave DEDUP keys unchanged. Historical canonical writes use full IDs; read compatibility accepts legacy local IDs. New Parquet archives use schema v3 and preserve provider/native/raw identity; existing completed archives remain immutable. The optional Python live writer preserves nanosecond receive time with the correct ILP field suffix. CTP callbacks and C++ data planes are unchanged.
