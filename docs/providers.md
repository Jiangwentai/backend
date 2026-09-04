# Provider architecture

Phase 9 makes CTP one implementation of a market-data provider rather than a system-wide special case.

## Boundaries

- `IRealtimeMarketDataProvider` defines lifecycle, subscriptions, capabilities, health, and an event-sink boundary.
- `IHistoricalDataProvider` and `IReferenceDataProvider` are separate interfaces so a provider does not advertise unsupported behavior.
- `ProviderManager` rejects duplicate provider IDs, starts providers with rollback on failure, stops them in reverse order, and routes subscriptions explicitly.
- `IMarketEventSink` accepts canonical `MarketEvent` values. The current queue sink accepts quote snapshots and visibly rejects unsupported future variants.

`ProviderId`, `ProviderState`, `ProviderCapabilities`, and `ProviderHealth` are strong provider-level concepts. CTP and Synthetic implement the realtime interface. IBKR and AKShare are identifiers reserved for later phases, not implemented connectors.

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

Non-quote canonical event variants are defined to prevent another core redesign, but Phase 9 downstream persistence and live publication support quote snapshots only.
