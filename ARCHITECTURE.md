# Architecture handoff

## Implemented system through Phase 12

```text
SyntheticProvider and/or CtpMarketDataAdapter
    -> canonical QuoteSnapshot / MarketEvent
    -> one bounded ingress SPSC queue per realtime provider
    -> Dispatcher
       |-> bounded lossless persistence SPSC queue
       |   -> dedicated QuestDB writer
       |   -> official QuestDB C/C++ QWP client
       |   -> disk Store-and-Forward
       |   -> QuestDB WAL + DEDUP
       `-> bounded freshness-first LiveQueue
           -> dedicated ZeroMQ PUB thread
           -> MessagePack v2 multipart frames
           -> Python async ZeroMQ SUB
           -> provider-aware LatestQuoteCache
              |-> ProviderSelector -> REST
              `-> bounded WebSocket connection manager
```

Phase 11/11B adds a separate historical plane:

```text
AKShare worker -> daily/1m endpoint adapters -> immutable raw Parquet
                              `------> canonical daily/1m bars -> QuestDB historical_bars
                              `------> reference/run/revision metadata -> PostgreSQL

AKShare quote poller -> QuoteSnapshot(BEST_EFFORT) -> shared live ingress
                    -> ZeroMQ PUB :5557 -> same FastAPI subscriber/cache
```

## Important decisions

- Persistence and live distribution are independent planes. Never turn ZeroMQ into persistence or QuestDB into a polled live bus.
- Every locally received event gets the process-stable `producer_id` and a global monotonic `seq` before ingress. QWP retries retain the same identity.
- QuestDB delivery is at least once. `DEDUP UPSERT KEYS(event_ts, provider, producer_id, seq)` collapses transport replay while preserving provider identity.
- CTP `TradingDay`, `ActionDay`, event time, and receive time are distinct. Never derive the trading day from the calendar date.
- Empty CTP `ActionDay` uses the existing Asia/Shanghai nearest-date rule with a 12-hour crossover boundary, then converts to UTC.
- CTP sentinel prices normalize to NaN internally and QuestDB NULL on persistence. Raw cumulative volume and turnover are not converted to deltas.
- Live delivery is best effort and freshness first. `LiveQueue` evicts only at actual capacity. WebSocket clients have independent bounded latest-per-provider-and-symbol buffers.
- Provider-native APIs stop at adapters. Realtime, historical, and reference capabilities use segregated interfaces; `ProviderManager` owns lifecycle, subscription routing, capabilities, and health.
- Synthetic and CTP may run concurrently. Each provider has a dedicated producer identity and SPSC queue; the Dispatcher round-robins all registered ingress queues.
- The canonical model reserves quote, trade, bid/ask, depth, and bar variants. Phase 9 providers and downstream sinks currently emit/accept quote snapshots only.
- AKShare is an optional Python-only historical/reference and best-effort quote provider. History remains isolated; a separate bounded quote poller emits `QuoteSnapshot` through the common live ingress and an independent ZeroMQ endpoint. It never runs on CTP callbacks, claims authoritative realtime, or synthesizes trades. It can be selected as fallback only by an explicitly enabled Phase 12 policy.
- FastAPI recovery subscribes to ZeroMQ before querying QuestDB; both live and recovery ticks pass through the same cache conflict resolver.
- FastAPI V1 runs exactly one worker because cache, connection registry, and metrics are process-local.
- Phase 12 adds a read-side `ProviderSelector` after the provider-aware cache. Explicit provider reads bypass arbitration; provider-omitted reads use the configured policy. Selection never overwrites source observations or changes persistence and fallback defaults off.
- Live transport HWM values are bounded and configurable: C++ `live.sndhwm` / `ZMQ_SNDHWM`, FastAPI `ZMQ_RCVHWM`, both defaulting to 1000 pending messages per peer and applied before bind/connect. Buffer tuning trades burst tolerance against memory and quote age; it does not provide subscriber delivery guarantees.
- C++ ZeroMQ publication uses pinned cppzmq `send_multipart(..., dontwait)` for the topic/body pair. A send exception terminates that publisher socket lifecycle; it never continues with potentially uncertain multipart state. Normal PUB/HWM loss remains best effort and is not observable as a failed send.

## Module responsibilities

- `cpp/include/market_data/market_tick.hpp`, `cpp/src/market_tick.cpp`: provider-neutral data model, fixed strings, numeric normalization, CTP-compatible timestamp normalization.
- `spsc_queue.hpp`: bounded single-producer/single-consumer lossless queue with metrics; never overwrites unread data.
- `provider.*`, `realtime_provider.hpp`, `provider_manager.*`, `event_sink.hpp`: provider boundaries, lifecycle, health, capabilities, subscriptions, and canonical event publication.
- `instrument_mapping.*`: explicit provider-symbol to canonical-instrument mapping boundary.
- `dispatcher.*`: sole consumer of all provider ingress queues and fan-out owner. Persistence is enqueued before best-effort live delivery.
- `questdb_writer.*`: dedicated QWP batching, Store-and-Forward acceptance, shutdown ACK.
- `live_queue.hpp`, `zmq_publisher.*`, `live_protocol.*`: bounded C++ live path, non-blocking multipart publication, and MessagePack v2 wire contract with v1 decode compatibility.
- `synthetic_generator.*`: synthetic realtime provider and CI/development fixture.
- `pipeline.*`: thread/object ownership and ordered startup/shutdown.
- `python/live`: multi-endpoint ZeroMQ subscriber, provider-aware latest cache, shared Python live ingress/publisher/persistence transports, and read-side provider selector.
- `python/api`: FastAPI models, lifespan, REST/WS, health, recovery repository, and WebSocket backpressure.
- `python/api/postgres_repository.py`: async PostgreSQL reference-metadata query boundary used by `/v1/instruments`.
- `sql/postgresql/002_reference_metadata.sql`, `003_providers.sql`: reference metadata plus provider registry and provider-instrument validity mappings.
- `python/archive`: offline paged QuestDB reader, explicit Arrow schema, immutable ZSTD Parquet writer, verification manifest, and CLI.
- `python/research`: read-only DuckDB scans over verified Parquet, trading-day-aware OHLCV derivation, explicit continuous-contract resolution, and CLI.
- `python/quality`: read-only verified-archive integrity and anomaly checks with machine-readable reports and CI exit status.
- `python/providers/akshare`: optional client seam, daily/1m/reference/quote adapters, canonical historical batches and `QuoteSnapshot`, immutable raw archive, repositories, scheduler/backfill, quote poller, health, metrics, and CLI.
- `python/api/metrics.py`: Prometheus-compatible API/live/WebSocket/dependency metrics; collector and QuestDB metrics remain in their owning processes.
- `sql/questdb/001_ctp_market_data.sql`, `002`–`006`: WAL/DEDUP snapshot schema and additive provider/event/instrument/quality migrations.
- `sql/questdb/007_historical_bars.sql`: provider-aware canonical historical bars with semantic DEDUP identity.
- `sql/postgresql/004_akshare.sql`: ingestion runs, unresolved symbols, reference records, latest versions, and revision audit.
- `cpp/include/market_data/ctp` and `cpp/src/ctp`: SDK-independent normalization/session logic plus the SDK-gated MdApi/Spi adapter. Only `adapter.cpp` includes proprietary headers.

## CTP constraints and operator verification

- `CThostFtdc*` structures must stay inside the CTP adapter implementation. Other modules receive only `MarketTick`.
- `OnRtnDepthMarketData` must do no database, network, filesystem, sleep, blocking lock, heavy log, or retry work.
- Every provider ingress queue is SPSC. One MdApi instance's market-data callback path must be the only producer for the CTP queue.
- Queue-full behavior cannot block inside the callback. It must increment visible failure/health metrics; silent loss is prohibited.
- Maintain an explicit session state: DISCONNECTED, CONNECTING, CONNECTED, AUTHENTICATING, AUTHENTICATED, LOGGING_IN, LOGGED_IN, SUBSCRIBING, READY, RECONNECTING, ERROR.
- Desired subscriptions survive disconnect; active subscriptions are rebuilt after authentication/login on reconnect.
- SDK support must default off. Enabling it requires an externally supplied, operator-pinned SDK root and a clear configure-time error if missing.
- Never log passwords, auth codes, or other credentials.
- Shutdown must first stop MdApi and callbacks, then reuse the existing ingress/persistence/live drain ordering.
- Legacy `source` selection remains compatible; `providers.synthetic.enabled` and `providers.ctp.enabled` permit concurrent operation. Builds default to `ENABLE_CTP=OFF`; enabling CTP requires an operator-supplied `CTP_SDK_ROOT`.
- The locally supplied Linux x86-64 v6.7.13 market-data SDK compiles with `ENABLE_CTP=ON`. It does not expose `ReqAuthenticate`, so authentication is compiled out for this package. Callback serialization must still be confirmed from its documentation, and a credentialed live-front smoke test remains required before production deployment.

## Important constraints and unfinished validation

- The local `ctp_file/` directory is operator-supplied and Git-ignored. Do not vendor or expose its proprietary files.
- A broker test front has reached CTP `READY`; an active market-hours test must still prove tick flow through QuestDB and REST/WebSocket.
- Standard PUB is freshness-first and may silently drop at HWM. `messages_sent_total` is socket acceptance, not end-to-end delivery.
- A publisher send exception stops the publisher thread. No automatic socket recreation currently exists; a future implementation must avoid uncertain multipart reuse and preserve shutdown ordering.
- Phase 12 affects provider-omitted REST quote reads only. WebSocket currently preserves each provider observation and has no selector protocol; clients must filter provider.
- FastAPI remains one worker because cache, selector, metrics, and WebSocket state are process-local.
- AKShare realtime is disabled by default and never authoritative. Its Sina 1m endpoint has a bounded recent window; ordinary night-session derivation does not invent exchange holidays.
- Historical bar identity and realtime transport identity remain distinct. Provider selection is a read projection and never becomes a persisted market event.
- The provider-aware QuestDB realtime table retains the legacy name `ctp_market_data`; renaming is deferred unless a future migration has functional justification.

## Important files

- Requirements: `REQUIREMENTS.md`
- Continuation status: `TASKS.md`
- Build graph: `CMakeLists.txt`, `cmake/Dependencies.cmake`
- Runtime configuration: `config/app.yaml`, `.env.example`
- C++ lifecycle: `cpp/include/market_data/pipeline.hpp`, `cpp/src/pipeline.cpp`
- Multipart publisher: `cpp/src/zmq_publisher.cpp`
- Provider selection: `python/live/selection.py`, `docs/phases/phase-12-provider-selection.md`
- AKShare operations: `python/providers/akshare/`, `docs/providers/akshare.md`
- Phase 4 request: `/root/.codex/attachments/58d6cac8-029d-4e96-8c0f-1448068b98b2/pasted-text.txt`
- Existing detailed docs: `docs/architecture.md`, `docs/delivery-semantics.md`


## Shared instrument resolution

Canonical identity and metadata registration are separate. The provider-independent `python/instruments` package resolves exact explicit mappings, normalized explicit mappings, and deterministic provider/exchange rules, then enriches metadata. Physical futures, provider continuous series, and rolling tenors retain distinct kinds; no delivery month is fabricated for LME 3M. AKShare historical and quote workers use this boundary, and quote association is by symbol identity. See [instrument rules and compatibility](docs/instruments.md).

Existing PostgreSQL mappings remain valid; typed aliases use existing provider/reference JSON metadata, without new PostgreSQL tables. QuestDB migrations 008–015 add nullable provenance/quality fields and leave DEDUP keys unchanged. Historical canonical writes use full IDs; read compatibility accepts legacy local IDs. New Parquet archives use schema v3 and preserve provider/native/raw identity; existing completed archives remain immutable. The optional Python live writer preserves nanosecond receive time with the correct ILP field suffix. CTP callbacks and C++ data planes are unchanged.

Historical coverage and cross-provider selection are read-side projections. Expected bars come from PostgreSQL calendars and product sessions; EXPLICIT and SINGLE never mix providers, while COMPOSITE selects whole observations per timestamp and retains actual provider provenance. Composite rows are not persisted.

Scheduled/on-demand acquisition is a separate PostgreSQL-backed control plane. Triggers enqueue deduplicated range jobs; bounded workers claim with `SKIP LOCKED`, honor persistent provider cooldown/backoff and call existing ingestion adapters. GET never invokes provider network APIs.
