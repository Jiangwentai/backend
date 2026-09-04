# Architecture handoff

## Implemented system through Phase 6

```text
Synthetic source or optional CTP MdApi source
    -> provider-neutral MarketTick
    -> bounded ingress SPSC queue
    -> Dispatcher
       |-> bounded lossless persistence SPSC queue
       |   -> dedicated QuestDB writer
       |   -> official QuestDB C/C++ QWP client
       |   -> disk Store-and-Forward
       |   -> QuestDB WAL + DEDUP
       `-> bounded freshness-first LiveQueue
           -> dedicated ZeroMQ PUB thread
           -> MessagePack v1 multipart frames
           -> Python async ZeroMQ SUB
           -> LatestQuoteCache
              |-> REST
              `-> bounded WebSocket connection manager
```

## Important decisions

- Persistence and live distribution are independent planes. Never turn ZeroMQ into persistence or QuestDB into a polled live bus.
- Every locally received event gets the process-stable `producer_id` and a global monotonic `seq` before ingress. QWP retries retain the same identity.
- QuestDB delivery is at least once. `DEDUP UPSERT KEYS(event_ts, producer_id, seq)` collapses transport replay. Identical upstream feed events with different sequence values remain separate rows.
- CTP `TradingDay`, `ActionDay`, event time, and receive time are distinct. Never derive the trading day from the calendar date.
- Empty CTP `ActionDay` uses the existing Asia/Shanghai nearest-date rule with a 12-hour crossover boundary, then converts to UTC.
- CTP sentinel prices normalize to NaN internally and QuestDB NULL on persistence. Raw cumulative volume and turnover are not converted to deltas.
- Live delivery is best effort and freshness first. `LiveQueue` evicts only at actual capacity. WebSocket clients have independent bounded latest-per-symbol buffers.
- FastAPI recovery subscribes to ZeroMQ before querying QuestDB; both live and recovery ticks pass through the same cache conflict resolver.
- FastAPI V1 runs exactly one worker because cache, connection registry, and metrics are process-local.

## Module responsibilities

- `cpp/include/market_data/market_tick.hpp`, `cpp/src/market_tick.cpp`: provider-neutral data model, fixed strings, numeric normalization, CTP-compatible timestamp normalization.
- `spsc_queue.hpp`: bounded single-producer/single-consumer lossless queue with metrics; never overwrites unread data.
- `dispatcher.*`: sole ingress consumer and fan-out owner. Persistence is enqueued before best-effort live delivery.
- `questdb_writer.*`: dedicated QWP batching, Store-and-Forward acceptance, shutdown ACK.
- `live_queue.hpp`, `zmq_publisher.*`, `live_protocol.*`: bounded C++ live path and MessagePack v1 wire contract.
- `synthetic_generator.*`: current pipeline source and CI/development fixture.
- `pipeline.*`: thread/object ownership and ordered startup/shutdown.
- `python/live`: the single ZeroMQ subscriber implementation and latest cache.
- `python/api`: FastAPI models, lifespan, REST/WS, health, recovery repository, and WebSocket backpressure.
- `python/api/postgres_repository.py`: async PostgreSQL reference-metadata query boundary used by `/v1/instruments`.
- `sql/postgresql/002_reference_metadata.sql`: normalized exchanges, products, contracts, calendars, sessions, roll rules, and continuous mappings.
- `python/archive`: offline paged QuestDB reader, explicit Arrow schema, immutable ZSTD Parquet writer, verification manifest, and CLI.
- `python/research`: read-only DuckDB scans over verified Parquet, trading-day-aware OHLCV derivation, explicit continuous-contract resolution, and CLI.
- `python/quality`: read-only verified-archive integrity and anomaly checks with machine-readable reports and CI exit status.
- `python/api/metrics.py`: Prometheus-compatible API/live/WebSocket/dependency metrics; collector and QuestDB metrics remain in their owning processes.
- `sql/questdb/001_ctp_market_data.sql`: explicit WAL/DEDUP market snapshot schema.
- `cpp/include/market_data/ctp` and `cpp/src/ctp`: SDK-independent normalization/session logic plus the SDK-gated MdApi/Spi adapter. Only `adapter.cpp` includes proprietary headers.

## Phase 4 constraints and operator verification

- `CThostFtdc*` structures must stay inside the CTP adapter implementation. Other modules receive only `MarketTick`.
- `OnRtnDepthMarketData` must do no database, network, filesystem, sleep, blocking lock, heavy log, or retry work.
- The current ingress queue is SPSC. One MdApi instance's market-data callback path must be its only producer. Synthetic and CTP sources are mutually exclusive.
- Queue-full behavior cannot block inside the callback. It must increment visible failure/health metrics; silent loss is prohibited.
- Maintain an explicit session state: DISCONNECTED, CONNECTING, CONNECTED, AUTHENTICATING, AUTHENTICATED, LOGGING_IN, LOGGED_IN, SUBSCRIBING, READY, RECONNECTING, ERROR.
- Desired subscriptions survive disconnect; active subscriptions are rebuilt after authentication/login on reconnect.
- SDK support must default off. Enabling it requires an externally supplied, operator-pinned SDK root and a clear configure-time error if missing.
- Never log passwords, auth codes, or other credentials.
- Shutdown must first stop MdApi and callbacks, then reuse the existing ingress/persistence/live drain ordering.
- CTP is selected mutually exclusively with synthetic input through `source`. Builds default to `ENABLE_CTP=OFF`; `ENABLE_CTP=ON` requires an operator-supplied `CTP_SDK_ROOT`.
- The locally supplied Linux x86-64 v6.7.13 market-data SDK compiles with `ENABLE_CTP=ON`. It does not expose `ReqAuthenticate`, so authentication is compiled out for this package. Callback serialization must still be confirmed from its documentation, and a credentialed live-front smoke test remains required before production deployment.

## Important files

- Requirements: `REQUIREMENTS.md`
- Continuation status: `TASKS.md`
- Build graph: `CMakeLists.txt`, `cmake/Dependencies.cmake`
- Runtime configuration: `config/app.yaml`, `.env.example`
- C++ lifecycle: `cpp/include/market_data/pipeline.hpp`, `cpp/src/pipeline.cpp`
- Phase 4 request: `/root/.codex/attachments/58d6cac8-029d-4e96-8c0f-1448068b98b2/pasted-text.txt`
- Existing detailed docs: `docs/architecture.md`, `docs/delivery-semantics.md`
