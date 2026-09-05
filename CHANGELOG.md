# Changelog

## 2026-09-05 — Live transport HWM configuration

- Made C++ PUB `SNDHWM` configurable through `live.sndhwm` / `ZMQ_SNDHWM`, and FastAPI SUB `RCVHWM` through `ZMQ_RCVHWM`, including Compose wiring. Both retain the default 1000 and require bounded positive 32-bit integer values before bind/connect.
- Added configuration validation and a pinned-libzmq slow-subscriber regression for silent PUB loss, intact multipart messages, and recovery after draining.
- Clarified that successful sends and would-block warnings cannot measure PUB HWM loss; increasing queues trades memory and freshness for burst tolerance without changing persistence semantics.

## 2026-09-05 — ZeroMQ multipart hardening

- Replaced manually paired topic/body sends with cppzmq's `send_multipart` helper while retaining non-blocking Live Path semantics.
- A send exception now terminates that publisher socket lifecycle instead of continuing with potentially uncertain multipart state. Normal PUB/HWM loss remains best-effort and is not falsely reported as reliable delivery.

## 2026-09-05 — Phase 12

- Added a provider-neutral read-side selector with `explicit`, `preferred`, and `ranked` policies. Default `explicit` mode preserves prior behavior and requires `provider=` when multiple feeds coexist.
- Added independently configured provider preference, freshness windows, opt-in failover, opt-in stale use, and quality ranking. Selected responses expose their reason, preferred provider, stale state, and whether failover occurred.
- Added `/v1/provider-selection/{symbol}` diagnostics for simultaneous observations and cross-provider price discrepancies, plus bounded-label selection/failover/failure/discrepancy metrics.
- Selection never rewrites cached events, affects persistence identity, invokes provider SDKs, or executes on CTP callbacks. Automatic fallback remains disabled unless explicitly configured.

## 2026-09-05 — Phase 11B

- Added AKShare/Sina 1-minute futures bars with strict OHLC/schema validation, explicit Asia/Shanghai conversion, night-session trading-day rules, immutable raw lineage, canonical idempotency/revision handling, range filtering, resumable backfill, periodic refresh, and stored FastAPI queries.
- Added an opt-in, conservatively paced AKShare quote poller. It emits only provider-neutral `QuoteSnapshot` events marked `provider=AKSHARE` and `quality=BEST_EFFORT` through the shared live ingress; it never reconstructs trades or silently replaces CTP.
- Added multi-endpoint live subscription, provider-aware case-normalized cache keys, AKShare age/stale fields, optional generic QuestDB live persistence, quote metrics, offline fixtures, and the `akshare-quotes` Compose service.
- The existing `ctp_market_data` table name is retained as documented technical debt; its schema and DEDUP key are already provider-aware, so Phase 11B avoids a cosmetic risky migration.

## Unreleased

- Phase 11 adds an optional, independently runnable AKShare 1.18.74 historical/reference provider without touching the realtime CTP/ZeroMQ path.
- Added verified Sina futures daily and 九期网 contract-reference endpoint adapters, strict schema/type/OHLC validation, symbol normalization, PostgreSQL canonical mapping, and visible unresolved-symbol quarantine. The incompatible 1.18.74 Sina display-main helper is registered but disabled.
- Added immutable raw ZSTD Parquet fetch archives with UUID lineage, manifests and hashes; canonical QuestDB daily bars use semantic DEDUP while PostgreSQL retains latest versions and explicit revision history.
- Added provider-wide throttling, bounded transient retry with jitter, health, bounded-cardinality metrics, dataset scheduler, resumable backfill state, operational CLI, dedicated optional worker image, migrations, offline fixtures, and database integration tests.
- Registered inventory (99QH) and position/ranking (Sina) endpoints as experimental and disabled; realtime polling, automatic fallback, arbitration, and Phase 11B remain out of scope.
- Phase 09 introduced strong provider identity, capability/health models, canonical event headers and explicit quote/trade/bid-ask/depth/bar semantics.
- Migrated Synthetic and CTP behind a common realtime-provider lifecycle, event sink, canonical subscriptions, instrument mapping, and `ProviderManager`.
- Added one SPSC ingress queue per realtime provider with round-robin fan-in, preserving callback non-blocking behavior and downstream persistence/live isolation.
- Added provider/event/instrument/quality columns and provider-aware DEDUP through additive QuestDB migrations; added PostgreSQL provider registry and instrument mappings.
- Advanced MessagePack live frames to schema v2 while retaining v1 decode compatibility; FastAPI responses, cache, WebSocket payloads, health, and metrics now expose provider context.
- Advanced new Parquet partitions to schema v2 and made DuckDB/quality paths provider-aware while retaining old archive reads and the existing `ctp/` partition layout.
- Phase 09 benchmark: queue throughput changed from 5.249M to 5.219M events/s (-0.6%); the 10k/s combined run persisted and published 30,001/30,001 with zero drops or failures.
- Compatibility: legacy `source`, quote fields, table name, archive paths, and WebSocket subscription protocol remain; operators must apply the additive Phase 09 database migrations before deploying the new writer/API.
- Known limitation: Phase 09 defines non-quote event types and provider boundaries but the current persistence/live implementation accepts quote snapshots only; IBKR, AKShare, failover, arbitration, and routing remain out of scope.
- Preserve the exchange prefix from CTP subscriptions and use it when market-data callbacks omit `ExchangeID`.

- Added discovery for nested, flat CTP SDK packages and Linux libraries shipped without the usual `lib` filename prefix.
- Kept repository-local `ctp_file/` SDK material out of Git and ordinary Docker build contexts.
- Staged a normalized runtime library with origin-relative linking so SDKs without an ELF SONAME do not embed container-only absolute paths.
- Fixed the MdSpi implementation destructor for SDK versions whose base SPI destructor is non-virtual.
- Successfully compiled and ran all 24 unit tests with the supplied Linux x86-64 v6.7.13 market-data SDK; authenticated and live-front validation remains pending.

## 0.8.0 - 2026-09-04

- Added a thread-safe collector pipeline metrics snapshot spanning input, ingress, persistence, QuestDB writer, live queue, ZeroMQ, and dispatcher state.
- Expanded the structured collector shutdown summary to expose loss, pressure, delivery, latency, failure, and health counters.
- Added Prometheus-compatible FastAPI request, dependency, live subscriber, and WebSocket metrics at `GET /metrics`.
- Added read-only archive quality checks for identity, timestamps, trading days, numeric validity, cumulative volume, and top-of-book anomalies.
- Added a machine-readable quality CLI with non-zero integrity-failure exit status and a read-only Compose tools service.
- Added monitoring, quality, CLI, and metrics endpoint tests plus operational alert/runbook documentation.

## 0.7.0 - 2026-09-04

- Added DuckDB utilities for reading completed immutable Parquet partitions directly as Arrow tables.
- Added 1m, 5m, 1h, and trading-day-aware 1d OHLCV generation from market snapshots.
- Derived bar volume from cumulative-volume differences without changing raw values, including explicit trading-day resets.
- Added explicit continuous-contract mapping reads that preserve the selected physical instrument.
- Added a read-only research Compose service, JSON-lines CLI, and `GET /v1/bars/{symbol}` worker-thread API route.
- Added tick filtering, OHLCV, night/day session, volume-reset, continuous mapping, CLI boundary, and API tests.

## 0.6.0 - 2026-09-04

- Added an offline QuestDB-to-Parquet archive tool with stable pagination and an explicit Arrow market-snapshot schema.
- Added immutable Hive-style partitions, ZSTD compression, atomic publication, and `_SUCCESS.json` verification manifests.
- Added verification of row counts, timestamp bounds, instruments, readability, and Parquet compression.
- Added a tools-profile Compose service and unit tests for archive safety, idempotency, tamper detection, and QuestDB pagination.
- Deliberately omitted source deletion because QuestDB requires TTL or whole physical partition removal; no data is deleted before operator-level archive completeness verification.
- Kept DuckDB research APIs and derived bars out of Phase 6.

## 0.5.0 - 2026-09-04

- Added normalized PostgreSQL metadata tables for exchanges, products, futures contracts, trading calendars, trading sessions, roll rules, and continuous-contract mappings.
- Added foreign keys, lifecycle/date checks, lookup indexes, and reference-metadata schema versioning.
- Added an asyncpg metadata repository and filtered, paginated `GET /v1/instruments` API.
- Added PostgreSQL health reporting, Compose wiring, unit tests, and an environment-gated real PostgreSQL repository test.
- Kept Parquet archive, DuckDB research, and derived-data computation out of Phase 5.

## 0.4.0 - 2026-09-04

- Added optional, operator-SDK-backed CTP market-data ingestion with explicit connection, authentication, login, subscription, reconnect, and resubscription states.
- Added `synthetic|ctp` input selection while preserving the existing ingress, dispatcher, QuestDB persistence, and ZeroMQ live paths.
- Added CTP snapshot normalization, callback failure metrics, desired/active subscriptions, and input-first graceful shutdown.
- Added `ENABLE_CTP=OFF` by default, SDK discovery, authentication API probing, secret-safe configuration, and CTP setup documentation.
- Added SDK-free normalizer, state-machine, reconnect/failure, and simulated dual-path tests.

## 0.3.0 - 2026-09-04

- Added Phase 3 FastAPI lifespan management, latest-quote REST endpoints, structured health states, and one-worker container deployment on Python 3.12.
- Added versioned, validated WebSocket subscriptions with immediate snapshots and per-client bounded latest-per-symbol coalescing.
- Added subscribe-first QuestDB startup recovery using QuestDB `LATEST ON` and the existing cache conflict-resolution semantics.
- Added WebSocket metrics, REST/WS/health/recovery/slow-client tests, full C++ PUB-to-WebSocket coverage, restart recovery coverage, and an API-path throughput benchmark.
- Corrected LiveQueue contention loss discovered by the combined benchmark; live eviction now occurs only at actual capacity.
- Pinned FastAPI 0.141.1, Uvicorn 0.52.4, Pydantic 2.13.5, HTTPX 0.28.1, and websockets 17.1.
- Kept Phase 4+, real CTP, and new infrastructure explicitly out of scope.

## 0.1.0 - 2026-09-04

- Added Phase 0 CMake, pinned dependencies, Docker Compose, YAML/environment configuration, PostgreSQL bootstrap, explicit QuestDB WAL/DEDUP schema, and structured logging.
- Made Compose host ports environment-configurable so an isolated development stack does not require stopping existing services.
- Added Phase 1 provider-neutral `MarketTick`, stable producer UUID/global sequence, timestamp and invalid-price normalization, observable bounded SPSC queues, persistence dispatcher, synthetic generator, and graceful pipeline shutdown.
- Added official QuestDB C/C++ 7.0.0 QWP writer with disk Store-and-Forward, configurable row/latency batching, and shutdown ACK barrier.
- Pinned the build toolchain to Rust 1.91.1, the minimum declared by that client release.
- Added unit tests, QuestDB QWP integration/replay/feed-duplicate DEDUP test, and synthetic queue throughput benchmark.
- At that release, explicitly deferred Phase 2+ live IPC/API and real CTP integration.

## 0.2.0 - 2026-09-04

- Added Phase 2's independent, bounded freshness-first Live Path without changing Persistence Path loss semantics.
- Added a dedicated C++ ZeroMQ PUB thread and versioned MessagePack multipart protocol keyed by `<exchange>.<instrument>`.
- Added the async Python SUB client, sequence-aware `LatestQuoteCache`, malformed/version frame isolation, and live-path metrics.
- Added live queue/protocol/dispatcher unit tests and a C++ PUB to Python SUB integration test.
- Added a combined 10k ticks/s benchmark that runs QuestDB QWP Store-and-Forward persistence and ZeroMQ publication concurrently.
- Pinned libzmq 4.3.5, cppzmq 4.9.0, msgpack-c++ 8.0.0, pyzmq 27.2.0, msgpack 1.2.2, pytest 8.4.2, and pytest-asyncio 1.1.0.
- Kept real CTP, FastAPI/WebSocket, and all Phase 3+ work explicitly deferred.
