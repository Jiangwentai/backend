# Changelog

## Unreleased

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
