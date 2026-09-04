# Changelog

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
