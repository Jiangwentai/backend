# Project task status

Last updated: 2026-09-04

## Completed

- Phase 0: CMake bootstrap, pinned dependencies, Docker Compose, QuestDB, PostgreSQL bootstrap, YAML/environment configuration, and structured logging.
- Phase 1: provider-neutral `MarketTick`, stable process `producer_id`, global monotonic `seq`, timestamp/invalid-value normalization, bounded ingress and persistence queues, dispatcher, synthetic generator, QuestDB QWP Store-and-Forward writer, WAL/DEDUP schema, graceful shutdown, unit/integration tests, and throughput benchmark.
- Phase 2: independent bounded Live Path, C++ ZeroMQ PUB, MessagePack v1 multipart protocol, Python SUB, `LatestQuoteCache`, live metrics, cross-language integration tests, and combined persistence/live benchmark.
- Phase 3: FastAPI lifespan, latest-quote REST endpoints, health endpoint, explicit WebSocket subscriptions, bounded per-client latest-per-symbol coalescing, startup recovery from QuestDB, single-worker deployment, tests, benchmark, and documentation.
- Phase 0–3 full external integration was last validated with QuestDB: CTest 15/15 and Python 3.12 tests 17/17 passed. The last combined 3-second benchmark processed 30,001 events end-to-end at approximately 10,000.3 events/s with zero persistence or live-queue loss.

## Phase 4 implementation complete pending operator SDK verification

The Phase 4 request is in `/root/.codex/attachments/58d6cac8-029d-4e96-8c0f-1448068b98b2/pasted-text.txt`. Read it and all of `REQUIREMENTS.md` before continuing.

Implemented:

- Added an SDK-gated `CThostFtdcMdApi`/`CThostFtdcMdSpi` adapter; only its implementation includes proprietary headers.
- Added optional authentication API probing, login, desired/active subscription tracking, reconnect/resubscription, metrics, and callback normalization.
- Added `synthetic|ctp` source selection without bypassing the existing Dispatcher or either downstream path.
- Added SDK-free normalizer, state-machine, and simulated dual-path tests plus configuration and setup documentation.
- The default `ENABLE_CTP=OFF` build is CI-safe. The locally supplied Linux x86-64 v6.7.13 market-data SDK now compiles successfully with `ENABLE_CTP=ON`; its header has no `ReqAuthenticate`, so this package uses the direct login path.
- Current CTP-enabled regression result: all 24 SDK-independent unit tests passed. A credentialed front/login/subscription smoke test remains operator verification.

Before continuing, review these preliminary files critically. They may be revised; do not treat their API as final.

## Operator verification still required

- Confirm from that SDK's documentation that one MdApi instance serializes `OnRtnDepthMarketData` callbacks, preserving the SPSC ingress assumption.
- Optionally run a secret-managed live front/account smoke test and verify lifecycle logs, subscriptions, metrics, QuestDB rows, and WebSocket delivery.

## Later phases

- Phase 5: PostgreSQL metadata expansion — implemented with normalized schema, async repository, `/v1/instruments`, health integration, and tests.
- Phase 6: Parquet archive — implemented with paged QuestDB reads, immutable ZSTD Parquet partitions, atomic completion manifests, verification, CLI, and tests. QuestDB retention remains an explicit operator action after complete physical-day verification.
- Phase 7: DuckDB research and derived data — implemented with completed-partition tick scans, 1m/5m/1h/1d OHLCV bars, cumulative-volume differencing, trading-day-aware daily aggregation, explicit continuous mappings, CLI, REST helper, and tests.
- Phase 8: monitoring and quality — implemented with unified collector snapshots, structured shutdown metrics, Prometheus-compatible API metrics, component health, immutable archive audits, CI exit codes, operational alert guidance, and tests.

Do not start these while Phase 4 is active.

## Known bugs and caveats

- There is no CTP SDK in the repository. Never vendor proprietary headers or binaries unless the user explicitly supplies a legitimately distributable SDK.
- The preliminary Phase 4 files are uncompiled and may contain integration defects until CMake and tests are added.
- FastAPI's current test stack emits upstream deprecation warnings about Starlette `TestClient` using `httpx`; tests pass and runtime behavior is unaffected.
- Integration tests are environment-gated and skip without QuestDB/C++ fixture variables.
- The repository currently has no committed baseline: `git status` reports project files as untracked. Do not delete or overwrite them under the assumption that they are disposable.

## Build and test commands

Build the C++ project without CTP:

```sh
docker build -f docker/build.Dockerfile -t market-data-build .
docker run --rm market-data-build sh -c 'ctest --test-dir build/dev --output-on-failure'
```

Build and test the Python 3.12 API:

```sh
docker build -f docker/api.Dockerfile -t market-data-api .
docker run --rm market-data-api pytest -q python/tests
```

Start an isolated database stack when default ports may be occupied:

```sh
QDB_HTTP_PORT=19000 QDB_PG_PORT=18812 QDB_METRICS_PORT=19003 \
POSTGRES_PORT=15432 API_PORT=18000 \
docker compose -p mdtest up -d questdb postgres

QDB_HTTP_PORT=19000 QDB_PG_PORT=18812 QDB_METRICS_PORT=19003 \
POSTGRES_PORT=15432 API_PORT=18000 \
docker compose -p mdtest run --rm questdb-init
```

Run QuestDB integration tests from the C++ build image:

```sh
docker run --rm --network mdtest_default \
  -e 'QDB_TEST_CONF=ws::addr=questdb:9000;sf_dir=/tmp/qwp-test;sender_id=integration;sender_pool_min=1;sender_pool_max=1;' \
  market-data-build sh -c 'ctest --test-dir build/dev --output-on-failure'
```

Remove only the isolated test stack when finished:

```sh
docker compose -p mdtest down -v
```
