# Financial Market Data Backend — Phase 0–8

C++20 persistence pipeline for provider-independent market snapshots. The implemented path is:

`SyntheticGenerator | CTP MdApi -> bounded ingress SPSC queue -> Dispatcher`, followed by two independent paths:

- Persistence: `bounded persistence SPSC queue -> dedicated QuestDB QWP writer -> disk Store-and-Forward -> QuestDB WAL/DEDUP`.
- Live IPC: `bounded freshness-first LiveQueue -> dedicated ZeroMQ PUB -> multipart(topic, MessagePack v1) -> async Python SUB -> LatestQuoteCache`.
- Web API: `LatestQuoteCache -> FastAPI REST + subscription-based WebSocket manager`.
- Metadata: `FastAPI -> asyncpg pool -> PostgreSQL exchanges/products/contracts/calendars/sessions/roll metadata`.
- Archive: `QuestDB closed logical partition -> paged reader -> Arrow schema -> immutable ZSTD Parquet + verification manifest`.
- Research: `completed Parquet partitions -> DuckDB -> raw tick scans / derived OHLCV bars / explicit continuous mappings`.

The real CTP market-data adapter is optional and requires an operator-supplied SDK. No proprietary SDK files or credentials are stored here.

## Build and test

The reproducible builder includes CMake and Rust 1.91.1 (the minimum required by the pinned official QuestDB C/C++ client):

```sh
docker build -f docker/build.Dockerfile -t market-data-build .
docker run --rm market-data-build
```

Start databases and create the explicit QuestDB schema:

```sh
docker compose up -d questdb postgres
docker compose run --rm questdb-init
```

PostgreSQL applies `sql/postgresql/001_bootstrap.sql` and `002_reference_metadata.sql` automatically only when creating a new data volume. For an existing volume, apply the Phase 5 migration explicitly:

```sh
docker compose exec -T postgres psql -U market_data -d market_data -v ON_ERROR_STOP=1 \
  < sql/postgresql/002_reference_metadata.sql
```

If the default host ports are occupied, override `QDB_HTTP_PORT`, `QDB_PG_PORT`, `QDB_METRICS_PORT`, or `POSTGRES_PORT`; service-to-service ports remain unchanged.

Run integration tests from a build container on the Compose network, with a unique local SF slot:

```sh
docker run --rm --network backend_default \
  -e 'QDB_TEST_CONF=ws::addr=questdb:9000;sf_dir=/tmp/qwp-test;sender_id=integration;sender_pool_min=1;sender_pool_max=1;' \
  market-data-build ctest --test-dir build/dev --output-on-failure -L integration
```

Run the collector on a host build with `./build/dev/market_data_collector config/app.yaml`; SIGINT/SIGTERM stops input, drains both queues, publishes the final QWP batch, waits for QuestDB ACK, then closes.

### CTP build and runtime

Synthetic mode and CI build with CTP disabled by default. To compile live CTP support, obtain a legitimate SDK from the operator and provide a root containing `include/ThostFtdcMdApi.h` and `lib/` or `lib64/` with `thostmduserapi_se` (or `thostmduserapi`):

```sh
cmake -S . -B build/ctp -DENABLE_CTP=ON -DCTP_SDK_ROOT=/opt/ctp-sdk
cmake --build build/ctp
```

The repository-local, Git-ignored `ctp_file/` directory is also supported for operator SDK packages with nested, flat layouts: use `-DCTP_SDK_ROOT="$PWD/ctp_file"`. It remains excluded from normal Docker build contexts.

Configuration fails clearly if the SDK is absent. CMake probes the supplied headers for `ReqAuthenticate`; the application does not claim a CTP version. Set `source: ctp`, the front address, subscriptions and non-secret identifiers in a local YAML file. Supply `CTP_PASSWORD` and, when the operator requires authentication, `CTP_APP_ID`, `CTP_AUTH_CODE`, and `CTP_AUTHENTICATION_REQUIRED=true` through the runtime environment. The front address is passed unchanged to `RegisterFront`, normally in the operator-provided `tcp://host:port` form. See [CTP setup](docs/ctp.md).

Run the C++ PUB/Python SUB integration tests with the normal builder command. Run the combined 10k ticks/s benchmark against the Compose QuestDB service with:

```sh
docker run --rm --network backend_default \
  -e 'QDB_CONNECTION=ws::addr=questdb:9000;sf_dir=/tmp/qwp-combined;sender_id=combined-benchmark;sender_pool_min=1;sender_pool_max=1;' \
  -e ZMQ_PUB_ENDPOINT=tcp://0.0.0.0:15557 \
  market-data-build ./build/dev/combined_throughput
```

## Integrity semantics

- Queue overflow never overwrites unread raw data. Ingress producers retry; persistence saturation marks the dispatcher degraded and emits CRITICAL logs.
- Live congestion never blocks persistence: its bounded queue evicts the oldest live item and records drops, preserving the freshest quotes.
- One process UUID plus a global monotonic sequence identifies every received event. Retries never create a new identity.
- Identical `(event_ts, producer_id, seq)` transport replays collapse through QuestDB DEDUP. Identical feed payloads with different `seq` remain distinct.
- QWP `flush()` records local Store-and-Forward acceptance. Shutdown separately waits for the server `ok` ACK.
- `drain_orphans=on` lets a new process recover prior process slots even though its new producer UUID selects a new active slot; replayed payload identities remain unchanged.
- `event_ts`, `action_day`, and `trading_day` stay separate. Empty CTP `ActionDay` normalization chooses the nearest local date across midnight and converts UTC+8 to UTC.
- Live frames use topic `<exchange>.<instrument>` plus a MessagePack map with `schema_version=1`. Python rejects unsupported versions and applies sequence-aware latest-value replacement per instrument.

Configuration defaults live in `config/app.yaml`; environment variables override deployment-sensitive fields. Never commit `.env`.

## Phase 3/5 API

Start the databases and one-worker API with `docker compose up --build questdb-init postgres api`. Endpoints are `GET /health`, `GET /v1/instruments`, `GET /v1/quotes`, `GET /v1/quotes/{symbol}`, and `WS /v1/stream/quotes`. The instruments endpoint supports `exchange`, `product`, `active_only`, `limit`, and `offset` query parameters.

Run Python 3.12 tests with:

```sh
docker build -f docker/api.Dockerfile -t market-data-api .
docker run --rm market-data-api pytest -q python/tests
```

WebSocket clients must explicitly subscribe using `{"protocol_version":1,"action":"subscribe","symbols":["SHFE.zn2610"]}`. The service sends an immediate cached snapshot when present and coalesces slow-client updates per symbol in a bounded buffer. See [architecture](docs/architecture.md) and [delivery semantics](docs/delivery-semantics.md).

Uvicorn is fixed to one worker because the cache and WebSocket registry are process-local. Multi-worker deployment requires a future synchronization design and is outside Phase 3.

## Phase 6 archive

Archive a closed logical partition with the tools-profile container:

```sh
docker compose --profile tools run --rm archive \
  --exchange SHFE --instrument zn2610 --trading-day 20260904
```

Output is written under `ctp/exchange=SHFE/instrument=zn2610/trading_day=2026-09-04/`. `part-0000.parquet` uses ZSTD and `_SUCCESS.json` records row count, timestamp bounds and instrument set. Writes use a temporary file and atomic rename; an existing completed partition is verified and never overwritten.

The tool intentionally does not delete QuestDB rows. QuestDB has no filtered row deletion; retention must use TTL or `DROP PARTITION` only after every logical archive within the corresponding physical QuestDB day has been verified.

## Phase 7 research and derived bars

`research.load_ticks` scans only archive partitions that have a `_SUCCESS.json`. `research.load_bars` derives 1m, 5m, 1h, or 1d OHLCV bars with DuckDB. Intraday buckets use `event_ts`; daily bars group by the preserved CTP `trading_day`, so a night session and its following day session remain together. Volume is the sum of non-negative differences in upstream cumulative volume, with the first snapshot of each trading day contributing its cumulative value.

Query through the read-only tools-profile container:

```sh
docker compose --profile tools run --rm research bars \
  --exchange SHFE --instrument zn2610 --start-day 20260904 \
  --end-day 20260904 --interval 5m
```

The API exposes the same bar helper at `GET /v1/bars/{EXCHANGE.instrument}` with `interval`, `start_day`, and `end_day` parameters. DuckDB work runs in a worker thread and reads the archive volume read-only. `load_continuous_ticks` accepts rows from PostgreSQL `continuous_contract_mapping`; it selects the explicit physical contract for each trading day and labels the result without rewriting raw rows.

## Phase 8 monitoring and quality

FastAPI exposes Prometheus-compatible application, live subscriber, WebSocket, and dependency metrics at `GET /metrics`; detailed component health remains at `GET /health`. QuestDB exposes its native metrics on the configured metrics port (default `9003`). The collector publishes a thread-safe `Pipeline::metrics()` snapshot for ingress, persistence, QuestDB, live, ZeroMQ, dispatcher, and input state, and emits the complete snapshot as one structured shutdown log.

Audit verified archive partitions with the read-only quality tool:

```sh
docker compose --profile tools run --rm quality \
  --exchange SHFE --instrument zn2610 \
  --start-day 20260904 --end-day 20260904
```

The command returns JSON and exits non-zero for integrity errors. Cumulative-volume decreases and crossed top-of-book snapshots are warnings because they may reflect upstream/session behavior and do not authorize alteration of raw data. See [operations and alerts](docs/operations.md).
