# Financial Market Data Backend — Phase 0–9, 11

C++20 persistence pipeline for provider-independent market snapshots. The implemented path is:

`Synthetic Provider | CTP Provider -> provider-local bounded SPSC queues -> fan-in Dispatcher`, followed by two independent paths:

- Persistence: `bounded persistence SPSC queue -> dedicated QuestDB QWP writer -> disk Store-and-Forward -> QuestDB WAL/DEDUP`.
- Live IPC: `bounded freshness-first LiveQueue -> dedicated ZeroMQ PUB -> multipart(topic, MessagePack v2) -> async Python SUB -> provider-aware LatestQuoteCache`. Topic/body are submitted through cppzmq's non-blocking multipart helper; a send exception ends that publisher socket lifecycle.
- Web API: `LatestQuoteCache -> FastAPI REST + subscription-based WebSocket manager`.
- Metadata: `FastAPI -> asyncpg pool -> PostgreSQL exchanges/products/contracts/calendars/sessions/roll metadata`.
- Archive: `QuestDB closed logical partition -> paged reader -> Arrow schema -> immutable ZSTD Parquet + verification manifest`.
- Supplemental history/reference: independent AKShare worker -> immutable raw Parquet -> canonical QuestDB daily/1-minute bars / PostgreSQL metadata.
- Supplemental live observation: opt-in AKShare quote poller -> shared live ingress -> ZeroMQ -> provider-aware API cache (`BEST_EFFORT`, never automatic fallback).
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

PostgreSQL migrations are applied automatically only when creating a new data volume. For an existing volume, apply the Phase 5 and Phase 9 migrations explicitly:

```sh
docker compose exec -T postgres psql -U market_data -d market_data -v ON_ERROR_STOP=1 \
  < sql/postgresql/002_reference_metadata.sql
docker compose exec -T postgres psql -U market_data -d market_data -v ON_ERROR_STOP=1 \
  < sql/postgresql/003_providers.sql
docker compose exec -T postgres psql -U market_data -d market_data -v ON_ERROR_STOP=1 \
  < sql/postgresql/004_akshare.sql
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
- Identical `(event_ts, provider, producer_id, seq)` transport replays collapse through QuestDB DEDUP. Identical feed payloads with different `seq`, or events from another provider, remain distinct.
- QWP `flush()` records local Store-and-Forward acceptance. Shutdown separately waits for the server `ok` ACK.
- `drain_orphans=on` lets a new process recover prior process slots even though its new producer UUID selects a new active slot; replayed payload identities remain unchanged.
- `event_ts`, `action_day`, and `trading_day` stay separate. Empty CTP `ActionDay` normalization chooses the nearest local date across midnight and converts UTC+8 to UTC.
- Live frames retain topic `<exchange>.<instrument>` and use MessagePack `schema_version=2` with `provider`, `event_type`, `instrument_id`, and `quality`. Decoders accept legacy v1 frames as CTP quote snapshots. Cache/coalescing identity includes provider.

## Phase 9 provider architecture

Provider-specific APIs terminate at adapters implementing the realtime provider interface and publish canonical events through an event sink. `ProviderManager` owns independent lifecycle and health. Each realtime provider has its own SPSC ingress queue; the single Dispatcher round-robins those queues into the unchanged persistence and freshness-first live paths. This avoids locks in CTP callbacks while allowing Synthetic and CTP to run concurrently.

Legacy `source: synthetic|ctp` remains supported. New configuration can enable providers independently:

```yaml
providers:
  synthetic: {enabled: false}
  ctp: {enabled: true}
```

The canonical model distinguishes quote snapshots, trades, bid/ask ticks, depth updates, and provider-supplied bars. The realtime plane persists and distributes quote snapshots only; IBKR connectivity remains unimplemented. Phase 11B adds AKShare 1-minute history and optional best-effort quote snapshots without changing CTP semantics. See [provider architecture](docs/providers.md) and [data model](docs/data-model.md).

## Phase 11/11B AKShare workers

AKShare 1.18.74 is an optional Python dependency used by dedicated workers. It supports Sina futures daily/1-minute bars, 九期网 contract/fee reference records, and opt-in Sina quote snapshots. Raw historical responses are written immutably before normalization; symbols require PostgreSQL `provider_instruments` mappings; canonical bars are idempotent and revisions are audited.

```sh
docker compose --profile akshare build akshare-worker
docker compose --profile akshare run --rm akshare-worker list-datasets
docker compose --profile akshare run --rm akshare-worker fetch RB2610 --exchange SHFE
```

Canonical results can be read with `/v1/bars/SHFE.rb2610?interval=1m&provider=akshare`; that request reads QuestDB and never calls upstream. Start quotes only after setting `AKSHARE_REALTIME_ENABLED=true`, `AKSHARE_QUOTE_INSTRUMENTS=SHFE.rb2610`, and the API's `AKSHARE_ZMQ_SUB_ENDPOINT`. Query with `/v1/quotes/SHFE.rb2610?provider=akshare`. Automatic fallback remains disabled by default. See [AKShare operations](docs/providers/akshare.md).

## Phase 12 provider selection

Provider-omitted quote requests default to safe `explicit` mode: if multiple providers exist, the API returns HTTP 409 and asks for `provider=`. Operators may enable `preferred` or `ranked` selection and separately opt into fallback:

```sh
PROVIDER_SELECTION_MODE=preferred
PROVIDER_PREFERENCE=ctp,ibkr,synthetic,akshare
PROVIDER_FALLBACK_ENABLED=true
PROVIDER_ALLOW_STALE=false
PROVIDER_DISCREPANCY_BPS=20
```

Selected quotes expose the policy reason and any fallback. Inspect all observations without altering them using `/v1/provider-selection/SHFE.rb2610`.

Live transport buffers default to 1000 pending messages per peer. Configure the C++ publisher with `live.sndhwm` in `config/app.yaml` or overriding `ZMQ_SNDHWM`; configure the FastAPI subscriber with `ZMQ_RCVHWM` (also forwarded by Compose). Both accept integers from 1 to 2147483647; zero/unbounded queues are rejected. Increasing buffers trades memory and quote freshness for burst tolerance; tune with representative slow-consumer measurements. Standard PUB may drop at HWM while send calls still succeed, so send counters and would-block logs cannot measure subscriber delivery or HWM loss. See [delivery semantics](docs/delivery-semantics.md).

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
