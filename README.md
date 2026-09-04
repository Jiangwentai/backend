# Financial Market Data Backend — Phase 0/1/2/3

C++20 persistence pipeline for provider-independent market snapshots. The implemented path is:

`SyntheticGenerator -> bounded ingress SPSC queue -> Dispatcher`, followed by two independent paths:

- Persistence: `bounded persistence SPSC queue -> dedicated QuestDB QWP writer -> disk Store-and-Forward -> QuestDB WAL/DEDUP`.
- Live IPC: `bounded freshness-first LiveQueue -> dedicated ZeroMQ PUB -> multipart(topic, MessagePack v1) -> async Python SUB -> LatestQuoteCache`.
- Web API: `LatestQuoteCache -> FastAPI REST + subscription-based WebSocket manager`.

There is intentionally no real CTP adapter, archive, or Phase 4+ functionality.

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

If the default host ports are occupied, override `QDB_HTTP_PORT`, `QDB_PG_PORT`, `QDB_METRICS_PORT`, or `POSTGRES_PORT`; service-to-service ports remain unchanged.

Run integration tests from a build container on the Compose network, with a unique local SF slot:

```sh
docker run --rm --network backend_default \
  -e 'QDB_TEST_CONF=ws::addr=questdb:9000;sf_dir=/tmp/qwp-test;sender_id=integration;sender_pool_min=1;sender_pool_max=1;' \
  market-data-build ctest --test-dir build/dev --output-on-failure -L integration
```

Run the collector on a host build with `./build/dev/market_data_collector config/app.yaml`; SIGINT/SIGTERM stops input, drains both queues, publishes the final QWP batch, waits for QuestDB ACK, then closes.

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

## Phase 3 API

Start QuestDB schema initialization and the one-worker API with `docker compose up --build questdb-init api`. Endpoints are `GET /health`, `GET /v1/quotes`, `GET /v1/quotes/{symbol}`, and `WS /v1/stream/quotes`.

Run Python 3.12 tests with:

```sh
docker build -f docker/api.Dockerfile -t market-data-api .
docker run --rm market-data-api pytest -q python/tests
```

WebSocket clients must explicitly subscribe using `{"protocol_version":1,"action":"subscribe","symbols":["SHFE.zn2610"]}`. The service sends an immediate cached snapshot when present and coalesces slow-client updates per symbol in a bounded buffer. See [architecture](docs/architecture.md) and [delivery semantics](docs/delivery-semantics.md).

Uvicorn is fixed to one worker because the cache and WebSocket registry are process-local. Multi-worker deployment requires a future synchronization design and is outside Phase 3.
