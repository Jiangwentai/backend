# Project task status

Last updated: 2026-09-05

## Current task

No implementation phase is active. Phase 12, ZeroMQ multipart hardening, and bounded live HWM configuration are complete. The next session should inspect `ROADMAP.md` and wait for an explicitly requested next phase; do not infer or pre-implement Phase 13.

Immediate remaining work is operator validation and maintenance:

- run a market-hours CTP smoke test through callback → QuestDB → ZeroMQ → FastAPI/WebSocket;
- confirm from authoritative SDK/broker documentation that one MdApi instance serializes `OnRtnDepthMarketData` callbacks, which underpins the SPSC ingress assumption;
- run internet-dependent AKShare 1-minute and quote smoke tests against mapped contracts;
- decide in a future explicitly scoped phase whether WebSocket subscriptions need provider-selection semantics. Phase 12 currently selects only provider-omitted REST quote reads.

## Completed

- Phase 0: CMake bootstrap, pinned dependencies, Docker Compose, QuestDB/PostgreSQL bootstrap, configuration, and structured logging.
- Phase 1: provider-neutral `MarketTick`, stable `producer_id`, monotonic `seq`, normalization, bounded queues, Dispatcher, Synthetic provider, QWP Store-and-Forward, WAL/DEDUP, shutdown, tests, and benchmark.
- Phase 2: independent Live Path, ZeroMQ PUB/SUB, MessagePack protocol, Python subscriber/cache, metrics, cross-language test, and benchmark.
- Phase 3: FastAPI lifespan, REST, health, explicit WebSocket subscriptions, bounded per-client coalescing, startup recovery, tests, and single-worker deployment.
- Phase 4: optional SDK-gated CTP adapter, state machine, login/reconnect/resubscription, empty-`ExchangeID` configured fallback, callback normalization, and simulated tests. The local Git-ignored SDK builds and a broker test front reached `READY`.
- Phase 5: normalized PostgreSQL reference metadata, async repository, `/v1/instruments`, health, migrations, and tests.
- Phase 6: paged QuestDB archive source, immutable ZSTD Parquet partitions/manifests, verification, CLI, and tests.
- Phase 7: DuckDB research, 1m/5m/1h/1d bars, cumulative-volume differencing, trading-day handling, continuous mappings, CLI/API helper, and tests.
- Phase 8: monitoring, Prometheus API metrics, health, archive audits, quality CLI/CI status, and tests.
- Phase 9: canonical multi-provider identity/events, segregated interfaces, `ProviderManager`, provider-local SPSC fan-in, provider-aware storage/live/API/archive/research paths, migrations, tests, and benchmark.
- Phase 11: optional pinned AKShare historical/reference worker, registry, mappings, immutable raw archive, daily bars, revisions, repositories, retry/rate limiting, health/metrics, scheduler/backfill, CLI, and tests.
- Phase 11B: AKShare 1-minute history and opt-in `BEST_EFFORT` quotes, normalization, incomplete-coverage reporting, raw lineage, common storage, independent poller, shared live ingress, staleness, optional persistence, Compose service, and tests.
- Phase 12: read-side provider selection with safe `explicit` default, `preferred`/`ranked` modes, freshness, opt-in fallback/stale use, transparent decisions, discrepancy diagnostics, metrics, and tests.
- ZeroMQ multipart hardening: C++ topic/body publication uses pinned cppzmq `send_multipart(..., dontwait)`; a send exception ends that socket lifecycle instead of continuing with uncertain multipart state.

- Live HWM maintenance: configurable C++ `ZMQ_SNDHWM` / YAML `live.sndhwm` and FastAPI `ZMQ_RCVHWM`, default 1000, strict bounded validation, Compose wiring, and slow-subscriber multipart/loss/recovery regression.

## Latest verification

- Python 3.12 after HWM maintenance: `73 passed, 6 skipped`; skips are environment-gated integration tests. The C++ build container with `LIVE_FIXTURE` additionally ran `75 passed, 4 skipped` on Python 3.11. Two upstream Starlette/httpx deprecation warnings remain.
- Default C++/cross-language suite after HWM maintenance: `41/41` CTest entries passed. The QuestDB test body skipped without `QDB_TEST_CONF`; this is not fresh database integration evidence. Focused HWM/config tests passed `7/7`, and Python live/API tests passed `25/25`.
- `docker compose --profile akshare config -q` passed after HWM maintenance.
- `git diff --check` passed after HWM maintenance.
- Live CTP operator output confirmed connect, login, subscription, and `READY`; this did not prove market-hours tick persistence and API/WebSocket delivery.

## Remaining work, known bugs, and limitations

- No Phase 10 specification exists in this handoff. Follow explicit roadmap/user requests rather than inferred numbering.
- `ctp_file/` is operator-supplied and Git-ignored. Never vendor it. The supplied Linux x86-64 v6.7.13 MdApi lacks `ReqAuthenticate`, so this package compiles the direct-login path.
- Confirm CTP callback serialization. If callbacks can be concurrent, deliberately revise the SPSC design without adding callback blocking.
- Run a market-hours CTP end-to-end test and verify real ticks, queue metrics, QuestDB rows, ZeroMQ, REST, and WebSocket.
- Standard `ZMQ_PUB` is deliberately lossy at HWM. A successful send means socket acceptance, not subscriber delivery; HWM loss cannot be counted reliably by `messages_sent_total`.
- A ZeroMQ send exception currently stops the publisher thread/socket. Automatic recreation is unfinished; any future restart must not reuse uncertain multipart state and must remain outside callbacks.
- FastAPI must remain one worker because cache, selector, metrics, and WebSocket clients are process-local.
- Phase 12 selection applies to provider-omitted REST quote reads. WebSocket sends provider-specific observations; clients must filter provider, and automatic mid-bar provider switching is unsafe.
- AKShare realtime defaults off and remains `BEST_EFFORT`. Sina 1-minute history is a bounded recent window without arbitrary range pagination.
- AKShare night-session fallback handles normal weekdays/weekends; authoritative exchange holidays require calendar metadata and must not be guessed.
- `provider_instrument_id` is optional and intended for stable native IDs such as IBKR `conId`; AKShare requests use `provider_symbol`.
- QuestDB table `ctp_market_data` is provider-specific naming debt, although schema and DEDUP identity are provider-aware. Do not perform a cosmetic migration without explicit scope.
- QuestDB retention deletion remains operator-only after complete archive verification.
- External integration tests require their DSNs/fixtures. FastAPI tests emit non-failing Starlette/httpx deprecation warnings.
- Session-start Git status contained a user deletion of `TASK.MD` and an untracked `oneTASK.MD` review note; both are preserved. `repomix-output.xml` had no local modification.

## Important files

- Instructions/status: `AGENTS.md`, `REQUIREMENTS.md`, `ROADMAP.md`, `ARCHITECTURE.md`, `TASKS.md`
- Build/runtime: `CMakeLists.txt`, `cmake/Dependencies.cmake`, `config/app.yaml`, `.env.example`, `compose.yaml`
- C++ lifecycle/live: `cpp/src/pipeline.cpp`, `cpp/src/dispatcher.cpp`, `cpp/src/questdb_writer.cpp`, `cpp/src/zmq_publisher.cpp`, `cpp/src/live_protocol.cpp`
- CTP: `cpp/include/market_data/ctp/`, `cpp/src/ctp/`, `cmake/FindCTP.cmake`, `docs/ctp.md`
- Python live/API: `python/live/cache.py`, `python/live/subscriber.py`, `python/live/selection.py`, `python/api/app.py`, `python/api/settings.py`
- AKShare: `python/providers/akshare/`, `docker/akshare.Dockerfile`, `docs/providers/akshare.md`
- Storage: `sql/questdb/`, `sql/postgresql/`
- Phase docs: `docs/phases/phase-09-multi-provider.md`, `docs/phases/phase-11b-akshare-intraday-and-realtime.md`, `docs/phases/phase-12-provider-selection.md`
- Semantics: `docs/delivery-semantics.md`

## Build and test commands

Default C++ build and full CTest:

```sh
docker build -f docker/build.Dockerfile -t market-data-build .
docker run --rm market-data-build \
  sh -c 'ctest --test-dir build/dev --output-on-failure'
```

Host CTP-enabled build using the operator SDK:

```sh
cmake -S . -B build/ctp \
  -DENABLE_CTP=ON \
  -DCTP_SDK_ROOT="$PWD/ctp_file"
cmake --build build/ctp --target market_data_collector -j"$(nproc)"
ctest --test-dir build/ctp --output-on-failure
```

Python/API full suite:

```sh
docker compose build api
docker compose run --rm --no-deps api python -m pytest -q python/tests
```

AKShare image/config validation:

```sh
docker compose --profile akshare build akshare-worker akshare-quotes
docker compose --profile akshare config -q
```

Isolated databases when normal ports are occupied:

```sh
QDB_HTTP_PORT=19000 QDB_PG_PORT=18812 QDB_METRICS_PORT=19003 \
POSTGRES_PORT=15432 API_PORT=18000 \
docker compose -p mdtest up -d questdb postgres

QDB_HTTP_PORT=19000 QDB_PG_PORT=18812 QDB_METRICS_PORT=19003 \
POSTGRES_PORT=15432 API_PORT=18000 \
docker compose -p mdtest run --rm questdb-init
```

Supply `POSTGRES_TEST_DSN`, `QDB_TEST_HTTP`, and/or C++ `QDB_TEST_CONF` for external integration tests. Remove only the isolated stack afterward:

```sh
docker run --rm --network mdtest_default \
  -e 'QDB_TEST_CONF=ws::addr=questdb:9000;sf_dir=/tmp/qwp-test;sender_id=integration;sender_pool_min=1;sender_pool_max=1;' \
  market-data-build \
  sh -c 'ctest --test-dir build/dev --output-on-failure'

docker compose -p mdtest down -v
```
