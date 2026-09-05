# TASK.MD

# Task 3 — Scheduled Refresh and On-Demand Historical Fetch Coordinator

Implementation status: complete (2026-09-06). The coordinator is provider-independent; provider execution remains in the worker adapter, and historical reads remain local-only.

## Objective

Implement a provider-independent historical acquisition coordinator supporting:

```text
scheduled refresh
+
on-demand ensure/fetch
```

for providers such as:

```text
AKShare
X
IBKR historical
future HTTP/public providers
```

The system must avoid continuous polling by default.

Goals:

```text
reduce external provider requests
avoid rate limits / bans
reuse local history
keep recent bars reasonably fresh
fill missing history when needed
support foreign instruments with no tick source
deduplicate repeated fetches
protect providers with cooldown/backoff
```

This task assumes Task 1 instrument resolution and Task 2 historical coverage are already implemented.

---

## Core Architecture

Historical read path and provider acquisition path must remain separate.

Required:

```text
Frontend
   ↓
GET /v1/bars
   ↓
local historical storage
   ↓
return immediately
```

Acquisition:

```text
Scheduled trigger / On-demand ensure
             ↓
Historical Fetch Coordinator
             ↓
coverage/freshness/cooldown/dedup checks
             ↓
Historical Worker
             ↓
AKShare / X / ...
             ↓
raw archive
             ↓
normalize
             ↓
provider-aware storage
```

Do not synchronously call AKShare/X inside `/v1/bars`.

---

# Acquisition Modes

Support:

```text
MANUAL
SCHEDULED
ON_DEMAND
```

### MANUAL

Operator explicitly requests a fetch/backfill.

### SCHEDULED

Low-frequency maintenance refresh.

### ON_DEMAND

Triggered because local history is:

```text
missing
partial
stale
newly requested
```

---

# No Continuous Polling

Historical providers such as AKShare must not be continuously polled by default.

Do not implement:

```text
while true:
    fetch every minute
```

Default strategy:

```text
daily/low-frequency scheduled refresh
+
request-triggered fetch when useful
```

---

# Historical Fetch Coordinator

Introduce a provider-independent coordinator such as:

```python
class HistoricalFetchCoordinator:
    async def ensure_history(...)
    async def request_refresh(...)
    async def schedule_refresh(...)
```

The coordinator decides whether a provider call is needed.

Provider adapters/workers perform the actual network fetch.

Do not put AKShare-specific HTTP logic in the coordinator.

---

# Ensure Request

Recommended conceptual model:

```python
@dataclass(frozen=True)
class HistoricalEnsureRequest:
    instrument_id: str
    interval: str
    start: datetime
    end: datetime

    preferred_provider: str | None
    reason: str
    force: bool = False
```

Possible reasons:

```text
MANUAL
SCHEDULED
MISSING_HISTORY
STALE_HISTORY
COVERAGE_GAP
```

---

# Fetch Identity and Dedup

Logical fetch identity must include at least:

```text
provider
instrument
interval
requested range
```

Equivalent active requests must not create duplicate provider calls.

Example:

```text
AKShare
SHFE.rb2610
1m
2026-09-01 → 2026-09-05
```

first request:

```text
QUEUED/RUNNING
```

second identical request:

```text
ALREADY_RUNNING
```

---

## Overlapping Fetches

If running:

```text
09/01 → 09/05
```

new request:

```text
09/02 → 09/04
```

do not start a second fetch.

If practical, partially overlapping requests may be reduced to uncovered range only.

Do not over-engineer range merging initially.

---

# Fetch Status

Introduce/reuse statuses equivalent to:

```python
class HistoricalFetchStatus(StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    COOLDOWN = "COOLDOWN"
```

---

# Fetch Metadata

Track where practical:

```text
fetch/request id
provider
instrument
interval
range_start
range_end
trigger
status

requested_at
started_at
completed_at

last_attempt_at
last_success_at
next_allowed_at

rows_received
rows_written

coverage_before
coverage_after

error_code
error_message
```

Reuse existing ingestion-run structures where possible.

Avoid duplicate lifecycle models unnecessarily.

---

# Refresh Policy

Introduce provider/interval refresh policy.

Conceptually:

```python
@dataclass(frozen=True)
class HistoricalRefreshPolicy:
    provider: str
    interval: str

    scheduled_enabled: bool
    request_triggered: bool

    min_refresh_interval_seconds: int
    stale_after_seconds: int | None

    recent_refresh_days: int
    immutable_after_days: int

    max_concurrency: int
```

All values must be configurable.

---

# Recommended AKShare Policy

Use conservative configurable defaults.

For example:

```text
1m:

scheduled_enabled=true
request_triggered=true

min_refresh_interval=300 seconds
stale_after=300 seconds

recent_refresh_days=3
immutable_after_days=7

max_concurrency=1 or 2
```

Do not hardcode these values into generic logic.

---

# Old / Recent / Live Edge

Treat different historical ranges differently.

## OLD

Older than:

```text
immutable_after_days
```

If local coverage is already complete:

```text
do not automatically re-fetch
```

unless:

```text
force=true
```

"Immutable" means:

```text
no routine automatic refresh
```

not that manual correction is forbidden.

---

## RECENT

Recent rolling window may be refreshed because providers can:

```text
backfill missing bars later
revise recent bars
publish delayed corrections
```

Recommended scheduled window:

```text
last 2–3 relevant trading days
```

configurable.

---

## LIVE EDGE

For instruments with no realtime/tick source, recent 1m history may act as near-live data.

Example:

```text
latest local bar = 17:00
expected latest provider bar = 17:07
stale_after = 5 minutes
```

A chart request may trigger one refresh.

Still obey:

```text
dedup
cooldown
provider backoff
concurrency limits
```

---

# Coverage vs Freshness

Keep separate:

```text
coverage
```

and:

```text
freshness
```

Example:

```text
coverage=99.9%
freshness lag=8 minutes
```

means:

```text
history is mostly complete
latest data is stale
```

Task 2 coverage should be reused.

---

# Freshness

Introduce/reuse:

```python
@dataclass(frozen=True)
class HistoricalFreshness:
    latest_bar_start: datetime | None
    expected_latest_bar_start: datetime | None
    lag_seconds: float | None
    stale: bool | None
```

Use market/session semantics where available.

Do not simply compare wall-clock time to latest bar.

---

## Market Closed

If the market is closed:

```text
last session close bar
```

may still be fresh.

Do not mark data stale simply because several wall-clock hours passed overnight.

---

## Unknown Foreign Sessions

If session metadata is not authoritative:

```text
freshness=UNKNOWN
```

is preferable to guessing.

---

## Provider Publication Delay

Support configurable:

```text
publication_delay_seconds
```

if needed.

Freshness should account for expected provider delay.

---

# Scheduled Refresh

Implement low-frequency scheduled maintenance.

Recommended:

```text
once daily
→ refresh recent N trading days
```

Do not re-fetch all history every day.

---

# Scheduled Universe

Do not refresh every known contract automatically.

Support one or more of:

```text
explicit pinned instruments
recently accessed instruments
active/front instruments
configured watchlist
```

V1 may start with:

```text
explicit configured instruments
+
recently requested instruments
```

---

## Expired Contracts

Expired physical contracts should stop daily refresh after they move outside the mutable/recent window.

Unless explicitly pinned.

---

## Continuous / Rolling Instruments

Instruments such as:

```text
LME.zn.3m
COMEX.gc.continuous
```

may remain eligible indefinitely.

---

# Scheduled Failure Isolation

One instrument/provider failure must not abort the whole scheduled cycle.

Example:

```text
RB succeeds
ZN fails
GC succeeds
```

scheduled run result:

```text
PARTIAL
```

not immediate total abort.

---

# On-Demand Ensure

Add an explicit API such as:

```http
POST /v1/history/ensure
```

Example body:

```json
{
  "instrument": "LME.zn.3m",
  "interval": "1m",
  "start": "...",
  "end": "..."
}
```

Meaning:

```text
attempt to make this range available locally
```

It does not synchronously return provider data.

Possible results:

```text
ALREADY_COMPLETE
ALREADY_RUNNING
COOLDOWN
QUEUED
NO_ELIGIBLE_PROVIDER
```

---

# GET /v1/bars

`GET /v1/bars` must continue reading local data.

It must not wait for provider fetch completion.

Recommended frontend flow:

```text
GET local bars
→ render immediately
→ inspect coverage/freshness
→ optionally POST /v1/history/ensure
→ later GET updated local bars
```

---

## Optional Auto Ensure

Optionally support:

```text
refresh=auto
```

on GET.

If enabled:

```text
queue ensure request
but still return local data immediately
```

No synchronous provider call.

If safeguards are not mature, default to explicit ensure instead.

---

# Stale-While-Refresh

Preferred behavior:

```text
1. return stored bars
2. determine stale/missing
3. queue refresh if allowed
4. next query sees new bars
```

Chart responsiveness must not depend on provider availability.

---

# Cooldown

Each provider/interval must enforce a minimum refresh interval.

Example:

```text
last successful AKShare fetch = 17:00
cooldown = 5 minutes
```

Requests at:

```text
17:01
17:02
17:04
```

must not call AKShare again.

Possible response:

```text
COOLDOWN
```

or:

```text
ALREADY_FRESH
```

---

# Provider Concurrency

Add provider-level concurrency limits.

Example:

```text
AKShare max concurrent historical fetches = 1
```

or:

```text
2
```

configurable.

Opening many charts must not launch dozens of simultaneous AKShare calls.

---

# Rate Limiting and Backoff

Automatic fetches must respect:

```text
minimum request interval
provider rate limits
cooldown
bounded concurrency
failure backoff
```

Use bounded exponential backoff with jitter where appropriate.

Example progression:

```text
1m
2m
4m
8m
...
cap
```

Exact values configurable.

---

# Failure Categories

Distinguish at least:

```text
NETWORK_ERROR
RATE_LIMIT
PROVIDER_ERROR
SCHEMA_ERROR
MAPPING_ERROR
EMPTY_RESPONSE
UNSUPPORTED_RANGE
```

Do not retry all failures identically.

---

## Rate Limit

If provider signals rate limiting:

```text
increase next_allowed_at
```

and suppress automatic requests until cooldown/backoff expires.

---

## Schema / Mapping Error

Do not retry aggressively.

These usually need:

```text
code fix
reference update
mapping correction
```

---

## Empty Response

Do not automatically interpret empty response as:

```text
complete history
```

Record actual outcome.

---

# Provider Capabilities

Reuse/extend historical capabilities such as:

```text
supports_historical_bars
supported_intervals
supports_arbitrary_range
bounded_recent_history
supports_latest_bars
recommended_min_refresh_interval
```

Coordinator must consult capabilities rather than provider-name checks.

---

# AKShare Bounded 1m

If AKShare minute history only supports a bounded recent window:

```text
do not repeatedly retry impossible old ranges
```

Capability/limitation metadata must make this explicit.

---

# Acquisition Provider Selection

Keep separate:

```text
which provider should I fetch from?
```

and:

```text
which provider should read-side selector return?
```

They are not necessarily identical.

Example:

```text
X preferred for read
but X does not support old range
```

Acquisition may use AKShare if policy allows.

---

## Preferred Provider

If ensure request specifies:

```text
preferred_provider=X
```

do not silently fall back unless acquisition fallback is explicitly configured.

---

# Missing Range Fetching

Use Task 2 coverage to identify missing ranges.

Example:

```text
requested:
09/01 → 09/05

local:
09/01 → 09/02
09/04 → 09/05

missing:
09/03
```

If provider supports range requests:

```text
fetch missing range
```

rather than blindly re-fetching everything.

---

# Providers Without Exact Range Support

Some upstream APIs may ignore start/end or expose only a recent window.

Adapters must report:

```text
requested range
actual returned range
provider limitation
```

Coverage determines what was actually gained.

Do not pretend unsupported range control exists.

---

# Raw Archive and Storage

Every actual fetch must preserve normal ingestion semantics:

```text
provider-specific raw archive
→ normalize
→ provider-aware historical storage
```

Scheduled/on-demand runs must not bypass lineage or validation.

---

# Trigger Metadata

Every fetch/run should record:

```text
MANUAL
SCHEDULED
ON_DEMAND
```

---

# Revisions

Repeated scheduled fetches may discover same-provider revisions.

That must use existing revision logic.

Do not treat expected refresh as a duplicate failure.

Cross-provider disagreement remains separate observations.

---

# Job Persistence

Inspect the existing scheduler/job infrastructure before adding schema.

Prefer reusing:

```text
provider_ingestion_runs
DatasetScheduler
ScheduledJob
existing worker queue
```

where practical.

If a persistent fetch request queue is needed, add it additively.

---

## Optional Fetch Request Table

Only if existing structures are insufficient, add a table conceptually like:

```text
historical_fetch_requests
```

with:

```text
id
provider
instrument
interval
range_start
range_end
trigger
status
force
created_at
started_at
completed_at
last_error
coverage_before
coverage_after
```

Do not create this if `provider_ingestion_runs` can cleanly support the lifecycle.

---

# Worker Semantics

A bounded worker queue is required.

Do not create unlimited background tasks.

Kafka/Redis are not required for V1.

A PostgreSQL-backed queue or existing worker abstraction is acceptable.

---

## Multi-Worker Safety

If multiple workers can consume persistent jobs, prevent duplicate execution using an appropriate locking/lease pattern.

If V1 intentionally runs one historical worker, document that and keep implementation simple.

---

# CLI

Prefer a generic historical worker CLI.

Examples:

```bash
historical-worker ensure \
  --instrument SHFE.rb2610 \
  --interval 1m \
  --start ... \
  --end ...
```

Optional:

```bash
historical-worker run-scheduled-refresh
```

for manually testing the scheduled logic.

Add status diagnostics for:

```text
queued
running
last success
cooldown
next allowed
```

---

# Lightweight Charts Use Case

Expected flow:

```text
open chart
↓
GET /v1/bars
↓
render local data immediately
↓
if stale/missing:
POST /v1/history/ensure
↓
coordinator decides whether provider fetch is allowed
↓
worker fetches once
↓
QuestDB updated
↓
later GET shows newer data
```

Repeated left-scroll or refresh events must not repeatedly hit AKShare.

---

# Domestic Strategy

For instruments with CTP realtime:

```text
AKShare/X
→ bootstrap/backfill/gap recovery

CTP
→ current session/live edge
```

Do not continuously refresh AKShare during normal domestic realtime operation.

---

# Foreign Strategy

For instruments with no tick source:

```text
daily scheduled refresh
+
stale-triggered on-demand refresh
```

may maintain recent 1m data.

This is near-live historical refresh, not a realtime tick feed.

---

# Metrics

Add low-cardinality metrics such as:

```text
historical_fetch_requests_total
historical_fetch_started_total
historical_fetch_success_total
historical_fetch_partial_total
historical_fetch_failed_total

historical_fetch_deduplicated_total
historical_fetch_cooldown_total
historical_fetch_skipped_complete_total

historical_scheduled_runs_total
historical_refresh_trigger_total
historical_provider_rate_limit_total
historical_provider_backoff_total
```

Useful labels:

```text
provider
interval
trigger
status
```

Do not use instrument symbols as labels.

---

# Required Tests

## Already Complete

Old range coverage 100%.

Ensure requested.

Expected:

```text
ALREADY_COMPLETE / SKIPPED
```

Provider client not called.

---

## Missing Range

Coverage partial.

Expected:

```text
fetch queued
```

---

## Duplicate Request

Two identical ensure requests concurrently.

Expected:

```text
one provider call
```

---

## Covered By Running Range

Running:

```text
09/01 → 09/05
```

new:

```text
09/02 → 09/04
```

Expected:

```text
no second provider call
```

---

## Cooldown

Last success 2 minutes ago.

Cooldown 5 minutes.

Expected:

```text
no provider call
COOLDOWN / ALREADY_FRESH
```

---

## Force

Manual `force=true`.

May bypass normal freshness/old-history rules.

Must still respect hard provider concurrency/rate protections.

---

## Scheduled Recent Window

Configured:

```text
recent_refresh_days=3
```

Expected:

```text
last 3 relevant trading days selected
```

---

## Old History

Old complete data:

```text
not scheduled for routine refresh
```

---

## Revision Window

Recent complete data may be re-fetched once per scheduled policy to detect corrections.

Cooldown still applies.

---

## Failure Isolation

Scheduled universe:

```text
RB
ZN
GC
```

ZN fails.

RB and GC still complete.

Scheduled run:

```text
PARTIAL
```

---

## Backoff

Repeated provider rate-limit/network failures increase `next_allowed_at`.

Successful fetch resets failure backoff.

---

## GET Does Not Block

Provider fetch is slow or fails.

`GET /v1/bars` still returns stored bars immediately.

---

## Market Closed Freshness

Last close bar exists.

Market closed.

Expected:

```text
not stale
```

if it equals canonical expected latest bar.

---

## Unknown Session

No authoritative foreign session metadata.

Expected:

```text
freshness=UNKNOWN
```

not arbitrary stale/fresh.

---

## Multiple Providers

Preferred acquisition provider unavailable/ineligible.

Fallback behavior must follow explicit configuration.

No random provider spraying.

---

# Hard Invariants

Do not:

* synchronously call AKShare/X from `/v1/bars`;
* continuously poll AKShare by default;
* refetch all history daily;
* refresh every known instrument blindly;
* launch unbounded concurrent fetches;
* bypass cooldown/backoff;
* equate fetch success with complete history;
* hide partial coverage;
* fabricate missing bars;
* merge providers during ingestion;
* alter Task 2 read-selection semantics;
* alter CTP realtime architecture;
* add Kafka/Redis solely for this feature.

---

# Documentation

Update relevant files, preferably:

```text
docs/historical-data.md
docs/providers/akshare.md
docs/architecture.md
docs/operations.md
CHANGELOG.md
```

Document:

```text
scheduled refresh + on-demand ensure
no continuous polling by default
GET reads local storage
coverage != freshness
domestic CTP vs foreign no-tick strategy
cooldown/dedup/backoff behavior
```

---

# Definition of Done

Complete when:

* scheduled refresh exists;
* on-demand ensure exists;
* `/v1/bars` never synchronously waits on provider fetch;
* stored bars remain usable during provider outage;
* recent rolling refresh window is configurable;
* old complete history is not routinely re-fetched;
* duplicate requests are suppressed;
* overlapping running requests are recognized where practical;
* cooldown works;
* provider concurrency is bounded;
* rate-limit/backoff is respected;
* scheduled failures are isolated;
* coverage and freshness remain separate;
* market-session-aware freshness works where metadata exists;
* AKShare bounded history limitations are respected;
* provider-specific revisions remain correct;
* multiple providers use the same coordinator;
* domestic CTP-backed instruments do not require AKShare polling;
* foreign no-tick instruments support conservative near-live refresh;
* required tests pass;
* documentation is updated.

---

# Codex Execution Instructions

Before implementation:

1. Inspect Task 2 coverage/selection implementation.
2. Inspect existing AKShare scheduler.
3. Inspect `DatasetScheduler` / `ScheduledJob`.
4. Inspect `provider_ingestion_runs`.
5. Inspect historical fetch/backfill commands.
6. Inspect `/v1/bars`.
7. Inspect provider capabilities and rate-limit logic.
8. Inspect current worker deployment model.
9. Reuse existing scheduler/job abstractions where possible.
10. Keep migrations additive.
11. Avoid unrelated refactors.

After implementation report:

```text
files changed
schema changes
fetch coordinator design
scheduler implementation
dedup behavior
cooldown rules
backoff rules
refresh windows
freshness logic
provider policies
API changes
CLI changes
tests added
tests executed
known limitations
```


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
