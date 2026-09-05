

```markdown
# Financial Market Data Backend
REQUIREMENTS.md
Version: 1.1
Primary Platform: Linux / Arch Linux
Primary Language: C++20
API Layer: Python 3.12+ / FastAPI
Primary Market: Chinese Futures via CTP
Status: Final Architecture Specification

## 1. Project Goal
Build a self-hosted financial market data backend initially focused on Chinese futures market data through the CTP API.
The system must support:
- high-frequency CTP market-data ingestion
- reliable persistence into QuestDB
- real-time quote distribution to API/WebSocket consumers
- contract and reference metadata in PostgreSQL
- immutable historical archive in Parquet
- historical research through DuckDB
- REST APIs
- WebSocket APIs
- future extension to additional market-data providers


Future providers may include: LME, CME, FRED, SEC, Binance, IBKR, Chinese exchanges, additional broker APIs.

AKShare is an implemented optional supplemental provider. It supplies persisted daily/1-minute historical bars and opt-in polled quote snapshots. AKShare snapshots must remain `BEST_EFFORT`, must not synthesize trades or depth, and must enter the provider-independent live ingress outside FastAPI and native provider callbacks. Provider fallback is disabled by default and may only occur under an explicit Phase 12 policy; every selected fallback must be visible in the API response and metrics.

The system is intended for: personal quantitative research, futures market analysis, historical data analysis, backtesting, market monitoring, and future live-strategy infrastructure.

The architecture must prioritize: correctness, data integrity, no silent data loss, low latency, raw-data preservation, observability, clear module boundaries, reproducibility, maintainability, and future extensibility.

## 2. Fundamental Architecture
The system contains two logically independent data planes.

### 2.1 Persistence Data Plane
The persistence path is responsible for reliable storage.
```text
CTP Front
    │
    ▼
C++ CTP Collector
    │
    ▼
MarketTick Normalizer
    │
    ▼
Ingress Ring Buffer
    │
    ▼
Dispatcher
    │
    ▼
Persistence Queue
    │
    ▼
QuestDB Writer Thread
    │
    ▼
QuestDB C++ Client
    │
    ▼
QWP over WebSocket
    │
    ▼
Store-and-Forward
    │
    ▼
QuestDB WAL
    │
    ▼
DEDUP

```

This path must favor: durability, completeness, ordering, observability, replayability. Silent data loss is prohibited.

### 2.2 Live Distribution Data Plane

The live path is responsible for low-latency quote distribution.

```text
CTP Front
    │
    ▼
C++ CTP Collector
    │
    ▼
MarketTick
    │
    ▼
Ingress Ring Buffer
    │
    ▼
Dispatcher
    │
    ▼
Live Queue
    │
    ▼
ZeroMQ PUB
    │
    ▼
ZeroMQ SUB
    │
    ▼
FastAPI
    │
    ├── Latest Quote Cache
    │
    ├── REST
    │
    └── WebSocket

```

This path must favor: freshness, low latency, latest state, low overhead. Unlike the persistence path, stale live updates MAY be dropped or coalesced.

## 3. Complete High-Level Architecture

```text
                         CTP Front
                             │
                             ▼
                     CThostFtdcMdApi
                             │
                             ▼
                  OnRtnDepthMarketData()
                             │
                      COPY / NORMALIZE
                             │
                             ▼
                        MarketTick
                             │
                             ▼
                    Ingress RingBuffer
                             │
                             ▼
                      Dispatcher Thread
                       /             \
                      /               \
                     ▼                 ▼
           Persistence Queue        Live Queue
                   │                    │
                   ▼                    ▼
           QuestDB Writer          ZeroMQ PUB
                   │                    │
                   ▼                    │
                  QWP                   │
                   │                    ▼
          Store-and-Forward        ZeroMQ SUB
                   │                    │
                   ▼                    ▼
               QuestDB              FastAPI
                   │                /      \
                   │               ▼        ▼
                   │             REST    WebSocket
                   │
                   ├───────────────┐
                   │               │
                   ▼               ▼
              Historical        Latest
                Query           Recovery
                   │
                   ▼
               Parquet
                   │
                   ▼
                DuckDB
                   │
                   ▼
          Research / Backtest


              PostgreSQL
                  │
                  ├── Exchanges
                  ├── Products
                  ├── Contracts
                  ├── Calendars
                  ├── Roll Rules
                  ├── Continuous Mapping
                  └── Reference Data

```

## 4. Technology Stack

### 4.1 Core

Use: C++20, CMake, QuestDB, PostgreSQL, ZeroMQ, MessagePack, Apache Arrow, Apache Parquet, DuckDB, Python 3.12+, FastAPI, Docker Compose.

### 4.2 C++ Libraries

Preferred: Official CTP C++ API, Official QuestDB C/C++ Client, libzmq, cppzmq, msgpack-c, spdlog, fmt, GoogleTest, nlohmann/json, toml++ or YAML parser, libpqxx, Apache Arrow C++ , Apache Parquet C++.
Pin production dependencies to known versions or commit hashes. Do not automatically track dependency main branches.

## 5. Initial Infrastructure Exclusions

Do NOT introduce heavyweight infrastructure in V1 without a demonstrated requirement.
Do not initially use: Kafka, Redpanda, RabbitMQ, Redis Streams, NATS, Flink, Spark, Kubernetes.
Redis itself is not required in V1.
ZeroMQ is explicitly allowed because it serves as an embedded lightweight inter-process live-data transport rather than a persistent message broker.

## 6. Responsibility of Each Storage System

**QuestDB**: Stores high-frequency time-series market data (CTP snapshots, quotes, true trade feeds, order-book data, 1m/5m/15m/30m/1h bars). It is hot market-data storage and time-series query engine. It is NOT the real-time application message bus.
**PostgreSQL**: Stores relational/reference data (exchanges, products, instruments, contracts, calendars, roll rules, configuration, metadata, macro data, fundamentals).
**Parquet**: Stores immutable long-term market history (snapshots, quotes, raw provider data). Uses ZSTD compression.
**DuckDB**: Provides analytical access to Parquet (research, backtesting, scanning, aggregation). DuckDB must not be used as the live market-data database.

## 7. CTP Collector

Implement the CTP market-data collector in C++20 using `CThostFtdcMdApi` and `CThostFtdcMdSpi`.
Required callbacks include: `OnFrontConnected`, `OnFrontDisconnected`, `OnRspUserLogin`, `OnRspSubMarketData`, `OnRtnDepthMarketData`, `OnRspError`.
The collector must support automatic connection, login, reconnect, resubscription, multiple exchanges/instruments, connection health monitoring, and subscription tracking.

## 8. CTP Market-Data Semantics

Standard `OnRtnDepthMarketData()` must be treated as a high-frequency market snapshot feed. Do NOT describe the raw CTP feed as true exchange trade-by-trade data. Internal semantic name `MarketSnapshot` or `MarketTick` is acceptable. Future actual trade-by-trade feeds must be stored separately.

## 9. Critical Callback Rule

The CTP callback thread MUST NEVER perform blocking database, filesystem or network work.
The following is prohibited:

```cpp
void OnRtnDepthMarketData(...) {
    questdb.write(...); // FATAL
}

```

Also prohibited inside the callback: QuestDB writes, ZeroMQ writes, PostgreSQL queries, filesystem writes, blocking mutexes, sleep, large memory allocation, heavy formatting, heavy logging, HTTP calls.
The callback must perform only: pointer validation, receive timestamp capture, field copying, lightweight normalization, sequence assignment, enqueue, return.

## 10. Required Callback Flow

`OnRtnDepthMarketData()` -> capture `recv_ts` -> copy CTP fields -> normalize values -> assign `producer_id` + `seq` -> push `IngressQueue` -> return immediately.

## 11. Internal MarketTick Model

No CTP-specific struct may escape the CTP adapter layer. Create a provider-independent model.
Conceptual structure:

```cpp
struct MarketTick {
    int64_t event_ts_us;
    int64_t recv_ts_ns;
    uint64_t seq;
    ProducerId producer_id;
    ExchangeCode exchange;
    InstrumentCode instrument;
    TradingDay trading_day;
    ActionDay action_day;
    double last_price;
    int64_t volume;
    double turnover;
    double open_interest;
    double upper_limit_price;
    double lower_limit_price;
    std::array<double, 5> bid_price;
    std::array<int32_t, 5> bid_volume;
    std::array<double, 5> ask_price;
    std::array<int32_t, 5> ask_volume;
};

```

Avoid dynamic allocation in the hot path. Use fixed-width strings or interned symbols. Correctness > micro-optimization.

## 12. Stable Event Identity

Each received CTP event must receive: `producer_id` (Generated once when a Collector process instance starts, e.g., UUID) and `seq` (A monotonically increasing local unsigned integer).
The sequence is global to the producer instance, not per instrument. Therefore: `producer_id` + `seq` uniquely identifies a locally received event.

## 13. Why Stable Event Identity Is Required

QWP retransmission may resend an already persisted frame if the server accepted the data but the client did not observe the ACK. Retransmissions must reuse exactly the same `producer_id`, `seq`, and `event_ts`. Never generate a new sequence number during retry.

## 14. Transport Duplicate vs Feed Duplicate

**Transport Duplicate**: CTP event -> QuestDB writes -> ACK lost -> QWP replay. This must NOT produce two stored rows. QuestDB DEDUP handles this.
**Feed Duplicate**: CTP sends Snapshot X -> later CTP sends identical Snapshot X again. These are two upstream events. The raw layer MUST preserve both. Do NOT destructively remove upstream feed duplicates during ingestion.

## 15. Timestamp Model

Preserve: `event_ts`, `recv_ts`, `trading_day`, `action_day`, `producer_id`, `seq`.

**event_ts**
Construct from CTP fields such as: ActionDay, UpdateTime, UpdateMillisec.

**CRITICAL IMPLEMENTATION DETAIL**:
CTP's `ActionDay` might be empty (especially on older front-end servers).
The C++ normalizer MUST implement a fallback logic using the local system time (similar to vnpy's approach).
It must correctly handle 23:59 to 00:00 cross-day edge cases (e.g., if the local clock just crossed midnight but the delayed tick is 23:59:59, or vice versa).
Convert the finalized time to UTC before storage.
CTP's event timestamp currently only requires millisecond-level source precision, so QuestDB TIMESTAMP is sufficient for the initial designated timestamp.

**recv_ts**
Capture as early as possible inside `OnRtnDepthMarketData()`. Use nanosecond-resolution local receive time where supported.

## 16. Clock Synchronization

The production host should run a reliable clock synchronization service (e.g., chrony). Use system clock for persisted receive timestamps. Use steady_clock for measuring durations inside the application.

## 17. Chinese Futures Trading-Day Handling

Never derive: `trading_day = event_ts.date()`.
Chinese futures night sessions make this incorrect. Preserve separately: `event_ts`, `action_day`, `trading_day`.
Example: local event time is `2026-09-03 21:30:00`, trading_day is `2026-09-04`.

## 18. Raw CTP Fields

Fields (Volume, Turnover, OpenInterest, LastPrice, etc.) must initially be stored exactly according to upstream CTP semantics. Do NOT convert cumulative Volume into per-event volume in the raw ingestion path.

## 19. Derived Values

Derived values (delta_volume, VWAP, OHLC, etc.) must NOT replace raw values. Raw values remain immutable.

## 20. Invalid CTP Numeric Values

CTP may use extreme floating-point values for unavailable fields. Implement `is_valid_price(double value)`. Invalid values must become database NULL where appropriate. Never persist values such as `DBL_MAX` or `1.7976931348623157e+308` as legitimate prices.

## 21. Ingress Queue

Use a bounded high-performance queue (SPSC lock-free ring buffer preferred if producer model allows). Initial target capacity: >= 1,000,000 MarketTick.
Expose metrics: capacity, size, usage_ratio, push_total, pop_total, push_failed_total, high_water_mark.
The ingress queue must NEVER silently overwrite unread events.

## 22. Dispatcher

A dedicated Dispatcher thread reads the Ingress Queue. It must fan each event into two independent downstream channels (Persistence Queue, Live Queue). Do NOT allow two consumers to compete for events on one ordinary queue.

## 23. Persistence Queue

Reliability-oriented. Bounded, observable, no silent drop, high-water alerting. If full: CRITICAL log, metric increment, health degradation, alert condition. Data must not simply be overwritten.

## 24. Live Queue

Freshness-oriented. It MAY drop stale snapshots, coalesce updates, replace older quote with latest quote. Loss on this path must NOT imply persistence loss.

## 25. QuestDB Transport

Primary protocol is QWP over WebSocket. Use the official QuestDB C/C++ client. Do NOT manually construct ILP strings for V1 writer.

## 26. QuestDB Connection

Externalized configuration (e.g., `QDB_HOST=127.0.0.1`, `QDB_SF_DIR=/var/lib/market-data/qwp-spool`). The production configuration must enable an on-disk Store-and-Forward directory.

## 27. QWP Delivery Semantics

Assume at-least-once delivery from the reconnect/replay layer. QuestDB tables receiving QWP Store-and-Forward data MUST provide stable row identity and enable DEDUP.

## 28. QuestDB Deduplication Requirement

Use `event_ts`, `producer_id`, `seq` as the storage deduplication key. `producer_id` and `seq` MUST remain unchanged across retransmission.

## 29. QuestDB Core Schema

Initial schema:

```sql
CREATE TABLE ctp_market_data (
    event_ts TIMESTAMP,
    recv_ts TIMESTAMP_NS,
    producer_id SYMBOL,
    seq LONG,
    exchange SYMBOL,
    instrument SYMBOL,
    trading_day SYMBOL,
    action_day SYMBOL,
    last_price DOUBLE,
    volume LONG,
    turnover DOUBLE,
    open_interest DOUBLE,
    upper_limit_price DOUBLE,
    lower_limit_price DOUBLE,
    bid_price1 DOUBLE, bid_volume1 INT, ask_price1 DOUBLE, ask_volume1 INT,
    bid_price2 DOUBLE, bid_volume2 INT, ask_price2 DOUBLE, ask_volume2 INT,
    bid_price3 DOUBLE, bid_volume3 INT, ask_price3 DOUBLE, ask_volume3 INT,
    bid_price4 DOUBLE, bid_volume4 INT, ask_price4 DOUBLE, ask_volume4 INT,
    bid_price5 DOUBLE, bid_volume5 INT, ask_price5 DOUBLE, ask_volume5 INT
)
TIMESTAMP(event_ts)
PARTITION BY DAY WAL
DEDUP UPSERT KEYS(
    event_ts,
    producer_id,
    seq
);

```

## 30. QuestDB Deduplication Acceptance Rules

Integration tests must demonstrate:

* Transport replay (insert identical `event_ts`, `producer_id`, `seq` twice -> 1 stored row).
* Feed Duplicate (insert same market data but seq=100 and seq=101 -> 2 stored rows).

## 31. QuestDB Batch Writer

Never flush individually. Initial defaults: `MAX_BATCH_ROWS=500`, `MAX_BATCH_LATENCY_MS=20`. Flush when either condition occurs first.

## 32. QuestDB Flush Semantics

Writer must distinguish "accepted by local client queue" from "acknowledged by QuestDB". During shutdown: stop ingestion, drain queue, flush, wait for ACK, close client.

## 33. QuestDB Failure Behavior

If QuestDB is unavailable, CTP reception SHOULD continue while local capacity exists, relying on Persistence Queue + QWP Store-and-Forward. No database outage may remain invisible.

## 34. ZeroMQ Live Transport

Use ZeroMQ PUB/SUB for C++ -> FastAPI real-time IPC. C++ side: PUB. Python side: SUB. Initial deployment: `tcp://127.0.0.1:5556`.

## 35. ZeroMQ Reliability Semantics

Best-effort distribution. NOT a persistence mechanism. Dropped live updates are acceptable provided QuestDB persistence remains unaffected.

## 36. ZeroMQ Publisher Thread

Never call `zmq_send()` inside the CTP callback. Flow must be: Callback -> IngressQueue -> Dispatcher -> LiveQueue -> ZeroMQ Publisher Thread -> PUB socket.

## 37. Live Wire Format

Do NOT send raw C++ struct memory over ZeroMQ. Use a versioned serialization format (MessagePack for V1).

## 38. ZeroMQ Multipart Format

Frame 1 Topic: `<exchange>.<instrument>` (e.g., `SHFE.zn2610`). Frame 2: MessagePack payload (schema_version, event_ts, recv_ts, producer_id, seq, etc.).

## 39. Message Schema Versioning

Every live payload must contain `schema_version`. Initial: `1`.

## 40. FastAPI ZeroMQ Subscriber

FastAPI runs a background async task. Connects to SUB, decodes MessagePack, validates schema, updates local cache, sends to WebSockets.

## 41. FastAPI Latest Quote Cache

Maintain in-process cache `LatestQuoteCache` mapping `(exchange, instrument)` -> latest `MarketTick`. Feeds REST and WS. Do NOT query QuestDB for every quote request.

## 42. FastAPI Startup Recovery

Startup process: 1. Start ZeroMQ SUB. 2. Buffer incoming messages. 3. Query QuestDB for latest persisted snapshots. 4. Init cache. 5. Reconcile buffered messages. 6. Normal live mode.

## 43. Latest Quote Conflict Resolution

When reconciling, use `event_ts`, `recv_ts`, `producer_id`, `seq`. For same producer, higher seq is later. Across producers, use timestamps.

## 44. FastAPI Worker Requirement

V1 must run 1 FastAPI worker (e.g., `uvicorn --workers 1`) because the latest cache and WS manager are process-local.

## 45. WebSocket API

Provide `WS /v1/stream/quotes`. MUST consume live feed from ZeroMQ. MUST NOT poll QuestDB.

## 46. Live Backpressure Policy

A slow browser must not block FastAPI, ZeroMQ, or C++ Collector. Drop/coalesce stale snapshots for slow clients.

## 47. PostgreSQL Initial Schema

Implement tables: `exchanges`, `products`, `futures_contracts`, `trading_calendar`, `trading_sessions`, `continuous_contract_mapping`.

## 48. Continuous Contract Rules

Raw physical futures must always be preserved. Continuous instruments (ZN_MAIN) are derived data. Roll rules must be explicit and configurable.

## 49. Parquet Archive

Implement historical cold storage (e.g., `data/market/ctp/exchange=SHFE/instrument=zn2610/trading_day=2026-09-04/part-0000.parquet`). Compression: ZSTD.

## 50. Archive Strategy

Configurable retention (e.g., QuestDB holds 3–12 months, Parquet holds complete history). Do not delete QuestDB data until archive verification completes.

## 51. Archive Verification

Before deleting hot data, verify: row count, timestamps, instrument set, Parquet readability.

## 52. DuckDB Research Layer

Provide Python utilities querying Parquet directly (e.g., `SELECT * FROM read_parquet(...)`). Provide API helpers `load_ticks`, `load_bars`.

## 53. Bar Generation

Generate bars (1m, 5m, 1h, 1d) from stored market snapshots. `bar_volume` must be calculated from cumulative volume differences. Handle night sessions, day transitions.

## 54. Do Not Poll QuestDB as a Message Bus

Prohibited live architecture: CTP -> QuestDB -> FastAPI polling -> WebSocket. QuestDB is for persistence/history.

## 55. Future Live Trading Architecture

A future strategy engine must consume the in-memory event stream directly via Dispatcher/Live Queue, NOT by querying QuestDB.

## 56. REST API

Initial endpoints: `GET /health`, `GET /v1/instruments`, `GET /v1/quotes/{symbol}`, `GET /v1/bars/{symbol}`, etc.

## 57. Configuration

Use YAML files (`app.yaml`, `ctp.yaml`, etc.) plus environment variables (`CTP_BROKER_ID`, `QDB_HOST`, `ZMQ_PUB_ENDPOINT`).

## 58. Secrets

Never commit passwords, AppIDs, AuthCodes, API keys. Provide `.env.example`.

## 59. Logging

Structured logging (spdlog). Levels TRACE to CRITICAL. Log connections, errors, queue pressure. Do NOT log every Tick at INFO.

## 60. Metrics

Expose metrics for CTP, Ingress Queue, Persistence Queue, QuestDB, Live, API. Design for future Prometheus integration.

## 61. Health Model

Expose `GET /health` with individual states and overall HEALTHY/DEGRADED/UNHEALTHY status.

## 62. Data Integrity Rules

Mandatory: no silent raw tick drops, raw fields preserved, stable identity, deduplicated replay, preserved feed duplicates, observable failures.

## 63. Graceful Shutdown

On SIGINT/SIGTERM: stop inputs, drain queues, flush QuestDB, wait for ACK, stop publishers, exit cleanly.

## 64. Crash Recovery

Persistence recovery relies on QWP Store-and-Forward + QuestDB DEDUP. Replayed messages retain original `producer_id`, `seq`, `event_ts`.

## 65. Synthetic Market Data Generator

Implement before real CTP. Configurable rates (1 to 50k ticks/s) and symbols. Used for tests, benchmarks, development.

## 66. Unit Tests

Test: UTC conversion, night-session handling, numeric normalization, queue overflow, batch flush, MessagePack encode/decode.

## 67. QuestDB Integration Tests

Test: Synthetic Tick -> Queue -> QWP -> QuestDB. Verify rows, fields, identity.

## 68. QWP Replay / DEDUP Tests

Mandatory test: Send identical `producer_id` + `seq` twice -> expect 1 row. Send seq 1 then 2 with identical market data -> expect 2 rows.

## 69. Live IPC Integration Tests

Test: Synthetic -> ZMQ PUB -> SUB -> FastAPI Cache. Verify routing, decoding, updates.

## 70. WebSocket Integration Tests

Test: Synthetic -> ZMQ -> FastAPI -> WS -> Client.

## 71. FastAPI Restart Test

Test: Stop FastAPI, produce data, restart FastAPI, recover state from QuestDB, resume ZMQ.

## 72. Performance Targets

Target: >= 10,000 snapshots/sec sustained. Callback p99 < 100 microseconds. Measure before optimizing.

## 73. Latency Metrics

Measure callback duration, queue latency, flush latency, ZeroMQ latency independently.

## 74. Project Structure

Provide standard C++ (`cpp/src`, `cpp/include`) and Python (`python/api`) directory separation alongside SQL, Docker, config, and docs directories.

## 75. Docker Compose

Provide dev env with QuestDB, PostgreSQL, FastAPI. CTP Collector may run on host initially.

## 76. Coding Principles

Correctness > optimization. RAII, bounded queues, clear thread ownership, minimal allocation in hot paths.

## 77. Thread Ownership

Every thread must have documented ownership. Do not casually share QuestDB sender or ZMQ sockets across threads unsafely.

## 78. Development Phases

* Phase 0: Bootstrap (CMake, Docker, Config)
* Phase 1: Core Pipeline (MarketTick, QWP, DEDUP, Synthetic)
* Phase 2: Live IPC (ZMQ PUB/SUB)
* Phase 3: Web API (REST, WS)
* Phase 4: CTP Integration
* Phase 5: PostgreSQL Metadata
* Phase 6: Historical Archive
* Phase 7: Derived Data
* Phase 8: Monitoring & Quality

## 79. V1 Definition of Done

Complete when CTP flows to QuestDB and WebSocket reliably. Reconnect works. DEDUP works. No silent losses. Tests pass.

## 80. Non-Goals for V1

No HFT execution engine, Kafka, Kubernetes, or complex ML platforms in V1.

## 81. Architecture Invariants

* Invariant 1: CTP callback != database writer
* Invariant 2: QuestDB != live message bus
* Invariant 3: ZeroMQ != reliable persistence layer
* Invariant 4: Persistence path = durability first
* Invariant 5: Live path = freshness first
* Invariant 6: QWP replay + stable identity + DEDUP = duplicate-safe persistence
* Invariant 7: Transport duplicate != feed duplicate
* Invariant 8: Raw data = immutable
* Invariant 9: Live trading strategy != QuestDB polling

## 82. Instructions to Codex

* Read this entire REQUIREMENTS.md before changing code.
* Implement one phase at a time.
* Keep MarketTick provider-independent.
* Never perform database I/O inside CTP callbacks.
* Never perform ZeroMQ I/O inside CTP callbacks.
* **When implementing timestamp normalization in C++, explicitly handle the case where CTP's ActionDay is empty by falling back to local system time and resolving 23:59/00:00 day-crossover offsets.**
* Never silently discard persistence data.
* Preserve upstream feed duplicates in raw data.
* Deduplicate transport replay using stable identity.
* Use QWP with Store-and-Forward for QuestDB.
* Use QuestDB DEDUP.
* Use ZeroMQ only for IPC distribution.
* Use one FastAPI worker in V1.
* Maintain wire-protocol versioning (MessagePack).
* If ambiguous, choose the simplest implementation compatible with invariants.

## 83. First Codex Task

Read REQUIREMENTS.md completely.
Implement Phase 0 and Phase 1 only.
Do not implement real CTP or WebSocket yet.
Required output: Repo structure, CMake, Docker Compose, Config, Logging, MarketTick, Queues, QWP Writer, DEDUP schema, Synthetic generator, and Tests.
Before writing: Inspect repo, produce plan, list files, identify conflicts.

```

```
