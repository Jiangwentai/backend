# Architecture through Phase 9

The C++ process owns ingestion, dispatch, QuestDB QWP persistence, and ZeroMQ publication. The Python process owns one ZeroMQ SUB socket, the process-local latest-quote cache, REST routes, and WebSocket connections. No WebSocket or REST operation runs on the persistence path.

```text
Synthetic Provider -> provider SPSC --.
                                      +-> Dispatcher -> PersistenceQueue -> QWP/SF -> QuestDB
CTP Provider ------> provider SPSC --'       `-------> LiveQueue -> ZMQ PUB -> ZMQ SUB
                                                                          |
                                                              Provider-aware cache
                                                                   /          \
                                                                REST       WS Manager
```

Synthetic and CTP may be enabled concurrently. Each provider owns its producer identity and SPSC ingress queue; the Dispatcher is the sole consumer and performs round-robin fan-in. For CTP, `OnRtnDepthMarketData` captures receive time, copies fixed-size fields into the canonical snapshot, normalizes it, assigns identity and sequence, attempts one non-blocking enqueue, and returns. It performs no database, network, filesystem, sleep, blocking-lock, or heavy logging work.

The CTP adapter is the only translation unit that includes proprietary SDK headers. Desired subscriptions survive disconnects; active subscriptions are cleared on disconnect and rebuilt after reconnect, optional authentication, and login. Shutdown unregisters/releases MdApi first, then drains ingress and persistence, waits for the QuestDB ACK, and allows the live publisher to drain.

CTP support is build-time optional. The repository makes no SDK-version claim: CMake probes the operator-supplied headers for authentication support. The SPSC boundary assumes a single MdApi instance serializes market-data callbacks; deployments whose supplied SDK documents concurrent `OnRtnDepthMarketData` calls require revisiting this boundary before production use.

FastAPI uses lifespan ownership. Startup creates cache and manager, starts SUB first, waits until its socket is initialized, then loads one latest QuestDB row per `(exchange, instrument)`. Live frames received during the query are already placed in the cache. Recovery rows pass through the same conflict resolver: same-producer higher `seq` wins; across producers timestamps decide. Only then is the API ready. Failure to recover leaves the service available but degraded.

Shutdown rejects new WebSocket clients, stops and closes SUB, closes all WebSockets and sender tasks, then closes the QuestDB HTTP client.

Phase 5 adds a separate PostgreSQL metadata plane. Exchanges, products, physical futures contracts, calendars, sessions, roll-rule definitions, and dated continuous-contract mappings live in normalized relational tables. FastAPI uses a bounded asyncpg pool for `GET /v1/instruments`; quote delivery remains entirely independent of PostgreSQL. PostgreSQL failure degrades health and returns `503` for metadata queries without interrupting the live or persistence data planes.

Phase 6 adds an offline, independently invoked archive plane. It pages a closed `(exchange, instrument, trading_day)` selection from QuestDB in stable event/identity order, converts it to the explicit market-data Arrow schema, and writes ZSTD Parquet under Hive-style directories. A completion manifest is published only after the Parquet file is readable and its row count, timestamp bounds, instrument set and compression match. Completed partitions are immutable and idempotently re-verified.

Archiving never runs in the collector callback or FastAPI lifespan. Source deletion is deliberately not automated because QuestDB does not support filtered row deletion; operators may apply TTL or drop a complete physical day only after all logical archives for that day are verified.

Phase 7 adds a read-only DuckDB research boundary over completed Parquet partitions. Tick scans and bar generation never modify the Phase 6 archive. The bar query orders snapshots by `(event_ts, producer_id, seq)`, computes volume differences inside each physical contract and preserved trading day, and aggregates supported 1m, 5m, 1h, and 1d intervals. Daily bars group on `trading_day`, not the UTC date. Continuous-series reads consume explicit dated PostgreSQL mappings and retain the selected physical instrument on every output row.

The historical bars REST route dispatches synchronous DuckDB work to a worker thread. It is an on-demand research/history path and does not participate in ZeroMQ live distribution or QuestDB persistence.

Phase 8 exposes the existing bounded-plane counters without joining their ownership domains. The collector provides an atomic metrics snapshot and structured lifecycle summary; FastAPI provides Prometheus text and health endpoints; QuestDB retains its native metrics endpoint. The API metrics endpoint observes Python/IPC behavior only and does not pretend to own collector or database-process metrics.

Archive quality checks run offline against verified, immutable partitions. Integrity failures produce a non-zero exit code suitable for CI or scheduled operations. Warning checks remain advisory and never rewrite, deduplicate, or delete raw snapshots.

Phase 9 introduces the provider boundary. Realtime adapters implement a common lifecycle and event sink; historical and reference capabilities remain segregated. `ProviderManager` owns startup rollback, reverse shutdown, subscription routing, capabilities, and health. The canonical model adds provider, event type, canonical instrument, and quality metadata and reserves quote, trade, bid/ask, depth, and bar variants. Current shared sinks support quote snapshots only.

QuestDB DEDUP identity is now `(event_ts, provider, producer_id, seq)`. MessagePack writers emit schema v2 while readers accept v1; API cache and WebSocket coalescing distinguish providers. PostgreSQL records provider registrations and provider-symbol mappings. New Parquet files use schema v2 while DuckDB reads old schema-v1 archives with defaults. IBKR, AKShare, automatic failover, and arbitration are deliberately deferred.

V1 must run exactly one Uvicorn worker because cache, subscriptions, and metrics are process-local. Multi-worker synchronization is intentionally deferred; no Redis or broker is introduced.
