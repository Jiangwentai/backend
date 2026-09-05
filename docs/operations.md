# Monitoring, quality, and alert operations

## Metric ownership

- The collector owns `Pipeline::metrics()`: input/CTP, ingress queue, persistence queue, QuestDB writer, live queue, ZeroMQ publisher, and dispatcher state.
- FastAPI owns `/metrics`: HTTP request totals/duration, live SUB decoding/cache updates, WebSocket delivery/backpressure, and its view of QuestDB/PostgreSQL health.
- QuestDB owns its native Prometheus endpoint on port `9003`. PostgreSQL should be monitored by the deployment's normal database exporter when required.

Do not add counters from independent processes together unless their delivery boundary and reset lifecycle are understood. A live-path drop does not imply persistence loss; any persistence rejection or writer failure is a durability incident.

## Minimum alerts

- Page immediately when persistence `push_failed_total`, input `rejected_total`, or QuestDB writer failures increase.
- Page when the dispatcher is degraded or the QuestDB writer reports unhealthy.
- Warn when ingress or persistence queue usage exceeds 80%; page above 95% if sustained.
- Warn when live feed health is stale during an expected trading session. Silence this alert outside the product's configured trading sessions.
- Warn on increases in ZeroMQ send failures, live decode failures, WebSocket send failures, or live/WebSocket drops. These affect freshness, not persisted completeness.
- Alert on sustained API 5xx responses or dependency health gauges at zero.
- Fail the archive promotion/retention job when the quality command exits non-zero. Never delete hot data after a failed archive or quality verification.

## Archive quality semantics

Errors are duplicate/missing stable identity, malformed trading day, negative cumulative volume, non-finite persisted price, or receive time preceding event time. Any error makes the report `FAIL` and exit status 1.

Cumulative-volume decreases within one trading day and crossed best bid/ask are warnings. Operators should investigate them, but the checker does not mutate raw history because provider feed anomalies and session behavior must remain observable.

Quality checks first re-run each Phase 6 manifest/readability verification, then aggregate checks in DuckDB. Run them only on closed partitions. Preserve the JSON report with the archive job record before applying any separate QuestDB retention operation.
# Historical acquisition

Run one bounded queue worker with the AKShare profile using the
`historical-fetch-worker` service. The `akshare-worker` service performs only the configured
low-frequency scheduled enqueue cycle. Monitor queued/running/failed rows in
`historical_fetch_requests` and provider cooldown/backoff in
`historical_provider_refresh_state`. Rate-limit, schema, mapping, empty response and
unsupported-range failures are recorded separately. No job requires Redis, Kafka, or an
unbounded task pool.
