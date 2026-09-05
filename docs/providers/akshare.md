# AKShare historical, intraday, and best-effort quote provider

## Purpose and boundary

AKShare runs as independently restartable Python workers. Historical acquisition never enters the realtime path. Optional quote polling runs outside FastAPI and native callbacks, then enters the provider-independent live ingress. CTP remains the primary Chinese-futures realtime source; AKShare is explicitly best-effort and can only be selected as fallback under an explicit Phase 12 policy.

The optional dependency is pinned to `akshare==1.18.74` in `python/requirements-akshare.txt`. Enabling the worker without that package produces a provider-specific installation error.

## Production endpoint registry

| Dataset | AKShare function | Upstream | Status | Destination |
|---|---|---|---|---|
| `futures_daily_sina` | `futures_zh_daily_sina(symbol)` | Sina Finance | stable/enabled | raw Parquet + canonical QuestDB daily bars |
| `futures_1m_sina` | `futures_zh_minute_sina(symbol, period="1")` | Sina Finance | stable/enabled | raw Parquet + canonical QuestDB 1-minute bars |
| `futures_realtime_quote` | `futures_zh_spot(symbols, market, adjust="0")` | Sina Finance | stable/enabled, runtime opt-in | shared realtime ingress |
| `futures_contracts_qihuo` | `futures_comm_info(symbol="所有")` | 九期网/9QIHUO | stable/enabled | raw Parquet + PostgreSQL reference records |
| `futures_contracts_sina` | `futures_display_main_sina()` | Sina Finance | experimental/disabled | none; 1.18.74 implementation is incompatible |
| `futures_inventory_99` | `futures_inventory_99(symbol)` | 99QH | experimental/disabled | none until accepted |
| `futures_positions_sina` | `futures_hold_pos_sina(symbol, contract, date)` | Sina Finance | experimental/disabled | none until accepted |

Only registered, enabled endpoints can run. The two experimental entries document discovery without silently expanding production behavior.

## Identity, mapping, and lineage

Provider-native symbols are normalized for lookup but never treated as canonical IDs. `provider_instruments` must contain an `akshare` mapping to an existing physical contract. An unresolved symbol leaves its raw archive intact, creates `provider_unresolved_instruments`, rejects canonical rows, increments mapping/rejection metrics, and makes the ingestion run `PARTIAL`.

Every fetch receives a UUID `fetch_id`. Its manifest and rows retain the AKShare function, upstream source, raw request parameters, fetch time, package version, schema version, row count, and SHA-256. Raw provider columns remain intact.

Raw layout:

```text
<AKSHARE_RAW_ROOT>/provider=akshare/dataset=<dataset>/fetch_date=YYYY-MM-DD/
  fetch_id=<uuid>/raw.parquet
  fetch_id=<uuid>/manifest.json
```

Directories are created exclusively, so repeated fetches never overwrite older responses.

## Daily bars and revisions

`futures_zh_daily_sina` is called with its verified `symbol` parameter and expected columns `date/open/high/low/close`, with optional `volume/hold/settle`. Dates, nullable numeric types, NaN, duplicates, and OHLC ranges are validated explicitly. The provider date is the futures trading day; local Asia/Shanghai midnight is converted to UTC for `bar_start`.

Canonical identity is `(provider, instrument_id, interval, bar_start)`, independent of `fetch_id`. QuestDB DEDUP exposes the latest accepted bar. PostgreSQL `historical_bar_versions` stores the current supplied payload, while every changed repeat creates a `historical_bar_revisions` row containing old/new payloads and fetch IDs. Both raw fetches remain recoverable.

AKShare continuous symbols are supplemental comparison/reference data and never replace internal roll rules or `continuous_contract_mapping`.

## One-minute bars

Minute rows require aligned timestamps and complete, consistent OHLC values. Source times are interpreted in `Asia/Shanghai` and stored as UTC. A 21:00-or-later ordinary night session maps to the next weekday trading day; authoritative holiday exceptions require exchange-calendar metadata and are never guessed. Missing volume/open interest remains null.

The Sina endpoint accepts no date range and exposes only a bounded recent window. Requested CLI ranges filter that response and report incomplete coverage. Full arbitrary-range claims and artificial pagination are intentionally avoided. Overlapping refreshes remain idempotent and revision-aware.

## Best-effort quotes

Quote subscriptions use canonical names and resolve through `provider_instruments`; batching is grouped by commodity (`CF`) versus financial (`FF`) markets. Every event is `QuoteSnapshot`, `provider=AKSHARE`, and `quality=BEST_EFFORT`. Receive time is always present; upstream event time is used only when both upstream date and time are trustworthy. Missing book levels remain null. Consecutive equal polls receive distinct sequence numbers because they are separate observations.

The poller enforces a three-second minimum, bounded request timeout/concurrency/batches, client pacing, bounded exponential retry, clean shutdown, health state, and quote metrics. It publishes through generic `LiveEventIngress`; the adapter itself knows nothing about ZeroMQ, FastAPI, or QuestDB. Persistence is off by default and, when enabled, uses generic `QuestDbLivePersistence`. The legacy `ctp_market_data` name is known provider-specific technical debt, while its identity is provider-aware.

## Operations

Build and inspect datasets:

```sh
docker compose --profile akshare build akshare-worker
docker compose --profile akshare run --rm akshare-worker list-datasets
docker compose --profile akshare run --rm akshare-worker metrics
docker compose --profile akshare run --rm akshare-worker refresh-reference
docker compose --profile akshare run --rm akshare-worker fetch futures-daily --instrument SHFE.rb2610 --start 2024-01-01
docker compose --profile akshare run --rm akshare-worker fetch futures-1m --instrument SHFE.rb2610
docker compose --profile akshare run --rm akshare-worker backfill futures-1m --instrument SHFE.rb2610 --state /raw/backfill.json
docker compose --profile akshare run --rm akshare-worker quote SHFE.rb2610
docker compose --profile akshare run --rm akshare-worker unresolved-symbols
```

For scheduled collection set `AKSHARE_SYMBOLS` and start the profile. The default interval is 86400 seconds. Requests use conservative bounded concurrency, a provider-wide minimum interval, and bounded exponential backoff with jitter. Schema, mapping, validation, and parameter errors are permanent and are not retried.

The backfill state file is atomically replaced after each instrument. Completed instruments are skipped on restart; failures remain listed for operator action. `GET /v1/providers/akshare/health` derives independent worker health from ingestion-run metadata. Normal `/v1/bars` requests never call AKShare synchronously.

Canonical rows are queried through the existing API with `GET /v1/bars/SHFE.zn2610?interval=1d&provider=akshare`; responses add `provider`, `source`, `upstream_source`, and `settlement` without removing the established bar fields.

## Troubleshooting and limitations

- Apply PostgreSQL migration `004_akshare.sql` and QuestDB migration `007_historical_bars.sql` before starting the worker.
- A missing/renamed/unexpected column fails visibly after raw preservation; meanings are never guessed.
- Empty daily/reference responses are errors, not assumed successful zero-row datasets.
- `futures_display_main_sina` is disabled because AKShare 1.18.74 passes a string result from `match_main_contract` into `pandas.concat`; it must be re-verified after an upstream fix.
- Inventory and position endpoints are unstable and unscheduled.
- Quote polling is public-source best effort, not exchange-direct realtime. Market depth, tick reconstruction, provider arbitration, and fallback are not implemented.
