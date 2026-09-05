# Phase 11B — AKShare intraday bars and best-effort quotes

Phase 11B adds two deliberately separate paths. `futures_zh_minute_sina(symbol, period="1")` produces historical `1m` bars, archived before normalization and stored through the common `historical_bars` repository. `futures_zh_spot` is polled by a dedicated worker and produces only `QuoteSnapshot` events through `LiveEventIngress`.

Canonical instruments must already have a PostgreSQL `provider_instruments` mapping. The worker never guesses provider symbols. Minute timestamps are interpreted in `Asia/Shanghai`; ordinary night sessions beginning at 21:00 map to the next weekday trading day. Exchange holidays require the metadata calendar and are not invented when unavailable.

The Sina minute endpoint has no start/end parameters and returns a bounded recent window. CLI ranges therefore filter returned rows and report `coverage_complete=false`; they never claim a requested historical range is complete. Re-running overlapping windows is safe because canonical writes deduplicate and revisions are recorded while each raw response remains immutable.

## Operations

```sh
docker compose --profile akshare build akshare-worker akshare-quotes
docker compose --profile akshare run --rm akshare-worker \
  fetch futures-1m --instrument SHFE.rb2610 --start 2026-09-01 --end 2026-09-05
docker compose --profile akshare run --rm akshare-worker \
  backfill futures-1m --instrument SHFE.rb2610 --state /raw/backfill-1m.json
docker compose --profile akshare run --rm akshare-worker quote SHFE.rb2610
```

For continuous polling set `AKSHARE_REALTIME_ENABLED=true`, set comma-separated canonical `AKSHARE_QUOTE_INSTRUMENTS`, start `akshare-quotes`, and configure the API with `AKSHARE_ZMQ_SUB_ENDPOINT=tcp://akshare-quotes:5557` (or the corresponding host endpoint). Polling defaults to disabled; the enforced minimum is three seconds. Timeout, batch size, concurrency, stale threshold, and interval are configurable. `AKSHARE_REALTIME_PERSIST=true` adds the generic live persistence transport.

AKShare quotes remain `provider=AKSHARE`, `quality=BEST_EFFORT`, and expose receive time, timestamp provenance, age, and stale state. They coexist with CTP in the provider-aware cache and are returned only when explicitly requested with `provider=akshare`; there is no arbitration or fallback.

The provider and upstream source remain separate (`AKSHARE` versus `SINA`). Missing depth stays null. The retained QuestDB table name `ctp_market_data` is provider-specific technical debt, although its schema and DEDUP identity are provider-aware; this phase intentionally avoids a cosmetic migration.
