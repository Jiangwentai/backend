# Phase 12 — Provider selection, arbitration, and failover

Phase 12 is implemented as a read-side projection after `LatestQuoteCache`. All CTP, Synthetic, AKShare, and future IBKR observations remain independently addressable by `(provider, exchange, instrument)`. Selection does not write to the cache, publish a new event, or alter QuestDB identity.

Modes:

- `explicit` (default): one observation may be returned directly; multiple providers require `?provider=` and otherwise return HTTP 409.
- `preferred`: select the first fresh provider in `PROVIDER_PREFERENCE`.
- `ranked`: rank fresh observations by declared quality, then provider preference and receive time.

Moving away from the first preferred provider requires `PROVIDER_FALLBACK_ENABLED=true`. Stale observations are excluded unless `PROVIDER_ALLOW_STALE=true`. Explicit provider requests remain available for diagnosis and expose staleness rather than hiding the observation.

Configuration:

```dotenv
PROVIDER_SELECTION_MODE=explicit
PROVIDER_PREFERENCE=ctp,ibkr,synthetic,akshare
PROVIDER_FALLBACK_ENABLED=false
PROVIDER_ALLOW_STALE=false
PROVIDER_DISCREPANCY_BPS=20
```

Example controlled failover:

```sh
PROVIDER_SELECTION_MODE=preferred \
PROVIDER_PREFERENCE=ctp,akshare \
PROVIDER_FALLBACK_ENABLED=true \
docker compose up -d api
```

Use `GET /v1/quotes/SHFE.rb2610` for the configured selection, `GET /v1/quotes/SHFE.rb2610?provider=ctp` for an exact source, and `GET /v1/provider-selection/SHFE.rb2610` for decision inputs and discrepancy diagnostics.

The diagnostic computes the maximum fresh-provider last-price spread in basis points. It does not average prices, reject upstream events, or initiate failover by itself. Selection, failover, no-eligible-provider, and discrepancy counters are exported on `/metrics`.
