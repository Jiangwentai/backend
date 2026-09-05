# Historical coverage and provider selection

Historical rows are independent provider observations. Their QuestDB identity remains
`(bar_start, provider, instrument_id, interval)`; provider disagreement is not a revision,
and composite results are never persisted.

Coverage compares a single range query against canonical expected bar starts generated
from PostgreSQL `trading_calendar` and the applicable product `trading_sessions`. Closed
days, lunch breaks and out-of-session bars do not improve coverage. Night sessions use
`night_session_open`, and crossing sessions retain the exchange trading day association.
`bar_start` means the inclusive start of the canonical interval in UTC.

Selection is opt-in on the existing route:

```text
GET /v1/bars/SHFE.rb2610?interval=1m&start_day=20260901&end_day=20260930&selection=single
GET /v1/bars/SHFE.rb2610?interval=1m&start_day=20260901&end_day=20260930&selection=composite
```

`SINGLE` chooses one provider for the whole range. Providers meeting
`HISTORICAL_MINIMUM_COVERAGE` are ordered by configured priority, quality, coverage, then
provider ID. `COMPOSITE` chooses the highest-ranked complete provider bar at each expected
timestamp; it never averages, interpolates, or merges fields. Every returned bar retains
its actual provider and a primary/fallback reason. `require_complete=true` returns a
structured HTTP 409 when the selected result is incomplete.

`GET /v1/historical-coverage` returns per-provider range coverage. Current provider health
does not exclude stored observations. Fetch success and range completeness are separate.

For compatibility, `/v1/bars` without selection continues reading the existing CTP archive,
and a plain `provider=` query continues returning the existing provider-only list. Supplying
`selection` (or strict completeness) returns the metadata envelope.

Policy is configured as comma-separated `provider:priority:quality` entries:

```text
HISTORICAL_PROVIDER_POLICY=ctp:100:DERIVED,ibkr:90:BROKER,akshare:50:PUBLIC
HISTORICAL_MINIMUM_COVERAGE=0.95
```

This layer is read-only. It does not schedule, fetch, or refresh missing history.

## Acquisition coordinator

Historical acquisition is a separate process from reads. `POST /v1/history/ensure` performs
only local coverage/freshness, deduplication, cooldown and capability checks, then inserts a
PostgreSQL request. It never calls a provider. `historical-fetch-worker` claims bounded work
with `FOR UPDATE SKIP LOCKED`, runs the normal provider raw-archive/normalize/store path, and
records SUCCESS, PARTIAL or categorized failure state.

Historical capability checks are interval- and market-specific. They include provider,
upstream source, domestic/foreign market (and optionally instrument kind), interval and range
semantics. A realtime snapshot capability never implies historical minute backfill. AKShare
foreign futures are eligible for confirmed daily history but not historical `1m`; without
another capable provider the coordinator returns `NO_ELIGIBLE_PROVIDER` with
`INTERVAL_NOT_SUPPORTED` before provider I/O.

Logical active requests are keyed by provider, upstream source, canonical instrument, interval and range.
An identical or contained request returns `ALREADY_RUNNING`. Provider/interval state holds
the last attempt/success and `next_allowed_at`; successful work enters the configured
cooldown, while network/rate failures use bounded exponential backoff. `force` may bypass a
success cooldown and complete/old-range skips, but never failure backoff or concurrency.

Scheduled refresh runs once per configured low-frequency cycle over pinned instruments plus
recently requested instruments. It requests only the recent mutable window. Complete old
history is not routinely refreshed; recent complete history may be revisited for same-provider
corrections. One failed instrument produces a PARTIAL scheduled cycle and does not stop peers.

Freshness is distinct from coverage. It compares the latest stored expected bar with the
latest expected session bar; a market-closed instrument is fresh when its last close bar is
present. Missing authoritative session metadata yields UNKNOWN rather than a guess.

Commands:

```text
historical-worker ensure --instrument SHFE.rb2610 --interval 1m --start ... --end ...
historical-worker run-scheduled-refresh --interval 1m
historical-worker run-fetch-worker [--once]
historical-worker fetch-status UUID
```

AKShare 1-minute acquisition is explicitly marked as a bounded recent-window capability and
does not repeatedly attempt impossible old ranges. The worker does not continuously call
AKShare: it only processes queued work. Acquisition-provider fallback defaults off and is
configured independently from Task 2 read selection.
