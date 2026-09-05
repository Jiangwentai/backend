Task 3 historical acquisition coordination is complete (2026-09-06). It adds scheduled and on-demand queued acquisition with persistent deduplication, cooldown/backoff, bounded provider concurrency, and local-only reads. It does not implement later phases.

Phase 11B — AKShare Intraday Bars and Best-Effort Realtime Quotes

Implementation status: complete (2026-09-05). Internet-dependent endpoint smoke tests remain operator-run; CI uses deterministic fakes.

1. Objective

Extend the existing AKShare provider with two additional capabilities:



1-minute historical/intraday futures bars

best-effort realtime quote polling

These are separate capabilities and MUST use separate logical pipelines.

Phase 11B MUST preserve all architectural invariants established in Phase 0–11A.

The purpose of this phase is to allow the backend to:



fetch AKShare futures 1-minute bars;

persist canonical 1-minute bars;

archive raw intraday fetches;

query stored 1-minute bars through FastAPI;

backfill historical 1-minute data;

periodically refresh recent intraday bars;

optionally poll AKShare for best-effort quote snapshots;

expose AKShare live quotes through the existing provider-aware realtime pipeline.

AKShare MUST NOT be treated as equivalent to CTP or other exchange/native realtime feeds.

2. Scope

Phase 11B contains two sub-capabilities:



11B.1 AKShare Intraday Historical Bars

11B.2 AKShare Best-Effort Realtime Quotes

They share:



provider registration;

provider symbol mappings;

health reporting;

observability;

configuration;

AKShare client abstraction.

They MUST NOT share storage semantics or event semantics where those differ.

3. Architectural Overview

3.1 Historical 1-Minute Path

AKShare

↓

AkshareClient

↓

FuturesMinuteBarEndpointAdapter

↓

Normalizer

↓

HistoricalDataBatch(interval=1m)

↓

Historical Ingestion Service

├── QuestDB historical_bars

├── Raw Parquet/ZSTD archive

└── PostgreSQL ingestion metadata

FastAPI queries stored canonical data:



GET /v1/bars

↓

QuestDB / historical storage

↓

stored bars

FastAPI MUST NOT call AKShare synchronously.

3.2 Best-Effort Realtime Quote Path

AKShare

↓

AkshareClient

↓

Realtime Quote Endpoint Adapter

↓

AkshareQuoteNormalizer

↓

QuoteSnapshot

↓

Shared Realtime Ingress

↓

Dispatcher

├── LiveQueue → ZeroMQ → FastAPI cache / WebSocket

└── optional shared persistence path

The quote adapter MUST NOT directly:



publish to ZeroMQ;

mutate FastAPI cache;

write provider-specific QuestDB rows;

send WebSocket messages.

4. Provider Capabilities

The provider capability model MUST distinguish historical intraday data from realtime market data.

AKShare SHOULD advertise:



historical_bars = true

intraday_bars = true

reference_data = true

best_effort_quotes = true

realtime_quotes = false

market_depth = false

trade_ticks = false

If a more generic capability representation already exists, use its equivalent.

The critical requirement is:



best_effort_quotes != realtime_quotes

AKShare MUST NOT claim authoritative low-latency realtime capability.

5. Terminology

The following terms MUST remain distinct.



Historical 1-minute bar

A completed or published OHLCV observation covering one minute.

Canonical type:



HistoricalBar

interval = 1m

Best-effort quote

A polled market snapshot from AKShare/public upstream source.

Canonical type:



QuoteSnapshot

quality = BEST_EFFORT

Trade tick

An actual transaction-level market event.

AKShare snapshot polling MUST NOT generate synthetic TradeTick events.

6. Provider Identity

All AKShare-originated canonical data MUST preserve:



provider = AKSHARE

Where an underlying upstream source is known, preserve it separately:



source = akshare

upstream_source = sina

or another actual source.

Do NOT collapse:



provider

and:



upstream_source

into one field.

7. Symbol Mapping (updated by instrument resolution maintenance)

Explicit provider mappings take precedence. Ordinary physical contracts may be canonicalized deterministically from provider/exchange rules without a pre-existing mapping or physical-contract metadata row. Unknown or ambiguous identities still fail visibly. See `docs/instruments.md`.

8. Product and Contract Preconditions (updated)

Identity recognition and metadata registration are separate. Existing `provider_instruments` foreign keys remain intact. Administrative aliases for unregistered physical or nonphysical instruments use existing `providers.metadata`; ordinary recognition performs no registration. Never invent physical delivery months for rolling or continuous products.

9. AKShare Client Abstraction

All network interaction with AKShare MUST pass through an injectable client abstraction.

Example:



AkshareClient

Tests MUST be able to substitute:



FakeAkshareClient

or equivalent.

Generic ingestion, FastAPI, normalization and repository layers MUST NOT import AKShare directly.

Endpoint-specific behavior MUST remain behind AKShare adapters.

10. Endpoint Adapter Registry

AKShare integration SHOULD continue using an endpoint-adapter registry.

Example conceptual registry:



futures_daily

futures_1m

futures_realtime_quote

inventory

warehouse

positions

contracts

Each endpoint adapter defines:



AKShare function;

supported input parameters;

expected columns;

symbol format;

normalization strategy;

empty-response semantics;

rate-limit policy;

upstream source metadata.

Avoid a giant:



download_everything()

implementation.

Part A — AKShare 1-Minute Historical Bars

11. 1-Minute Historical Capability

AKShare MUST support futures 1-minute bar ingestion where the selected upstream endpoint provides such data.

This is historical/intraday ingestion.

It is NOT considered realtime tick ingestion.

Canonical interval:



1m

12. CLI — One-Shot 1-Minute Fetch

Provide an explicit CLI command.

Preferred canonical form:



akshare-worker fetch futures-1m \

--instrument SHFE.rb2610

Provider-symbol form MAY also be supported:



akshare-worker fetch futures-1m \

--symbol RB2610 \

--exchange SHFE

Docker example:



docker compose --profile akshare run --rm \

akshare-worker fetch futures-1m \

--instrument SHFE.rb2610

The CLI MUST clearly identify the requested dataset as 1-minute bars.

Do not overload a generic ambiguous command such as:



fetch RB2610

without dataset context.

13. Date-Range Fetching

Where the upstream endpoint supports date ranges, Phase 11B MUST expose them.

Example:



akshare-worker fetch futures-1m \

--instrument SHFE.rb2610 \

--start 2026-08-01 \

--end 2026-09-05

The worker MUST validate:



start <= end

and reject malformed ranges.

14. Upstream Lookback and Row Limits

AKShare or its upstream source may impose:



maximum row counts;

maximum lookback periods;

limited historical depth;

endpoint-specific pagination;

truncated results.

The adapter MUST NOT assume an unlimited date range.

If the endpoint imposes limits, implementation MUST:



detect or document the limit;

chunk requests where possible;

merge chunks;

deduplicate overlaps;

report incomplete coverage;

never silently claim a truncated result is complete.

15. Historical Backfill

Provide resumable 1-minute backfill.

Example:



akshare-worker backfill futures-1m \

--instrument SHFE.rb2610 \

--start 2026-01-01 \

--end 2026-09-05

Backfill MUST support:



bounded request windows;

progress checkpoints;

retries;

resume after interruption;

rate limiting;

ingestion-run metadata;

idempotent re-execution.

16. Backfill State

Backfill progress SHOULD persist enough state to resume safely.

At minimum record:



provider

dataset

instrument

requested_start

requested_end

completed_until

status

attempt_count

last_error

updated_at

This MAY use the existing:



provider_ingestion_runs

model where suitable.

17. Canonical 1-Minute Bar Schema

Normalized 1-minute bars MUST contain or map to:



provider

exchange

instrument_id

provider_symbol

interval

bar_start

trading_day



open

high

low

close



volume

open_interest

turnover

settlement



source

upstream_source

fetched_at

Required canonical values:



provider = AKSHARE

interval = 1m

Fields unavailable upstream MUST remain NULL.

Do not fabricate:



open interest;

turnover;

settlement;

bid/ask;

exchange-native sequence IDs.

18. Timestamp Semantics

bar_start MUST represent the start timestamp of the one-minute interval.

Chinese futures local timestamps MUST be interpreted in:



Asia/Shanghai

before conversion into canonical UTC storage representation if the existing schema stores UTC.

Timezone conversion MUST be explicit and tested.

19. Trading Day Semantics

Chinese futures night sessions MUST be handled correctly.

Do NOT infer:



trading_day = calendar_date(bar_start)

for all bars.

Example:



2026-09-04 21:15 Asia/Shanghai

may belong to a later futures trading day.

The implementation SHOULD use the existing:



exchange calendar;

product trading sessions;

trading-day model.

If authoritative trading-day information is unavailable from the upstream response, derivation MUST use documented exchange-session rules rather than naive local-date conversion.

20. Historical Bar Identity

Canonical historical identity MUST remain separate from realtime event identity.

Recommended logical identity:



provider

exchange

instrument_id

interval

bar_start

or the equivalent existing historical-bar key.

Do NOT use:



producer_id + seq

as historical bar identity.

Those fields belong to realtime transport semantics.

21. Historical Idempotency

Repeated ingestion of the same unchanged canonical 1-minute bar MUST NOT create uncontrolled duplicates.

For example:



AKSHARE

SHFE

rb2610

1m

2026-09-05T01:01:00Z

fetched twice with identical values should remain one canonical accepted bar.

Raw fetch archives may preserve both fetch executions independently.

22. Revision Detection

If the same logical bar is fetched later with changed values:



same:

provider

exchange

instrument_id

interval

bar_start



changed:

open/high/low/close

volume

open_interest

turnover

settlement

the system MUST detect a revision.

Required behavior:



preserve original raw fetch;

preserve new raw fetch;

increment revision count;

apply the existing canonical revision policy;

expose the latest accepted canonical version;

do not silently discard the discrepancy.

23. Raw Intraday Archive

Every successful 1-minute upstream fetch SHOULD be archived immutably.

Recommended:



Parquet + ZSTD

Conceptual layout:



raw/

provider=akshare/

dataset=futures_1m/

exchange=SHFE/

symbol=RB2610/

fetch_id=<uuid>/

data.parquet

metadata.json

Exact physical layout may follow the existing archive architecture.

24. Raw Fetch Metadata

Each raw 1-minute fetch SHOULD preserve:



fetch_id

provider

dataset

AKShare function

upstream source

provider symbol

canonical instrument

request parameters

requested date range

fetch timestamp

AKShare package version

normalizer version

schema version

row count

optional content hash

Raw archive lineage MUST be sufficient to reproduce or investigate ingestion behavior.

25. Canonical Storage

Normalized 1-minute bars MUST be persisted to the existing canonical historical bar store.

Current expected QuestDB table:



historical_bars

Rows MUST be queryable by:



provider

exchange

instrument_id

interval

bar_start

trading_day

Example:



provider = AKSHARE

exchange = SHFE

instrument_id = rb2610

interval = 1m

26. QuestDB Integration

Phase 11B MUST reuse the existing historical ingestion/repository abstraction.

AKShare endpoint adapters MUST NOT directly implement custom QuestDB writes.

Preferred flow:



AKShare adapter

↓

HistoricalDataBatch

↓

Historical ingestion service

↓

QuestDB repository

Provider-specific logic ends before the storage repository.

27. FastAPI 1-Minute Query

The existing endpoint:



GET /v1/bars/{symbol}

MUST support:



interval=1m

provider=akshare

Example:



curl \

'http://127.0.0.1:8000/v1/bars/SHFE.rb2610?interval=1m&provider=akshare'

Expected data source:



QuestDB historical_bars

FastAPI MUST NOT call AKShare during this GET request.

28. FastAPI Query Flow

Correct:



Client

↓

FastAPI

↓

Historical Repository

↓

QuestDB

↓

stored bars

Incorrect:



Client

↓

FastAPI

↓

AKShare network request

↓

wait for public website

↓

response

The second design is prohibited.

29. Range Filtering API

Existing:



start_day

end_day

MUST work with AKShare 1-minute bars.

Example:



/v1/bars/SHFE.rb2610

?interval=1m

&provider=akshare

&start_day=20260901

&end_day=20260905

Phase 11B MAY introduce:



start_ts

end_ts

for intraday precision if justified.

Existing API compatibility MUST remain intact.

30. Historical Validation

1-minute ingestion MUST validate at least:



timestamp parseability;

timezone validity;

timestamp ordering;

duplicate logical keys;

missing required OHLC fields;

non-numeric data;

invalid numeric sentinel values;

negative volume where invalid;

schema changes;

trading-day derivation;

interval correctness.

31. OHLC Validation

Minimum consistency checks:



high >= open

high >= close

high >= low



low <= open

low <= close

Equivalent compact logic:



high >= max(open, close, low)

low <= min(open, close, high)

Malformed rows SHOULD be rejected or quarantined.

Do not silently modify market prices to force them to pass validation unless a documented normalization rule exists.

32. Missing Values

Missing optional fields MUST remain NULL.

Examples:



open_interest = NULL

turnover = NULL

settlement = NULL

Do NOT substitute:



0

unless zero is actually reported and semantically valid.

33. Schema Drift

AKShare/public endpoint schemas may change.

Each 1-minute endpoint adapter MUST define:



required columns;

optional columns;

known aliases if explicitly documented;

expected data types.

If required fields disappear or semantics become ambiguous:



fail visibly;

increment schema-error metrics;

mark provider or dataset degraded;

do not guess.

34. Empty Response Semantics

An empty DataFrame/list MUST have endpoint-specific handling.

Possible interpretations include:



no data in requested range;

contract not listed yet;

expired contract;

market closed;

upstream unavailable;

invalid symbol;

changed schema.

The adapter MUST NOT universally treat empty output as success.

35. Intraday Refresh

Phase 11B MAY support periodic refresh of recent 1-minute bars.

Example:



refresh recent bars every 5 minutes

The refresh job MUST remain an intraday historical polling job.

It MUST NOT be presented as tick-level realtime.

36. Refresh Overlap

Periodic refresh SHOULD intentionally overlap recent bars.

Example:



each refresh re-fetch last 10–30 minutes

This helps detect upstream revisions.

Because ingestion is idempotent and revision-aware, overlapping refreshes MUST be safe.

Part B — AKShare Best-Effort Realtime Quotes

37. Realtime Quote Objective

AKShare MAY provide best-effort quote snapshots for:



development;

monitoring;

cross-provider comparison;

fallback observation by human operators;

validation of provider-independent realtime infrastructure.

It MUST NOT become the authoritative production realtime futures feed.

38. Realtime Quality

Every AKShare live quote MUST expose:



quality = BEST_EFFORT

Do NOT label it:



REALTIME

EXCHANGE_DIRECT

AUTHORITATIVE

unless the actual source semantics later justify those terms.

39. Quote Event Type

AKShare realtime polling MUST emit:



QuoteSnapshot

unless the upstream endpoint explicitly provides another trustworthy market-data event type.

It MUST NOT synthesize:



TradeTick

BidAskTick

DepthUpdate

from price differences between polls.

40. No Trade Reconstruction

Example:



poll #1 last_price = 3500

poll #2 last_price = 3501

This means only:



latest observed snapshot changed

It does NOT prove:



trade occurred at 3501

Therefore no TradeTick may be generated.

41. Realtime Runtime Isolation

AKShare quote polling MUST run outside FastAPI.

Recommended:



akshare-worker serve-quotes

or a dedicated:



akshare-quotes

service.

It MUST NOT run in:



FastAPI request handlers;

WebSocket handlers;

CTP callbacks;

IBKR callbacks;

persistence-writer threads.

42. Polling Configuration

Required configuration:



AKSHARE_REALTIME_ENABLED

AKSHARE_QUOTE_POLL_INTERVAL_SECONDS

AKSHARE_QUOTE_REQUEST_TIMEOUT_SECONDS

AKSHARE_QUOTE_MAX_CONCURRENCY

AKSHARE_QUOTE_BATCH_SIZE

AKSHARE_QUOTE_STALE_AFTER_SECONDS

AKSHARE_REALTIME_PERSIST

Recommended default:



AKSHARE_REALTIME_ENABLED=false

The backend MUST remain fully operational with AKShare realtime disabled.

43. Polling Frequency

Initial quote polling SHOULD be conservative.

Recommended starting range:



3–5 seconds

Do not assume sub-second updates.

A minimum polling interval SHOULD be enforced.

Unbounded high-frequency polling of public upstream endpoints is prohibited.

44. Batch Quote Fetches

When supported, endpoint adapters SHOULD batch multiple symbols.

Preferred:



one request

→ RB2610

→ ZN2610

→ CU2610

rather than one HTTP request per contract.

Batch logic belongs inside the endpoint adapter.

45. Subscription Model

AKShare quote polling MUST consume the shared canonical subscription representation.

Example:



Subscription {

instrument_id = SHFE.rb2610

market_data_kind = QUOTE

}

It MUST resolve provider-native symbols using explicit provider mappings first, followed by deterministic provider formatters.

46. Realtime Normalization

Canonical QuoteSnapshot SHOULD support:



provider

event_type

instrument_id

exchange

instrument

quality



event_ts

recv_ts

timestamp_source



last_price

volume

turnover

open_interest



upper_limit_price

lower_limit_price



bid_price1

bid_volume1

ask_price1

ask_volume1



source

upstream_source



producer_id

seq

Only populate fields supplied with reliable semantics.

Missing depth fields remain NULL.

47. Realtime Timestamps

recv_ts MUST always be populated.

If upstream provides a trustworthy event timestamp:



event_ts = upstream time

timestamp_source = UPSTREAM

Otherwise:



event_ts = recv_ts

timestamp_source = RECEIVE_TIME

Do not fabricate exchange-level timing precision.

48. Realtime Stable Identity

Each running AKShare realtime producer gets a stable per-process:



producer_id

Each emitted quote receives monotonically increasing:



seq

Transport identity remains:



provider

producer_id

seq

event_ts

Retries MUST preserve original identity.

New process run → new producer_id.

49. Feed Duplicates

Identical consecutive polled snapshots are separate feed observations.

Example:



12:00:01 last_price = 3500

12:00:05 last_price = 3500

These MUST NOT be considered transport duplicates merely because all market fields match.

Live freshness paths MAY coalesce repeated snapshots if the generic live pipeline already supports that policy.

50. Transport Duplicates

The same canonical event replayed due to transport retry:



same provider

same producer_id

same seq

same event_ts

MUST remain deduplicatable through the existing persistence semantics.

51. Shared Realtime Ingress

AKShare normalized QuoteSnapshot events MUST enter the provider-independent realtime ingress.

Correct:



AKShare adapter

↓

QuoteSnapshot

↓

shared ingress

↓

Dispatcher

↓

ZeroMQ / persistence

Incorrect:



AKShare adapter

↓

FastAPI cache directly

52. Provider-Aware Quote Cache

The same canonical instrument may simultaneously have:



CTP SHFE.rb2610

AKSHARE SHFE.rb2610

IBKR SHFE.rb2610

These MUST coexist.

Recommended key:



(provider, exchange, instrument)

or equivalent provider-aware identity.

One provider MUST NOT overwrite another provider's observation.

53. Quote API

FastAPI MUST support explicit AKShare quote queries.

Example:



GET /v1/quotes/SHFE.rb2610?provider=akshare

The returned object MUST expose:



provider = AKSHARE

quality = BEST_EFFORT

FastAPI MUST read from the live cache.

It MUST NOT synchronously fetch AKShare.

54. Provider-Omitted Queries

For:



GET /v1/quotes/SHFE.rb2610

Phase 11B MUST NOT silently use AKShare merely because another provider is unavailable.

Existing deterministic default-provider behavior may remain.

Automatic fallback is explicitly out of scope.

55. No Provider Arbitration

Phase 11B MUST NOT implement:



CTP missing → use AKShare automatically

IBKR missing → use AKShare automatically

CTP differs from AKShare → choose one

average provider prices

vote between providers

That belongs to a later provider-selection/arbitration phase.

56. Quote Staleness

AKShare quotes MUST expose enough information for staleness detection.

At minimum:



recv_ts

provider

quality

Recommended configuration:



AKSHARE_QUOTE_STALE_AFTER_SECONDS

A stale AKShare quote MUST not appear semantically equivalent to a fresh CTP observation.

57. Optional Realtime Persistence

Best-effort AKShare quote persistence MAY be configurable.

Example:



AKSHARE_REALTIME_PERSIST=false

If enabled, use the shared generic persistence path.

Do NOT build an AKShare-specific QuestDB writer.

58. Generic Realtime Table Naming

If the realtime persistence table is still named:



ctp_market_data

Phase 11B MUST document that this name is provider-specific technical debt.

A future provider-neutral name is preferred:



market_events

quote_snapshots

market_data_events

However Phase 11B MUST NOT perform a risky migration solely for cosmetic naming.

Only migrate if multi-provider correctness requires it.

Part C — Reliability and Operations

59. Error Classification

AKShare operations SHOULD classify errors into:



TRANSIENT_NETWORK

RATE_LIMIT

UPSTREAM_UNAVAILABLE

SCHEMA_CHANGED

INVALID_RESPONSE

MAPPING_ERROR

INVALID_SYMBOL

PERMANENT

Historical and realtime jobs SHOULD share error classification utilities where appropriate.

60. Retry Policy

Transient failures MUST use bounded exponential backoff with jitter.

Example:



1s

2s

4s

8s

16s

max 30–60s

No tight retry loops.

Historical batch jobs SHOULD eventually fail visibly after bounded retries.

Realtime pollers SHOULD remain alive and continue future polling attempts.

61. Rate Limiting

AKShare access MUST use conservative request pacing.

The implementation SHOULD support:



max concurrency

minimum request spacing

endpoint-specific limits

A backfill job MUST NOT starve realtime quote polling.

If needed, separate rate-limit budgets SHOULD be used for:



historical

reference

realtime

62. Health Model

AKShare provider health SHOULD expose separate sub-status where practical:



historical

reference

best_effort_realtime

Overall provider states:



AVAILABLE

DEGRADED

UNAVAILABLE

Example:



{

"provider": "akshare",

"status": "DEGRADED",

"capabilities": {

"historical_bars": true,

"intraday_bars": true,

"reference_data": true,

"best_effort_quotes": true

},

"historical": {

"last_success": "...",

"last_error": null

},

"realtime": {

"last_success": "...",

"active_subscriptions": 4,

"consecutive_failures": 2

}

}

63. Historical Metrics

Add metrics at minimum:



akshare_historical_requests_total

akshare_historical_request_failures_total



akshare_historical_1m_requests_total

akshare_historical_1m_rows_received_total

akshare_historical_1m_rows_written_total

akshare_historical_1m_revisions_total

akshare_historical_1m_schema_errors_total

akshare_historical_1m_mapping_errors_total

akshare_historical_1m_empty_responses_total

akshare_historical_1m_request_latency_seconds

64. Realtime Metrics

Add:



akshare_quote_requests_total

akshare_quote_request_failures_total

akshare_quote_rows_received_total

akshare_quote_events_emitted_total

akshare_quote_mapping_errors_total

akshare_quote_schema_errors_total

akshare_quote_empty_responses_total

akshare_quote_retries_total

akshare_quote_request_latency_seconds

akshare_quote_poll_duration_seconds

akshare_quote_poll_lag_seconds

akshare_quote_last_success_timestamp

akshare_quote_active_subscriptions

Avoid unbounded per-symbol metric labels unless the monitoring architecture explicitly permits them.

65. Structured Logging

Historical log context SHOULD include:



provider=akshare

dataset=futures_1m

fetch_id

instrument

provider_symbol

requested_start

requested_end

rows_received

rows_written

revisions

duration

error_class

Realtime logs SHOULD include:



provider=akshare

component=quote_poller

endpoint

subscription_count

rows_received

events_emitted

duration

error_class

Do not log complete DataFrames during normal operation.

66. Provider Ingestion Runs

Continue using or extend:



provider_ingestion_runs

Recommended fields:



id

provider_id

dataset

endpoint



started_at

completed_at

status



request_params



rows_received

rows_normalized

rows_rejected

rows_written

revision_count



error_class

error_message



provider_version

schema_version

1-minute jobs MUST be observable here.

67. Version Pinning

The AKShare package version MUST remain pinned.

Every ingestion run SHOULD record the AKShare package version.

Schema changes after package upgrades MUST be testable and traceable.

68. Docker Deployment

AKShare remains an optional profile.

Recommended:



--profile akshare

Possible services:



akshare-worker

akshare-quotes

or one service with different commands.

Examples:



docker compose --profile akshare run --rm \

akshare-worker fetch futures-1m \

--instrument SHFE.rb2610

and:



docker compose --profile akshare up -d akshare-quotes

Exact naming may follow existing compose conventions.

69. Development vs Production Behavior

Development MAY use bind-mounted Python source and reload behavior.

Production SHOULD use immutable built images.

AKShare functionality MUST not depend on source bind mounts to work correctly.

70. FastAPI Independence

FastAPI startup MUST NOT fail solely because AKShare is unavailable.

FastAPI historical queries read stored data.

FastAPI realtime queries read live cache.

Therefore:



AKShare public source unavailable

MUST NOT make:



FastAPI

CTP

IBKR

QuestDB

PostgreSQL

unavailable.

71. CTP Isolation

Phase 11B MUST NOT change CTP callback behavior.

CTP callback remains:



capture timestamp

copy fields

normalize

assign identity

non-blocking enqueue

return

AKShare operations MUST never execute on CTP callback threads.

72. IBKR Isolation

AKShare failures or delays MUST not block future IBKR callback handling or subscription management.

Provider failures must remain isolated.

73. Shutdown — Historical Jobs

Graceful shutdown of historical workers SHOULD:



stop starting new fetch chunks;

allow bounded in-flight request completion;

persist checkpoint state;

record ingestion run status;

close HTTP clients.

74. Shutdown — Realtime Poller

Graceful quote-poller shutdown MUST:



stop accepting new subscriptions;

stop scheduling new network polls;

cancel or await bounded in-flight requests;

emit already-normalized events where appropriate;

close client;

terminate cleanly.

Shutdown MUST NOT hang indefinitely.

Part D — Testing

75. Unit Tests

CI MUST NOT require external internet access.

All AKShare behavior MUST be testable using fixtures/fakes.

76. 1-Minute Parsing Tests

Fixtures MUST cover:



normal 1-minute rows;

missing optional columns;

invalid timestamps;

duplicate rows;

unordered rows;

numeric strings;

null values;

invalid OHLC;

schema drift.

77. Trading-Day Tests

Tests MUST include night-session examples.

At minimum verify that:



local calendar date

is not automatically assumed to equal:



futures trading day

78. Idempotency Tests

Fetching the same unchanged 1-minute dataset twice MUST not create duplicate canonical bars.

Expected:



fetch 1 → N writes

fetch 2 → 0 new canonical rows

or equivalent update semantics.

79. Revision Tests

Given:



fetch 1:

close = 3500



fetch 2:

same logical bar

close = 3501

the system MUST:



recognize a revision;

preserve raw fetch lineage;

increment revisions;

expose latest accepted canonical value.

80. Mapping Tests

Known mapping:



SHFE.rb2610

→ RB2610

must resolve.

Unknown mapping MUST not be guessed.

81. Range-Chunking Tests

If an endpoint requires chunked retrieval:



requested range

→ chunk A

→ chunk B

→ overlap

→ merge

tests MUST verify:



correct coverage;

no duplicate canonical rows;

retry-safe resume.

82. Fake AKShare Client

Provide:



FakeAkshareClient

or equivalent.

It SHOULD support deterministic fixtures for:



1-minute history;

realtime quotes;

empty responses;

exceptions;

schema changes.

83. Realtime Quality Test

Every normalized AKShare live quote MUST contain:



provider = AKSHARE

quality = BEST_EFFORT

84. No Trade Synthesis Test

Changing quote snapshots MUST not generate TradeTick.

Example:



snapshot A price=3500

snapshot B price=3501

Expected emitted event count:



2 QuoteSnapshot

0 TradeTick

85. Provider-Aware Cache Test

Given:



CTP SHFE.rb2610

AKSHARE SHFE.rb2610

both must remain retrievable independently.

AKShare update MUST NOT overwrite CTP.

86. API Historical Integration Test

Test path:



FakeAkshareClient

↓

1m normalizer

↓

historical ingestion

↓

test historical repository / QuestDB fixture

↓

GET /v1/bars/SHFE.rb2610?interval=1m&provider=akshare

Expected:



HTTP 200

stored 1m bars

No external network.

87. API Realtime Integration Test

Test:



FakeAkshareClient

↓

quote poller

↓

QuoteSnapshot

↓

shared ingress

↓

ZeroMQ / equivalent test transport

↓

FastAPI cache

↓

GET /v1/quotes/SHFE.rb2610?provider=akshare

Expected:



provider=AKSHARE

quality=BEST_EFFORT

88. Failure Isolation Test

Simulate AKShare endpoint failure.

Verify:



CTP pipeline remains functional;

FastAPI remains available;

QuestDB remains available;

PostgreSQL remains available;

AKShare health becomes degraded/unavailable;

historical stored data remains queryable.

89. Manual Smoke Tests

Manual internet-dependent tests MAY exist outside CI.



Daily historical

docker compose --profile akshare run --rm \

akshare-worker fetch futures-daily \

--instrument SHFE.rb2610

1-minute historical

docker compose --profile akshare run --rm \

akshare-worker fetch futures-1m \

--instrument SHFE.rb2610

Query stored 1-minute bars

curl \

'http://127.0.0.1:8000/v1/bars/SHFE.rb2610?interval=1m&provider=akshare'

One-shot quote

docker compose --profile akshare run --rm \

akshare-worker quote SHFE.rb2610

Part E — Documentation

90. Documentation Updates

Update:



REQUIREMENTS.md

ROADMAP.md

CHANGELOG.md



docs/architecture.md

docs/providers.md

docs/providers/akshare.md

docs/data-model.md

docs/delivery-semantics.md

Create or update:



docs/phases/phase-11b-akshare-intraday-and-realtime.md

91. AKShare Provider Documentation

docs/providers/akshare.md MUST document:



historical daily capability;

historical 1-minute capability;

reference capability;

best-effort quote capability;

provider vs upstream source;

rate-limit assumptions;

source limitations;

symbol mapping rules;

storage destinations;

raw archive lineage;

realtime quality semantics;

known endpoint lookback limits;

known schema fragility.

92. Data Model Documentation

docs/data-model.md MUST explicitly distinguish:



HistoricalBar

from:



QuoteSnapshot

and:



TradeTick

It MUST document historical bar identity separately from realtime event identity.

93. Delivery Semantics Documentation

docs/delivery-semantics.md MUST state:

Historical:



logical idempotency based on canonical bar key

Realtime:



at-least-once transport

stable producer_id + seq

transport duplicate deduplication

feed duplicates preserved

Do not mix the two identity models.

Part F — Non-Goals

94. Explicit Non-Goals

Phase 11B MUST NOT implement:



AKShare as primary professional realtime feed;

high-frequency sub-second scraping;

guaranteed tick-by-tick semantics;

reconstructed exchange transaction stream;

synthetic trade ticks;

synthetic order-book depth;

exchange sequence reconstruction;

automatic provider failover;

provider price averaging;

provider arbitration;

order routing;

order execution;

trading strategy execution;

Redis solely for AKShare;

Kafka solely for AKShare;

Celery solely for AKShare;

synchronous AKShare calls inside FastAPI;

direct AKShare writes to FastAPI cache;

direct AKShare ZeroMQ publishing from endpoint adapters;

support for every AKShare endpoint.

Part G — Architectural Invariants

95. Mandatory Invariants

After Phase 11B all of the following MUST remain true:



AKShare 1-minute bars are historical data.

AKShare quote polling is best-effort realtime data.

1-minute bars do not enter the live quote pipeline.

QuoteSnapshot events do not automatically become historical 1-minute bars.

FastAPI never synchronously calls AKShare.

AKShare provider-specific code ends at normalization/event emission.

AKShare endpoint adapters do not directly mutate FastAPI state.

AKShare endpoint adapters do not directly publish ZeroMQ.

Historical ingestion uses the common historical repository/storage service.

Realtime quotes use the common realtime ingress.

Canonical identity comes from explicit mappings first, then vetted deterministic provider/exchange rules.

Unknown symbols are never silently guessed.

Provider identity survives storage and live delivery.

AKShare quotes are explicitly marked BEST_EFFORT.

No automatic fallback to AKShare exists.

No TradeTick is synthesized from snapshot differences.

CTP callback behavior remains unchanged.

AKShare failure cannot break CTP.

AKShare failure cannot break IBKR.

Raw historical fetches retain provenance.

Canonical historical writes remain idempotent.

Historical revisions remain detectable.

Realtime transport retries retain stable identity.

FastAPI historical queries read stored data.

FastAPI live queries read the realtime cache.

Part H — Definition of Done

96. Phase 11B Definition of Done

Phase 11B is complete only when all applicable items below are satisfied.



Provider capabilities

AKShare advertises historical bars.

AKShare advertises intraday bars.

AKShare advertises reference data.

AKShare advertises best-effort quotes.

AKShare does not advertise authoritative realtime quotes.

AKShare does not advertise trade ticks.

AKShare does not advertise market depth unless genuinely supported later.

1-minute historical bars

futures 1-minute endpoint adapter exists.

1-minute data can be fetched for mapped Chinese futures contracts.

canonical instrument → provider symbol mapping is used.

unresolved or ambiguous identities fail visibly; an ordinary deterministic contract does not require a mapping row.

canonical interval is 1m.

timestamps are normalized correctly.

Asia/Shanghai source timestamps are handled correctly.

futures night-session trading day is handled correctly.

OHLC validation exists.

invalid rows are rejected/quarantined.

optional missing fields remain NULL.

canonical rows are written to historical_bars.

repeated unchanged fetches are idempotent.

revisions are detected.

revision counts are reported.

raw fetch archives are preserved in Parquet/ZSTD.

fetch metadata includes lineage.

bounded date-range fetching works where supported.

upstream range limits are handled explicitly.

chunked requests merge correctly.

overlapping requests do not duplicate canonical rows.

resumable backfill exists.

backfill state/checkpointing exists.

recent intraday refresh can be scheduled if enabled.

schema drift fails visibly.

FastAPI historical API

/v1/bars/{symbol}?interval=1m&provider=akshare works.

API reads from canonical stored bars.

API does not contact AKShare synchronously.

start_day filtering works.

end_day filtering works.

API returns canonical provider identity.

Best-effort realtime

dedicated AKShare quote poller exists.

realtime polling defaults to disabled.

polling interval is configurable.

minimum safe polling interval is enforced.

bounded concurrency exists.

batching is used where endpoint supports it.

subscription resolution uses provider_instruments.

realtime normalization produces QuoteSnapshot.

every AKShare quote has provider=AKSHARE.

every AKShare quote has quality=BEST_EFFORT.

recv_ts is always populated.

timestamp provenance is represented.

producer_id exists.

seq is monotonic per producer.

transport retries preserve identity.

feed duplicates are not confused with transport duplicates.

no TradeTick is synthesized.

quotes enter shared realtime ingress.

adapters do not publish ZeroMQ directly.

provider-aware cache supports simultaneous CTP/AKShare values.

/v1/quotes/{symbol}?provider=akshare works.

provider-less requests do not silently fall back to AKShare.

quote staleness is observable.

optional realtime persistence uses shared persistence service.

Reliability

transient network errors retry.

retry is bounded.

retry uses backoff and jitter.

rate-limit handling exists.

empty responses use endpoint-specific semantics.

schema drift is detectable.

AKShare provider health is observable.

AKShare realtime health is observable.

historical ingestion health is observable.

graceful shutdown works.

AKShare failure does not block API startup.

AKShare failure does not affect CTP.

AKShare failure does not affect IBKR.

Observability

1-minute request metrics exist.

1-minute rows-received metrics exist.

1-minute rows-written metrics exist.

revision metrics exist.

mapping-error metrics exist.

schema-error metrics exist.

quote request metrics exist.

quote failure metrics exist.

quote latency metrics exist.

quote emitted-event metrics exist.

last-success metrics exist.

ingestion runs are persisted.

Testing

unit tests do not require internet.

FakeAkshareClient exists.

1-minute fixtures exist.

quote fixtures exist.

schema-drift fixtures exist.

night-session tests exist.

idempotency tests exist.

revision tests exist.

mapping-error tests exist.

range-chunking tests exist.

retry tests exist.

quote quality tests exist.

no-trade-synthesis test exists.

provider-aware cache test exists.

FastAPI historical integration test exists.

FastAPI realtime integration test exists.

failure-isolation test exists.

all previous Phase 0–11A tests continue to pass.

Documentation

REQUIREMENTS.md updated.

ROADMAP.md updated.

CHANGELOG.md updated.

architecture documentation updated.

AKShare provider documentation updated.

data model documentation updated.

delivery semantics documentation updated.

Phase 11B document added.

Part I — Recommended Implementation Order

97. Phase 11B Implementation Sequence

Implement in this order.



11B.1 Historical 1-Minute Endpoint

Implement:



AkshareClient

↓

FuturesMinuteBarEndpointAdapter

No realtime polling yet.

Goal:



fetch one RB2610 1m dataset successfully

11B.2 1-Minute Normalizer

Normalize into:



HistoricalDataBatch

interval=1m

Implement:



timestamps;

OHLC;

volume;

OI where available;

provider identity;

symbol mapping;

data-quality validation.

11B.3 Canonical 1-Minute Persistence

Connect to:



historical_bars

Verify manually:



SELECT count()

FROM historical_bars

WHERE provider='AKSHARE'

AND exchange='SHFE'

AND instrument_id='rb2610'

AND interval='1m';

11B.4 Raw Archive

Archive each fetch into immutable Parquet/ZSTD with fetch metadata.

11B.5 Idempotency and Revision Detection

Verify:



same fetch twice

→ no duplicate canonical rows

and:



same logical bar with changed values

→ revision detected

11B.6 Range Fetching

Implement:



--start

--end

and endpoint-specific request chunking.

11B.7 Backfill and Resume

Implement resumable:



backfill futures-1m

with checkpoints.

11B.8 FastAPI 1-Minute Query

Verify:



curl \

'http://127.0.0.1:8000/v1/bars/SHFE.rb2610?interval=1m&provider=akshare'

Expected:



HTTP 200

stored canonical 1-minute bars

11B.9 Best-Effort Quote Capability

Add capability metadata:



best_effort_quotes

BEST_EFFORT

No polling yet.

11B.10 Quote Adapter

Implement endpoint adapter and QuoteSnapshot normalization.

11B.11 Quote Poller

Implement:



polling;

batching;

bounded concurrency;

timeout;

retry;

backoff;

staleness;

health.

11B.12 Shared Realtime Integration

Connect:



AKShare QuoteSnapshot

↓

shared ingress

↓

Dispatcher

↓

ZeroMQ

↓

FastAPI cache

11B.13 Provider-Aware API

Verify:



curl \

'http://127.0.0.1:8000/v1/quotes/SHFE.rb2610?provider=akshare'

while allowing CTP/IBKR observations for the same contract to coexist.

11B.14 Monitoring and Health

Complete metrics, health and structured logs.

11B.15 Tests and Regression

Run:



AKShare unit tests;

ingestion tests;

FastAPI tests;

provider-aware cache tests;

historical storage tests;

Phase 0–11A regression suite.

11B.16 Documentation

Finalize phase documentation and update architectural documents.

98. Final Architecture After Phase 11B

Market Data Backend



┌───────────────────────────────────────────────────────┐

│ Providers │

│ │

│ CTP IBKR AKShare │

│ │ │ │ │

│ │ │ ┌────────┴────────┐ │

│ │ │ │ │ │

│ realtime │ historical quotes │

│ │ 1m / daily best-effort │

└────┬─────────────┴────────────┬───────────────┬───────┘

│ │ │

▼ ▼ ▼

Canonical Realtime HistoricalDataBatch QuoteSnapshot

Market Events │ │

│ │ │

│ ▼ │

│ Historical Ingestion │

│ │ │ │

│ ▼ ▼ │

│ QuestDB Parquet │

│ historical raw archive │

│ _bars │

│ │

└──────────────────┬──────────────────────┘

▼

Shared Live Ingress

│

Dispatcher

│

┌────────┴─────────┐

▼ ▼

Persistence ZeroMQ

│

▼

FastAPI

┌──────┴───────┐

▼ ▼

REST Quotes WebSocket



Historical API:



FastAPI

↓

Historical Repository

↓

QuestDB / historical_bars

99. Final Design Principles

Phase 11B MUST preserve these conceptual boundaries:



AKShare 1-minute bars

=

historical intraday data

=

persisted canonical bars

while:



AKShare quote polling

=

best-effort live observation

=

QuoteSnapshot

and:



CTP / future authoritative providers

=

professional realtime market feed

These three concepts MUST NOT be conflated.

The backend should be able to answer independently:



What 1-minute AKShare bars have I stored?

and:



What is AKShare's latest observed quote?

without either capability depending on the other.

100. Phase Boundary

Phase 11B ends when AKShare has reliable:



Historical:

daily bars

1-minute bars

backfill

storage

raw lineage

FastAPI queries



Realtime:

best-effort quote polling

provider-aware live cache

quality labeling

health / metrics

Phase 11B MUST NOT implement cross-provider selection.

The recommended next phase is:



Phase 12 — Provider Selection, Arbitration and Failover

which may address:



preferred provider

fallback policy

freshness ranking

provider quality ranking

cross-provider discrepancy detection

manual/automatic provider selection

Those concerns remain explicitly outside Phase 11B.

## Phase 12 — Provider Selection, Arbitration and Failover

Implementation status: complete (2026-09-05).

Phase 12 introduces a provider-neutral read-side selection layer over the provider-aware latest quote cache. It implements explicit, preferred-provider, and quality-ranked modes; per-provider freshness; separately enabled fallback and stale use; transparent decision metadata; discrepancy diagnostics; and bounded-label metrics.

The default is `explicit`, so multiple providers still require `provider=`. Failover is disabled unless `PROVIDER_FALLBACK_ENABLED=true`. Selection must not mutate cached observations, average prices, synthesize events, change persistence identity, call provider SDKs, or run on native callback threads. Explicit provider queries always return that provider's observation, including its stale status. `/v1/provider-selection/{symbol}` exposes the inputs and decision for operators.

Phase 12 is complete when policy decisions are deterministic and tested, stale/unavailable primaries fail over only under explicit configuration, AKShare quality remains lower than authoritative realtime feeds, discrepancies are observable without automatic price merging, and all earlier regression suites pass.


## Instrument resolution maintenance — implemented

Implemented the explicitly requested `currentTASK.MD` instrument identity work; this does not infer a numbered Phase 13. Shared parsing/formatting, kinds, metadata independence, explicit override/ambiguity diagnostics, AKShare symbol joins, CLI administration, foreign reference sync, provenance persistence and schema-v3 archives are documented in `docs/instruments.md`. Foreign price ingestion, arbitrary front-month selection and automatic instrument registration remain outside this task.
