# Phase 11 — AKShare Historical & Reference Data Provider

## Status

**Phase:** 11
**Name:** AKShare Historical & Reference Data Provider
**Prerequisite:** Phase 09 Multi-Provider Architecture completed
**Recommended:** Phase 10 IBKR may be completed first, but is not a hard dependency
**Primary Language:** Python 3.12+
**Provider:** AKShare
**Scope Type:** Historical data, reference data, supplemental datasets
**Real-Time Status:** Non-primary / best-effort only; real-time streaming is NOT part of the primary Phase 11 scope

---

# 1. Objective

Integrate AKShare as a first-class provider for historical, reference, exchange-public and supplemental financial-market datasets.

AKShare must primarily provide:

```text
Historical futures data
Daily bars
Historical contract data
Continuous-contract reference data
Exchange-public datasets
Contract/reference metadata
Inventory / warehouse datasets where available
Position/ranking datasets where available
Supplemental market data
Historical gap filling
Data cross-validation
```

AKShare must NOT replace CTP as the primary low-latency Chinese futures real-time market-data provider.

The Phase 11 implementation must follow the multi-provider architecture established in Phase 09.

---

# 2. Architectural Position

AKShare is fundamentally different from CTP and IBKR.

The intended architecture is:

```text
                       AKShare
                          │
                          ▼
                  Python Provider
                          │
                          ▼
                   Endpoint Adapter
                          │
                          ▼
                     Normalizer
                          │
                ┌─────────┴─────────┐
                ▼                   ▼
        HistoricalDataBatch   ReferenceDataBatch
                │                   │
                └─────────┬─────────┘
                          ▼
                Provider-independent
                 Ingestion Services
                          │
             ┌────────────┼────────────┐
             ▼            ▼            ▼
        Raw Parquet    QuestDB     PostgreSQL
                         Bars       Metadata
                          │
                          ▼
                       DuckDB
```

AKShare historical/reference requests must NOT enter:

```text
CTP callback pipeline
high-frequency IngressQueue
PersistenceQueue intended for realtime ticks
ZeroMQ live feed
WebSocket live quote path
```

unless a future explicitly approved best-effort realtime feature is implemented.

---

# 3. Core Principle

The AKShare adapter is responsible for:

```text
calling AKShare
receiving DataFrames
validating returned schema
normalizing provider-native fields
producing canonical data batches
reporting source lineage
reporting provider health
```

The AKShare adapter must NOT contain:

```text
QuestDB-specific SQL
PostgreSQL-specific business logic
Parquet directory policy
FastAPI route logic
continuous-contract business rules
bar-generation rules
WebSocket logic
```

Those responsibilities belong to shared services.

---

# 4. Provider Capabilities

AKShare must declare explicit capabilities.

Conceptually:

```python
ProviderCapabilities(
    realtime_quotes=False,
    tick_by_tick=False,
    market_depth=False,
    historical_ticks=False,
    historical_bars=True,
    reference_data=True,
)
```

Optional best-effort quotes must not be advertised as primary realtime capability in the initial Phase 11 implementation.

If later enabled, expose them using a distinct capability such as:

```text
best_effort_quotes = true
```

rather than:

```text
realtime_quotes = true
```

unless the semantics have been explicitly verified.

---

# 5. Provider Identity

Use the canonical provider identity established in Phase 09:

```text
ProviderId::AKShare
```

or its Python equivalent:

```python
ProviderId.AKSHARE
```

Do not use arbitrary provider strings throughout the code.

Canonical value:

```text
AKSHARE
```

---

# 6. Python Implementation

Implement AKShare integration as a separate Python module/process.

Preferred location:

```text
python/
└── providers/
    └── akshare/
        ├── __init__.py
        ├── provider.py
        ├── client.py
        ├── registry.py
        ├── models.py
        ├── normalizers/
        ├── endpoints/
        ├── scheduler.py
        ├── health.py
        ├── metrics.py
        └── tests/
```

Do not embed Python into the C++ CTP process.

Do not link C++ directly against Python solely for AKShare.

---

# 7. Process Isolation

AKShare failures must not affect:

```text
CTP Collector
IBKR Provider
QuestDB realtime writer
ZeroMQ publisher
FastAPI live feed
```

The AKShare worker should be independently restartable.

Conceptually:

```text
market-data-core
    │
    ├── CTP process
    ├── IBKR process
    ├── FastAPI process
    └── AKShare worker
```

A broken AKShare endpoint must never stop realtime market-data collection.

---

# 8. Provider Interfaces

Phase 11 must implement the historical/reference interfaces established by Phase 09.

Conceptually:

```python
class HistoricalDataProvider(Protocol):

    async def fetch_bars(
        self,
        request: HistoricalBarRequest,
    ) -> HistoricalDataBatch:
        ...
```

and:

```python
class ReferenceDataProvider(Protocol):

    async def fetch_reference(
        self,
        request: ReferenceDataRequest,
    ) -> ReferenceDataBatch:
        ...
```

Exact interfaces should follow the actual Phase 09 implementation.

Do not duplicate existing canonical interfaces.

---

# 9. Do Not Create One Giant AKShare Function

Prohibited architecture:

```python
def download_everything():
    ...
```

Prefer endpoint-specific adapters.

Example:

```text
AkshareProvider
      │
      ├── FuturesDailyAdapter
      ├── FuturesContractAdapter
      ├── FuturesInventoryAdapter
      ├── FuturesPositionAdapter
      └── FuturesContinuousReferenceAdapter
```

Each adapter should have clearly defined:

```text
input
source
expected columns
normalization
canonical output
quality rules
```

---

# 10. Endpoint Registry

Create a registry describing supported AKShare datasets.

Conceptual model:

```python
@dataclass
class EndpointDefinition:
    name: str
    function_name: str
    dataset_type: DatasetType
    upstream_source: str | None
    frequency: DataFrequency
    enabled: bool
```

The registry prevents AKShare function names from being scattered throughout the codebase.

---

# 11. Initial Dataset Scope

Phase 11 should initially prioritize Chinese futures.

Required dataset categories:

```text
physical futures contract daily bars

continuous/main-contract historical data
for comparison/reference only

exchange/product symbol information

contract-related public information

inventory / warehouse information
where AKShare exposes suitable data

position/ranking data
where available and stable
```

Do not attempt to integrate every AKShare endpoint in one phase.

---

# 12. Historical Contract Bars

Support historical daily futures contract data.

Canonical output should approximately represent:

```text
provider
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
settlement

source
fetched_at
```

For daily futures data:

```text
interval = 1d
```

---

# 13. Canonical Historical Bar Model

Use or extend the canonical bar model.

Conceptually:

```python
@dataclass
class HistoricalBar:
    provider: ProviderId

    instrument_id: int
    provider_symbol: str

    interval: str

    bar_start: datetime
    trading_day: date

    open: Decimal | float | None
    high: Decimal | float | None
    low: Decimal | float | None
    close: Decimal | float | None

    volume: int | None
    open_interest: int | None
    turnover: float | None
    settlement: float | None

    fetched_at: datetime

    source: str
```

Reuse existing canonical models where possible.

---

# 14. Do Not Trust Provider Symbols as Canonical Identity

AKShare symbols are provider-native identifiers.

Never assume:

```text
AKShare symbol
=
canonical InstrumentId
```

Required mapping:

```text
AKShare provider symbol
        ↓
provider_instruments
        ↓
canonical InstrumentId
```

---

# 15. Instrument Mapping

Use the PostgreSQL:

```text
provider_instruments
```

mapping introduced in Phase 09.

Example:

```text
provider       AKSHARE
provider_symbol zn2610
exchange        SHFE
instrument_id   100042
```

Unmapped symbols must not silently create random canonical instruments.

---

# 16. Unresolved Instrument Policy

If AKShare returns a symbol that cannot be resolved:

```text
do not silently discard it
```

and:

```text
do not silently create a canonical instrument
```

Instead:

```text
record unresolved mapping
store raw source data
increment metric
log warning
quarantine normalized row if needed
```

Provide an operator-visible unresolved-instrument report.

---

# 17. Symbol Normalization

AKShare may expose symbols using different formatting conventions.

Normalization may include:

```text
case normalization
exchange suffix normalization
contract-month formatting
continuous-symbol recognition
whitespace removal
Chinese-name mapping
```

Provider symbol normalization must occur before canonical instrument resolution.

Never alter the raw provider symbol stored in lineage metadata.

Preserve both:

```text
raw_provider_symbol
normalized_provider_symbol
```

where useful.

---

# 18. Continuous Contract Data

AKShare continuous/main-contract datasets are supplemental/reference data.

They must NOT replace the backend's internally generated:

```text
continuous_contract_mapping
main contract selection
roll rules
back-adjusted series
```

AKShare continuous contracts may be used for:

```text
comparison
validation
research
fallback history
```

but the internally defined continuous-contract methodology remains authoritative for this backend.

---

# 19. Source Lineage

Every AKShare ingestion must record sufficient lineage.

At minimum:

```text
provider = AKSHARE

AKShare function name

upstream source identifier
if known

provider symbol

request parameters

fetch timestamp

AKShare package version

normalizer/schema version
```

Where practical also record:

```text
row count
response hash
source date range
```

---

# 20. Upstream Source Must Be Distinguished from AKShare

AKShare is often an aggregation/access layer.

Therefore distinguish:

```text
provider
```

from:

```text
upstream_source
```

Example conceptual metadata:

```text
provider = AKSHARE
upstream_source = SINA
```

or:

```text
provider = AKSHARE
upstream_source = SHFE
```

where the source can be reliably identified.

Do not represent all AKShare data as if AKShare itself were the originating exchange.

---

# 21. Raw Data Preservation

Before irreversible normalization, preserve the fetched source dataset.

Preferred format:

```text
Parquet + ZSTD
```

Conceptual path:

```text
data/raw/
  provider=akshare/
    dataset=futures_daily/
      fetch_date=2026-09-05/
        ...
```

Raw archives must retain provider-native columns.

---

# 22. Raw Archive Immutability

Raw fetches must be immutable.

If the same dataset is fetched again:

```text
do not overwrite the previous raw fetch
```

Store another fetch version.

This allows analysis of:

```text
source revisions
schema changes
provider corrections
historical corrections
```

---

# 23. Fetch Identity

Each fetch operation should have:

```text
fetch_id
```

Example:

```text
UUID
```

All raw and normalized records generated from one fetch should be traceable to that fetch.

Conceptually:

```text
fetch_id
provider
endpoint
started_at
completed_at
status
rows_received
rows_accepted
rows_rejected
```

---

# 24. Ingestion Run Metadata

Add a PostgreSQL table or equivalent metadata store:

```text
provider_ingestion_runs
```

Suggested fields:

```text
id
provider_id
dataset
endpoint

started_at
completed_at

status

request_parameters JSONB

rows_received
rows_normalized
rows_rejected
rows_written

error_code
error_message

provider_version
schema_version
```

---

# 25. Storage Routing

Different AKShare data classes should go to different storage systems.

## Historical time series

Examples:

```text
daily futures bars
historical price series
```

Canonical data may go to:

```text
QuestDB
+
Parquet
```

---

## Reference data

Examples:

```text
contract metadata
exchange information
product metadata
```

Canonical data goes to:

```text
PostgreSQL
```

---

## Large raw datasets

Store primarily in:

```text
Parquet
```

and query via:

```text
DuckDB
```

---

# 26. No Direct Storage Logic Inside Endpoint Adapter

Prohibited:

```python
def fetch_daily(...):
    df = ak.xxx(...)
    questdb.write(...)
```

Required:

```text
Endpoint Adapter
    ↓
Canonical Batch
    ↓
Ingestion Service
    ↓
Storage Repository
```

This keeps provider acquisition separate from persistence.

---

# 27. Repository Layer

Use provider-independent repositories where existing architecture permits.

Conceptually:

```text
HistoricalBarRepository
ReferenceDataRepository
RawArchiveRepository
IngestionRunRepository
```

The AKShare module should produce data, not own storage policy.

---

# 28. Idempotency

Scheduled historical retrieval must be safe to run repeatedly.

Example:

```text
job runs
→ downloads 2026-09-04

job retries
→ downloads 2026-09-04 again
```

Canonical storage must not accumulate uncontrolled duplicate bars.

---

# 29. Historical Bar Identity

Logical canonical identity should include at minimum:

```text
provider
instrument_id
interval
bar_start
```

If source distinctions require it, include:

```text
upstream_source
```

Do not use:

```text
fetch_id
```

as the bar identity.

`fetch_id` is lineage, not market-data identity.

---

# 30. Historical Revisions

Historical providers may return revised values.

Therefore repeated ingestion of:

```text
same provider
same instrument
same interval
same bar_start
```

with changed market values must be detected.

Do not simply ignore revisions.

Record:

```text
revision detected
previous value
new value
fetch_id
fetched_at
```

according to the existing data-version policy.

Raw source versions must always remain recoverable.

---

# 31. Revision Policy

Canonical query behavior should normally expose the latest accepted version.

Raw/archive data must preserve previous versions.

Conceptually:

```text
RAW
fetch 1 → value A

RAW
fetch 2 → value B

CANONICAL
latest accepted → value B
```

The implementation must not destroy evidence that value A was previously supplied.

---

# 32. QuestDB Historical Writes

If canonical historical bars are written to QuestDB:

* use explicit schema
* use deterministic identity
* use DEDUP where appropriate
* do not reuse QWP realtime `producer_id + seq` semantics as the primary bar identity
* preserve provider and source metadata

Historical data has different semantic identity from realtime received events.

---

# 33. Historical vs Realtime Identity

Do not confuse:

```text
Realtime event identity:

producer_id + seq
```

with:

```text
Historical bar identity:

provider
instrument_id
interval
bar_start
```

These solve different problems.

---

# 34. Trading Day

For futures daily data, preserve:

```text
trading_day
```

explicitly.

Do not blindly infer futures trading day from arbitrary timestamps if the provider supplies a date intended as the trading date.

Use the canonical PostgreSQL trading calendar for validation.

---

# 35. Timezones

Canonical timestamps must use the backend's established UTC conventions.

Provider-native dates/times must be interpreted using the appropriate market timezone.

Chinese futures:

```text
Asia/Shanghai
```

Do not assume server-local timezone.

---

# 36. Data Type Normalization

Normalize incoming DataFrame types explicitly.

Do not depend on pandas inferred dtypes remaining stable.

Validate and convert:

```text
dates
timestamps
floats
integers
nullable integers
strings
booleans
```

before canonical ingestion.

---

# 37. Missing Values

Normalize:

```text
NaN
NaT
None
empty strings
provider-specific missing markers
```

according to canonical nullable semantics.

Never allow:

```text
NaN
```

to unintentionally become a legitimate database numeric value when NULL is appropriate.

---

# 38. Schema Validation

Every endpoint adapter must define expected columns.

Example:

```python
REQUIRED_COLUMNS = {
    "date",
    "open",
    "high",
    "low",
    "close",
}
```

Optional fields should be explicitly identified.

If required columns disappear:

```text
FAIL VISIBLE
```

Do not silently continue with incorrectly shifted field mappings.

---

# 39. Schema Drift

AKShare or its upstream sources may change returned columns.

Implement schema-drift detection.

Detect:

```text
missing required columns
unexpected renamed columns
unexpected datatype changes
unexpected empty responses
unexpected duplicate rows
```

Emit:

```text
metric
warning/error
failed ingestion run
```

where appropriate.

---

# 40. Do Not Guess Changed Columns

If an endpoint previously returned:

```text
hold
```

and suddenly returns an unrecognized replacement field:

```text
do not automatically guess its meaning
```

Fail the normalizer for that dataset and retain the raw response.

This prevents silent data corruption.

---

# 41. Data Quality Rules

Historical bars must validate:

```text
high >= low

low <= open <= high
when values are present

low <= close <= high
when values are present

volume >= 0
when semantically appropriate

open_interest >= 0
when semantically appropriate

valid trading date
```

Invalid rows must be observable.

---

# 42. Cross-Source Validation

Where the same market data exists from another provider, optionally support validation.

Example:

```text
AKShare daily close
vs
internally derived CTP daily close
```

Calculate differences but do not automatically overwrite one source based solely on mismatch.

Expose discrepancy reports.

---

# 43. Provider Priority

AKShare must not automatically override higher-authority data.

Initial suggested priority for Chinese futures:

```text
Realtime:
CTP

Internally derived bars:
CTP-derived

Historical supplemental:
AKShare

Reference/exchange-public:
source dependent
```

Final authority rules should be configured explicitly.

---

# 44. Data Provenance in API

Where historical data may come from different providers, API output should optionally expose:

```text
provider
source
```

Example concept:

```json
{
  "instrument_id": 10001,
  "date": "2026-09-04",
  "close": 22150,
  "provider": "AKSHARE",
  "source": "SINA"
}
```

Do not pretend provider origin is irrelevant.

---

# 45. Scheduler

Implement scheduled collection without introducing heavyweight infrastructure.

Preferred:

```text
existing project scheduler
```

or a simple dedicated Python scheduler/service.

Acceptable alternatives include:

```text
systemd timer
cron
lightweight in-process scheduler
```

Do NOT introduce:

```text
Celery
Kafka
RabbitMQ
Redis
```

solely for AKShare scheduling.

---

# 46. Job Types

Support jobs such as:

```text
daily futures update

historical backfill

reference-data refresh

inventory refresh

position/ranking refresh

validation job
```

Each job must have a unique job identity and observable status.

---

# 47. Scheduling Must Be Dataset-Specific

Do not run every dataset every minute.

Example conceptual cadence:

```text
contract/reference metadata
→ daily or less often

daily bars
→ after market close

inventory
→ according to publication schedule

historical backfill
→ on demand
```

Cadence must be configurable.

---

# 48. Backfill Mode

Provide an explicit historical backfill workflow.

Conceptually:

```bash
python -m providers.akshare.backfill \
  --dataset futures_daily \
  --instrument SHFE.zn2610 \
  --start 2020-01-01 \
  --end 2026-09-04
```

Exact CLI may differ.

---

# 49. Incremental Update Mode

Normal operation should avoid repeatedly downloading entire history where the endpoint supports narrower requests.

Conceptually determine:

```text
latest stored canonical date
        ↓
request missing range
        ↓
ingest
```

If a specific AKShare endpoint only returns full history, handle idempotency correctly instead.

---

# 50. Request Throttling

Implement provider-wide and endpoint-specific throttling.

Do not issue uncontrolled loops such as:

```python
for every instrument:
    call immediately
```

without pacing.

Configuration should support:

```text
minimum request interval

maximum concurrent requests

endpoint-specific cooldown

retry limit
```

---

# 51. Concurrency

Do not assume more concurrency is always better.

AKShare calls are often wrappers around external public endpoints.

Use conservative bounded concurrency.

Initial default should favor:

```text
reliability
```

over maximum download speed.

---

# 52. Retry Policy

Retry transient failures.

Examples:

```text
connection reset
timeout
temporary upstream failure
HTTP transient error
```

Use:

```text
bounded retry count
exponential backoff
jitter
```

Do not retry indefinitely.

---

# 53. Permanent Failure

Do not repeatedly retry:

```text
invalid symbol
unsupported endpoint
schema mismatch
invalid parameters
mapping failure
```

as if they were transient network errors.

Classify errors.

---

# 54. Error Classification

Define errors such as:

```text
TransientProviderError
PermanentProviderError
SchemaError
MappingError
ValidationError
EmptyDatasetError
RateLimitError
```

Reuse project-wide error models where possible.

---

# 55. Empty Response Semantics

An empty DataFrame does not always mean:

```text
valid zero-row result
```

It may mean:

```text
invalid symbol
upstream unavailable
non-trading date
endpoint change
```

Each adapter must explicitly define how an empty response is interpreted.

Do not silently mark all empty responses successful.

---

# 56. Provider Health

Expose AKShare provider health independently.

Conceptual state:

```text
AVAILABLE
DEGRADED
UNAVAILABLE
```

Health should consider:

```text
recent successful request
recent failure rate
schema failures
last successful fetch
```

Do not use socket-oriented concepts such as `CONNECTED` where they do not make sense.

---

# 57. Metrics

Add metrics such as:

```text
akshare_requests_total

akshare_requests_failed_total

akshare_request_latency_seconds

akshare_rows_received_total

akshare_rows_normalized_total

akshare_rows_rejected_total

akshare_schema_errors_total

akshare_mapping_errors_total

akshare_retries_total

akshare_last_success_timestamp

akshare_ingestion_runs_total

akshare_ingestion_failures_total
```

Prefer generic provider labels if the Phase 09 metrics architecture supports them.

Example:

```text
provider_requests_total{provider="akshare"}
```

---

# 58. Dataset-Level Metrics

Where useful expose:

```text
provider_rows_received_total{
    provider="akshare",
    dataset="futures_daily"
}
```

Avoid unbounded labels such as individual instrument symbols if they create excessive metric cardinality.

---

# 59. Logging

Structured logs must include:

```text
provider=AKSHARE
dataset
endpoint
fetch_id
duration
row_count
status
```

Never log full DataFrames at INFO.

Never log every row.

---

# 60. Configuration

Add:

```yaml
providers:

  akshare:

    enabled: true

    historical:
      enabled: true

    reference:
      enabled: true

    best_effort_quotes:
      enabled: false

    rate_limit:
      max_concurrency: 2
      min_interval_ms: 500

    retry:
      max_attempts: 3

    raw_archive:
      enabled: true
```

Exact configuration format should follow existing project conventions.

---

# 61. AKShare Version Pinning

Pin AKShare in the Python dependency configuration.

Do not deploy production code against:

```text
unbounded latest version
```

without lockfile/version control.

Record the active AKShare package version in ingestion lineage.

---

# 62. Dependency Isolation

AKShare must be an optional Python dependency.

The following must continue working without AKShare installed:

```text
C++ build

CTP provider

IBKR provider

FastAPI core where AKShare is disabled

synthetic mode
```

If AKShare provider is enabled but the package is missing:

```text
fail with clear provider-specific error
```

not a cryptic global startup failure.

---

# 63. Docker

The AKShare worker may run in a dedicated container.

Conceptually:

```text
docker-compose:

questdb
postgres
fastapi
akshare-worker
```

CTP may continue on the host if required.

The AKShare worker must not require CTP SDK libraries.

---

# 64. FastAPI Integration

Expose historical/reference retrieval through existing FastAPI APIs rather than creating an independent AKShare web API.

Example:

```text
GET /v1/bars/...
```

should continue operating on canonical storage.

The client should not need to call:

```text
/v1/akshare/...
```

for normal market data.

Provider-specific diagnostics/admin endpoints may exist separately if needed.

---

# 65. On-Demand Refresh

If implemented, an admin-level refresh endpoint may request:

```text
refresh historical dataset
```

but the normal data API must not trigger AKShare network calls synchronously.

Prohibited:

```text
GET /v1/bars/zn2610
        ↓
call AKShare
        ↓
wait
        ↓
return
```

Normal queries should read canonical storage.

---

# 66. Best-Effort Realtime Quotes

Real-time AKShare polling is explicitly OUT OF PRIMARY SCOPE for Phase 11.

Do NOT initially connect AKShare polling to the normal realtime quote path.

This avoids creating an implicit assumption that AKShare and CTP have equivalent realtime semantics.

---

# 67. Optional Phase 11B

If explicitly requested after Phase 11 acceptance, implement:

```text
Phase 11B
AKShare Best-Effort Quote Provider
```

Possible architecture:

```text
AKShare Poller
      ↓
QuoteSnapshot
      ↓
provider-independent Live Event Service
      ↓
ZeroMQ
      ↓
FastAPI
```

It must NOT:

```text
go through the CTP callback path
pretend to be tick-by-tick
pretend to be primary realtime
```

---

# 68. Best-Effort Quote Metadata

If Phase 11B is implemented, all emitted quotes must contain:

```text
provider = AKSHARE

quality = BEST_EFFORT
```

and where known:

```text
upstream_source
source_timestamp
fetch_timestamp
```

FastAPI must expose this quality state.

---

# 69. No Silent Fallback

Never implement:

```text
CTP down
→ silently show AKShare as if it were CTP
```

If later fallback is explicitly added, clients must be informed:

```json
{
  "provider": "AKSHARE",
  "quality": "BEST_EFFORT",
  "fallback": true
}
```

Provider transparency is mandatory.

---

# 70. Testing Without Internet

CI must NOT depend on live AKShare endpoints.

All normal tests must use:

```text
mocked DataFrames
fixtures
recorded schema samples
fake provider clients
```

Network integration tests must be optional.

---

# 71. Client Abstraction

Wrap AKShare itself.

Do not call:

```python
ak.some_function(...)
```

from every adapter independently without a test seam.

Preferred:

```python
class AkshareClient:
    ...
```

Adapters depend on this client abstraction.

Tests can replace it with:

```text
FakeAkshareClient
```

---

# 72. Required Unit Tests

At minimum test:

```text
provider capabilities

provider health

endpoint registry

historical bar normalization

date normalization

numeric normalization

NaN handling

schema validation

schema drift detection

symbol normalization

instrument mapping

unresolved symbol handling

source lineage generation

fetch_id generation

revision detection

idempotency

retry classification

empty response handling
```

---

# 73. Futures Daily Fixture Tests

Provide representative fixtures for multiple exchanges.

At minimum:

```text
SHFE
DCE
CZCE
CFFEX
GFEX
INE
```

where supported by the implemented dataset.

Do not assume all exchanges use identical symbol casing or field conventions.

---

# 74. Historical Idempotency Test

Execute the same normalized dataset twice.

Expected:

```text
raw archives:
2 fetch versions may exist

canonical bars:
1 logical bar per
provider/instrument/interval/bar_start
unless revision policy retains explicit versions
```

No uncontrolled duplicates.

---

# 75. Historical Revision Test

First fetch:

```text
2026-09-01
close = 22000
```

Second fetch:

```text
2026-09-01
close = 22010
```

Expected:

```text
revision detected

previous raw fetch retained

new raw fetch retained

canonical latest accepted value = 22010
```

according to configured revision policy.

---

# 76. Mapping Failure Test

Given:

```text
unknown AKShare symbol
```

Expected:

```text
raw data preserved
canonical row not silently misassigned
mapping error metric incremented
ingestion run reflects rejected row
```

---

# 77. Schema Drift Test

Given an endpoint fixture missing a required column:

Expected:

```text
normalization fails visibly

raw response preserved

canonical database not corrupted

schema error recorded
```

---

# 78. Data Quality Test

Test invalid bars such as:

```text
high < low
negative volume
close > high
```

Verify they are rejected/quarantined according to existing data-quality policy.

---

# 79. PostgreSQL Integration Tests

Test:

```text
provider registry

provider instrument mapping

ingestion run metadata

reference upsert

revision metadata
```

Use test database/container infrastructure already present in the project.

---

# 80. QuestDB Integration Tests

For historical bars stored in QuestDB verify:

```text
provider identity

instrument identity

bar timestamp

OHLC

volume

open interest

settlement

idempotent re-ingestion
```

---

# 81. Parquet Tests

Verify raw archive preserves:

```text
provider-native columns

provider

upstream source

fetch_id

fetch timestamp

AKShare version

request parameters
```

and can be read successfully.

---

# 82. DuckDB Tests

Verify DuckDB can query AKShare archives by:

```text
provider

dataset

instrument

date range
```

Example concept:

```sql
SELECT *
FROM read_parquet('data/raw/provider=akshare/**/*.parquet')
WHERE ...
```

---

# 83. Existing System Regression

Phase 11 must not break:

```text
CTP realtime ingestion

IBKR if Phase 10 exists

QuestDB QWP pipeline

QWP DEDUP

ZeroMQ live feed

FastAPI WebSocket

PostgreSQL metadata

Parquet existing archive

DuckDB existing research layer

bar generation

continuous contracts

monitoring

data quality
```

---

# 84. Performance

AKShare is not a low-latency provider.

Do not optimize it using the same targets as CTP callbacks.

Performance priorities are:

```text
reasonable throughput
bounded concurrency
memory safety
idempotency
reliability
observable progress
```

Historical bulk loads should process data in batches rather than one database write per row.

---

# 85. Memory Management

Large DataFrames must not be retained indefinitely.

For large backfills:

```text
fetch
normalize
persist/archive
release
```

in bounded batches where practical.

Avoid accumulating years of all instruments into one in-memory DataFrame.

---

# 86. Backfill Progress

Long backfills must expose progress.

Example:

```text
requested instruments
completed instruments
failed instruments
rows fetched
rows written
current dataset
elapsed time
```

Partial backfill failure must be restartable.

---

# 87. Resume Support

A failed backfill should be able to resume without restarting successfully completed work.

Use:

```text
ingestion-run metadata
canonical storage state
raw archive state
```

rather than relying solely on an in-memory counter.

---

# 88. CLI

Provide an operational CLI or equivalent command surface.

Conceptual commands:

```text
akshare-provider health

akshare-provider list-datasets

akshare-provider fetch ...

akshare-provider backfill ...

akshare-provider refresh-reference ...

akshare-provider unresolved-symbols
```

Exact CLI framework should follow existing Python tooling.

---

# 89. Dataset Discovery

Do not automatically enable every AKShare endpoint found in the library.

Only endpoints explicitly registered in:

```text
EndpointRegistry
```

are production-supported.

This prevents an AKShare package update from unexpectedly expanding production behavior.

---

# 90. Endpoint Stability Classification

Optionally classify endpoints:

```text
STABLE
EXPERIMENTAL
DISABLED
```

Experimental adapters must not be scheduled by default.

---

# 91. Documentation

Create:

```text
docs/providers/akshare.md
```

Document:

```text
provider purpose

supported datasets

AKShare package version

endpoint registry

upstream sources

canonical mappings

scheduler

backfill

rate limiting

revision policy

raw archive format

troubleshooting

known limitations
```

---

# 92. Update Existing Documentation

Update:

```text
REQUIREMENTS.md
ROADMAP.md
README.md
CHANGELOG.md
docs/providers.md
docs/data-model.md
docs/architecture.md
```

Do not rewrite unrelated documentation.

---

# 93. Explicit Non-Scope

Do NOT implement in Phase 11:

```text
primary realtime AKShare feed

high-frequency polling

tick-by-tick reconstruction

market depth

automatic CTP fallback

automatic IBKR fallback

provider arbitration

trading API

orders

positions

accounts

Kafka

Redis Streams

Celery

RabbitMQ

Kubernetes

every available AKShare endpoint
```

---

# 94. Phase 11 Definition of Done

Phase 11 is complete when all of the following are true:

1. AKShare exists as a registered Provider.
2. AKShare is implemented in Python.
3. AKShare runs independently from the C++ realtime core.
4. Historical provider interface is implemented.
5. Reference provider interface is implemented.
6. Provider capabilities are correct.
7. Supported datasets are explicitly registered.
8. Historical futures bars can be fetched.
9. Provider-native data is normalized to canonical models.
10. AKShare symbols resolve through canonical InstrumentId mappings.
11. Unresolved mappings are observable.
12. Source lineage is preserved.
13. AKShare library version is recorded.
14. Fetch operations receive stable fetch IDs.
15. Raw responses are archived immutably.
16. Canonical historical ingestion is idempotent.
17. Historical revisions are detectable.
18. Schema drift is detected.
19. Invalid data cannot silently corrupt canonical storage.
20. Rate limiting exists.
21. Retry logic exists.
22. Retry behavior is bounded.
23. Scheduled collection works.
24. Backfill works.
25. Backfill is resumable.
26. PostgreSQL reference ingestion works.
27. QuestDB historical bar ingestion works where applicable.
28. Parquet raw archive works.
29. DuckDB can query AKShare archives.
30. Provider health is exposed.
31. Provider metrics exist.
32. CI tests require no external AKShare network access.
33. Existing Phase 0–10 functionality remains operational.
34. CTP remains the primary Chinese futures realtime provider.
35. AKShare realtime polling is NOT enabled by default.
36. No automatic provider fallback exists.
37. Documentation is complete.
38. CHANGELOG is updated.

---

# 95. Required Codex Work Process

Before implementation:

1. Read `REQUIREMENTS.md`.
2. Read `ROADMAP.md`.
3. Read `docs/providers.md`.
4. Read `docs/providers/akshare.md` if already present.
5. Read this Phase 11 specification completely.
6. Inspect Phase 09 provider abstractions.
7. Inspect PostgreSQL provider mappings.
8. Inspect canonical bar models.
9. Inspect QuestDB bar schemas.
10. Inspect raw Parquet archive architecture.
11. Inspect DuckDB research utilities.
12. Inspect scheduler/maintenance infrastructure.
13. Run the complete existing test suite.
14. Identify reusable provider-independent services.
15. Identify any conflict with the installed AKShare version.
16. Produce a concise implementation plan.
17. List files to create.
18. List files to modify.
19. List migrations.
20. List supported AKShare endpoints proposed for initial implementation.

Only then begin writing production code.

---

# 96. Endpoint Verification Rule

Before implementing any AKShare endpoint:

1. inspect the AKShare version pinned by the project
2. verify the function exists in that version
3. inspect its current parameters
4. inspect returned columns
5. identify the upstream source where possible
6. create a fixture representing the observed schema
7. implement the adapter against that verified schema

Do NOT implement AKShare calls from memory.

Do NOT invent undocumented parameters.

---

# 97. Initial Delivery Strategy

Do not attempt all datasets simultaneously.

Recommended order:

```text
Step 1
AKShare provider shell
+
client abstraction
+
health
+
endpoint registry

Step 2
instrument/symbol resolution

Step 3
one futures daily historical adapter

Step 4
raw archive
+
lineage

Step 5
canonical bar persistence

Step 6
idempotency/revisions

Step 7
additional futures exchanges

Step 8
reference datasets

Step 9
inventory/position datasets

Step 10
scheduler

Step 11
backfill/resume

Step 12
DuckDB integration

Step 13
metrics/data quality

Step 14
regression tests

Step 15
documentation
```

Keep tests passing after each significant step.

---

# 98. No Blind Endpoint Expansion

After the first endpoint works, do not automatically copy/paste adapters for dozens of AKShare APIs.

For each additional endpoint define:

```text
business purpose

upstream source

canonical destination

refresh frequency

schema

identity

revision behavior

data-quality rules
```

before enabling it.

---

# 99. Architectural Success Criterion

Before Phase 11:

```text
AKShare
=
external Python package
```

After Phase 11:

```text
AKShare
=
controlled, observable, versioned
Historical / Reference Provider
```

The backend must be able to answer:

```text
Where did this value come from?

When was it fetched?

Which AKShare endpoint produced it?

Which upstream source did that endpoint represent?

Which canonical instrument does it belong to?

Was this value revised later?

Can the original source response be reproduced or inspected?
```

If these questions cannot be answered, the AKShare integration is incomplete.

---

# 100. Final Codex Instruction

After completing Phase 11:

1. run the complete test suite
2. run AKShare provider tests
3. run PostgreSQL integration tests
4. run QuestDB historical-data integration tests
5. run Parquet/DuckDB tests
6. report supported AKShare datasets
7. report exact AKShare version used
8. report every endpoint implemented
9. report each endpoint's upstream source where known
10. report database migrations
11. report raw archive layout
12. report revision behavior
13. report unresolved-symbol behavior
14. report test coverage
15. report known unstable endpoints
16. report remaining risks
17. update documentation
18. update CHANGELOG
19. do NOT enable best-effort realtime polling unless explicitly requested
20. do NOT begin the next phase automatically
