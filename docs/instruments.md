# Futures instrument identity

Canonical identity and metadata registration are separate concerns. A physical contract with a deterministic provider/exchange rule does not need a pre-existing `provider_instruments` or `futures_contracts` row. Recognition is not evidence that a contract is listed, active, or tradable.

## Resolution boundary

`python/instruments` has no provider SDK, HTTP, QuestDB, or CTP callback dependency. `ProviderInstrumentResolver.resolve_raw(provider, raw_symbol, exchange_hint=..., as_of=...)` returns a typed `InstrumentResolution`:

1. Exact explicit provider mapping.
2. Unique normalized explicit mapping.
3. Provider product/alias classification and deterministic provider/exchange parsing.
4. Optional metadata registration lookup.
5. An explicit unresolved reason when identity is insufficient.

Raw provider input, normalized comparison key, canonical identity, instrument kind, resolution method, delivery month/tenor, mapping precedence, metadata registration, and conflict status remain separate fields. Conflicting exchange prefix and caller hint fail with `EXCHANGE_HINT_CONFLICT`. An explicit mapping overrides a different parser result and emits `provider_mapping_parser_conflict`; its metadata describes the explicit target. Ambiguous normalized mappings fail rather than selecting an arbitrary latest row. Active mappings use inclusive validity dates, defaulting to today's date for lookup. CZCE parsing never silently borrows that default date.

`format_provider_symbol` performs explicit reverse lookup before deterministic formatting, then checks the forward round trip. It refuses a formatter result that would resolve to another canonical instrument because of an override. `resolve() -> (exchange, local_code)` and `provider_symbol() -> (exchange, local_code, provider_symbol)` remain compatibility wrappers on the AKShare metadata repository. New ingestion consumes the full canonical identity from the generic resolver.

## Domestic rules

AKShare normalization trims surrounding spaces, folds ASCII case, accepts supported exchange prefixes (`SHFE.RB2610`, `SHFE_RB2610`, `SHFE-RB2610`) and the existing dot suffix (`RB2610.SHFE`), recognizes a single product/month separator, and supports unambiguous reversed `2610RB`. It does not strip arbitrary punctuation or internal whitespace. Unknown providers retain case unless their dialect is explicitly implemented.

Vetted product registries cover SHFE, INE, DCE, CZCE, CFFEX, and GFEX. Unknown roots are unresolved; known roots on another exchange produce `PRODUCT_EXCHANGE_MISMATCH`. INE products are separate from SHFE despite the provider's umbrella exchange list. Examples:

| Provider input | Context | Canonical identity | Kind |
|---|---|---|---|
| RB2610 / rb2610 / RB-2610 / 2610RB | SHFE | SHFE.rb2610 | PHYSICAL_FUTURE |
| CU2610 | SHFE | SHFE.cu2610 | PHYSICAL_FUTURE |
| I2701 | DCE | DCE.i2701 | PHYSICAL_FUTURE |
| IF2612 | CFFEX | CFFEX.if2612 | PHYSICAL_FUTURE |
| LC2701 | GFEX | GFEX.lc2701 | PHYSICAL_FUTURE |
| SR701 | CZCE, as_of=2026-09-05 | CZCE.sr2701 | PHYSICAL_FUTURE |
| RB0 | AKShare/Sina | SHFE.rb.continuous | CONTINUOUS_FUTURE |

`expand_yymm` centralizes year handling: without `as_of`, the documented identity epoch is 2000–2099. With an explicit date it chooses the unique year within ±49 years; the 50-year tie is unresolved. Canonical codes retain the existing YYMM spelling; use a consistent explicit century context for historical research.

CZCE YMM requires `as_of` and a unique delivery month in the narrow window from 12 months before to 36 months after that month. Outside this window, supply a full YYMM or a dated explicit alias. Historical callers pass the requested end/start date; the live poller explicitly uses today's Shanghai date. Reverse AKShare formatting uses full YYMM, including CZCE. Provider-native three-digit exceptions can use explicit aliases. RB00/RB888/RB999 are not physical contracts and remain unresolved synthetic conventions. No exchange holidays, listing dates, expiry dates, multiplier, tick size, or currency are invented.

## Foreign rules and provider definitions

All 12 international month codes are supported. The initial vetted AKShare root + YY + month-code dialect recognizes GC/SI/HG (COMEX), CL/NG (NYMEX), and SB/CT (ICEUS). Examples: GC25Z → COMEX.gc2512; CL26F → NYMEX.cl2601; NG26H → NYMEX.ng2603; SB26K → ICEUS.sb2605. Other providers can inject their own `ForeignProductDefinition` and `ProviderProductDefinition` records; an AKShare symbol convention is not assumed for IBKR.

A root-only Sina GC is the provider's gold product series, represented as COMEX.gc.continuous. This does not identify its physical front month, specify a roll schedule, or claim exchange-direct delivery. Provider aliases CAD/ZSD/AHD/NID/PBD/SND identify the underlying LME three-month rolling reference: e.g. ZSD → LME.zn.3m, `kind=ROLLING_TENOR`, `tenor=P3M`, `delivery_month=null`. Sina detail pages may describe the price service as CFD quotes; this identity describes the referenced tenor, not a claim of a tradable exchange-native contract. Kind models also support INDEX, SPOT, CFD, SYNTHETIC, and UNKNOWN via explicit definitions. Unrecognized foreign product rows are retained as UNKNOWN reference records and do not gain fabricated identities.

Reference sources verified for this implementation:

- Installed AKShare **1.18.74**, `futures/cons.py` (`market_exchange_symbols`, display-name comments), `futures_zh_spot` and `futures_hq_subscribe_exchange_symbol`. No SDK is imported by the generic package.
- [AKShare futures API documentation](https://akshare.akfamily.xyz/data/futures/futures.html): Eastmoney physical symbol examples, Sina product-code names, and LME 3M reference definitions. Runtime APIs were checked against installed 1.18.74, not assumed from the moving documentation version.
- [CME month codes](https://www.cmegroup.com/month-codes.html), [CME product-code advisory](https://www.cmegroup.com/tools-information/lookups/advisories/market-regulation/NYMEX_COMEX_RA1006-4.html), and [ICE Sugar No. 11](https://www.ice.com/products/23).

These registries are deliberately finite and require reviewed updates for new products. An unknown code never uses fuzzy matching.

## PostgreSQL storage and administration

No PostgreSQL migration is needed. Existing `provider_instruments` rows, foreign keys, validity dates and metadata remain intact. Existing physical rows can continue receiving aliases there. For an unregistered physical identity or a nonphysical instrument, administrative aliases are stored in `providers.metadata.instrument_aliases` as serialized `ExplicitMapping` records. This avoids creating fake physical contracts. A metadata registration check still returns false for an aliased physical contract that has no `futures_contracts` row.

`providers.metadata.product_definitions` holds reviewed provider product definitions. The `futures_foreign_products` reference dataset reuses `provider_reference_records.payload.definition`. Operator definitions take precedence over synchronized definitions, which take precedence over bundled rules. Reference sync validates the code/name pair for known definitions, archives every response, and fails visibly on semantic drift. It does not insert physical contracts or alter explicit aliases. For physical metadata, only real `futures_contracts` registration makes `metadata_registered=true`; absence is false. Offline/unchecked registration is null.

No auto-registration is implemented. `resolve-instrument`, `provider-symbol`, `list-instruments`, and `audit-instruments` do not write database state. Only `add-instrument` is an administrative mutation; it refuses conflicting normalized aliases. Existing ambiguous database rows remain readable for diagnostics; exact mappings retain priority and audits report the ambiguity.

## AKShare CLI

Run through the optional worker image, or `PYTHONPATH=python python -m providers.akshare`:

```sh
akshare-worker resolve-instrument --exchange SHFE RB2610 --json
akshare-worker resolve-instrument --exchange CZCE SR701 --as-of 2026-09-05 --json
akshare-worker resolve-instrument GC25Z --json
akshare-worker resolve-instrument ZSD --json
akshare-worker provider-symbol SHFE.rb2610 --json
akshare-worker provider-symbol COMEX.gc2512 --json
akshare-worker add-instrument SHFE.rb2610 --provider-symbol SPECIAL_RB --json
akshare-worker list-instruments --json
akshare-worker audit-instruments --json
akshare-worker refresh-reference --dataset foreign-products
akshare-worker fetch futures-daily --instrument SHFE.rb2610
akshare-worker fetch futures-1m --instrument SHFE.rb2610
akshare-worker fetch futures-foreign-daily --instrument LME.zn.3m
```

`akshare-worker` above denotes the `akshare-worker` Compose service (`docker compose --profile akshare run --rm akshare-worker ...`). The actual Python entry point is `python -m providers.akshare`. The legacy `fetch RB2610 --exchange SHFE` form remains supported for daily data.

Identity commands require no AKShare SDK or QuestDB. If `POSTGRES_DSN` is configured, it is used and connection failures are not silently ignored. With `--offline` or no DSN, only bundled deterministic rules are consulted; JSON states `mapping_source=offline_rules_only` and `metadata_registered=null`. Production ingestion still requires PostgreSQL to honor operator overrides and record lineage/runs. Exit codes: 0 successful resolution/clean audit, 2 unresolved/conflicting audit, 1 operational failure.

Domestic daily/1m/quote adapters consume the resolver. Foreign Sina daily history uses the separate `futures-foreign-daily` dataset, formats canonical identities such as `LME.zn.3m` to aliases such as `ZSD`, and calls `futures_foreign_hist`. Cross-routing is rejected: foreign identities never pass through domestic daily/1m endpoints, and domestic identities never pass through the foreign endpoint. Eastmoney remains a distinct namespace; its physical month-code symbols are resolvable, but `futures_global_hist_em` ingestion is not enabled in this task.

## Quote association and observability

A complete response batch must match the requested normalized native-symbol set. Duplicate requested aliases, duplicate returned symbols, missing symbols, unexpected symbols, and prefix conflicts fail before any event in that batch is emitted. Pinned AKShare's `symbol` may be a Chinese display name, so a finite provider-local name registry handles known names such as 螺纹钢2610, 铜2610, and 沪锌2610. Unknown display names fail; `adjust=1` metadata assigned by the upstream library's positional processing is not used as proof of identity. Earlier valid batches in a poll may already have been emitted when a later batch fails; transport identity remains the original process producer ID and increasing sequence.

`instrument_resolution_total`, explicit/normalized/parser/unresolved/conflict counters, `instrument_metadata_missing_total`, and enum-bounded `instrument_kind_total` are process-local and available through `ResolutionMetrics.render()` / AKShare metric rendering. No symbol labels are used. Existing short-lived `metrics` CLI commands report their own process counters, not a separate running worker's counters. Warning logs attach structured resolution context; historical unresolved records retain it in existing `resolution_note` JSON as well as raw lineage. Native symbol, raw returned name, kind and upstream source are exposed in live API responses; missing numeric observations remain null.

## Deployment and compatibility

Apply **QuestDB migrations 008–014 before deploying the new API/worker/archive reader** (`docker compose run --rm questdb-init`). Each file contains one statement because the pinned HTTP SQL endpoint rejects multiple statements in one request. The migrations add nullable provenance columns only; no tables, primary identities or DEDUP keys are replaced.

Historical writes now consistently use full `EXCHANGE.instrument` canonical IDs, correcting a mismatch between the old database resolver's local-code tuple and normalizer expectations. API reads accept both old local IDs and canonical IDs. If both exist for the same provider/interval/bar timestamp, the canonical row wins in that read projection; stored legacy rows are not deleted or rewritten. Old and new spellings can still coexist physically, so use a separately reviewed migration if physical consolidation is required. The existing historical DEDUP columns are unchanged.

New snapshot Parquet archives use schema v3 to retain native/raw symbols, instrument kind, source and upstream source. Completed v1/v2 archives are immutable and still readable. Archive/research paths accept only the defined typed suffixes; arbitrary dots/path traversal remain invalid. The optional Python live writer now encodes `recv_ts` with the verified nanosecond `n` field suffix, correcting the previous overflowing microsecond encoding; replay identity is unchanged. The C++ callback, normalizer, queues, writer, PUB behavior and provider selection are untouched.
