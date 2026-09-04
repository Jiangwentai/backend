# ADR 0001: Provider-local SPSC queues with Dispatcher fan-in

- Status: accepted
- Date: 2026-09-05

## Context

Before Phase 9, the pipeline selected exactly one Synthetic or CTP producer for one SPSC ingress queue. Supporting concurrent providers by sharing that queue would violate its single-producer contract; replacing it with a locked MPSC queue would add contention to the latency-sensitive CTP callback.

## Decision

Allocate one bounded SPSC queue and producer identity per realtime provider. Register every queue with the existing Dispatcher, which remains the sole consumer and visits ingress queues round-robin. Providers publish canonical events through `IMarketEventSink`; downstream persistence and live queues stay shared.

## Consequences

- Callback paths remain non-blocking and do not contend with other providers.
- Queue capacity and loss/failure metrics remain attributable to a provider boundary.
- Shared persistence, delivery, API, and archive semantics avoid provider-specific pipeline copies.
- Fairness is simple round-robin rather than priority or quality arbitration.
- Queue registration is fixed before Dispatcher startup.
- Automatic failover and provider arbitration remain future work.
