# Delivery semantics

The QuestDB path remains durability-first QWP Store-and-Forward with at-least-once replay and `DEDUP UPSERT KEYS(event_ts, provider, producer_id, seq)`. Transport duplicates collapse while distinct feed events and provider identities remain distinct.

ZeroMQ and WebSocket delivery are best effort. ZeroMQ does not replay during API downtime; QuestDB is queried once at API startup to recover latest state, never polled as a live bus.

WebSocket clients receive nothing until they subscribe with protocol version 1:

```json
{"protocol_version":1,"action":"subscribe","symbols":["SHFE.zn2610"]}
```

`unsubscribe` has the same shape. `subscribe_all` is explicit and takes no symbols. Invalid symbols, actions, and versions produce a versioned `INVALID_REQUEST` response. A subscription immediately queues the cached snapshot when available.

Every connection has its own bounded latest-per-provider-and-symbol buffer. A newer pending update for the same provider and symbol replaces the old one. At capacity, the oldest pending key is evicted. Each connection has an independent sender task, so a slow or failed browser cannot block the ZeroMQ subscriber, cache, other clients, C++ pipeline, or persistence. Drops and slow-client incidents are observable in WebSocket metrics.

The subscription control protocol remains version 1 for compatibility. Quote envelopes and MessagePack frames use schema version 2 and include `provider`, `event_type`, `instrument_id`, and `quality`; decoders continue to accept version 1 frames with CTP-compatible defaults.
# Historical and realtime identity

Historical bars are logically idempotent on `(provider, instrument_id, interval, bar_start)`. Re-fetches retain independent raw lineage, unchanged canonical values upsert safely, and changed values create revision records.

Realtime transport is at least once. A producer keeps one `producer_id` for its process and assigns monotonically increasing `seq`; a retry retains that identity. Two separate polls with identical market fields are still separate feed observations and receive separate sequence numbers. AKShare snapshot differences never create synthetic trades.

Provider arbitration is a read-side projection over the latest independent observations. It does not create a new market event and therefore does not receive a producer identity or enter persistence. Explicit queries always preserve the requested source. Configured failover is reported to the caller and counted; it never erases the unavailable or stale primary observation.
