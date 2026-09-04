# Delivery semantics

The QuestDB path remains durability-first QWP Store-and-Forward with at-least-once replay and `DEDUP UPSERT KEYS(event_ts, producer_id, seq)`. Transport duplicates collapse while distinct feed events retain distinct sequence values.

ZeroMQ and WebSocket delivery are best effort. ZeroMQ does not replay during API downtime; QuestDB is queried once at API startup to recover latest state, never polled as a live bus.

WebSocket clients receive nothing until they subscribe with protocol version 1:

```json
{"protocol_version":1,"action":"subscribe","symbols":["SHFE.zn2610"]}
```

`unsubscribe` has the same shape. `subscribe_all` is explicit and takes no symbols. Invalid symbols, actions, and versions produce a versioned `INVALID_REQUEST` response. A subscription immediately queues the cached snapshot when available.

Every connection has its own bounded latest-per-symbol buffer. A newer pending update for the same symbol replaces the old one. At capacity, the oldest pending symbol is evicted. Each connection has an independent sender task, so a slow or failed browser cannot block the ZeroMQ subscriber, cache, other clients, C++ pipeline, or persistence. Drops and slow-client incidents are observable in WebSocket metrics.
