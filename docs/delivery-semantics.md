# Delivery semantics

The QuestDB path remains durability-first QWP Store-and-Forward with at-least-once replay and `DEDUP UPSERT KEYS(event_ts, provider, producer_id, seq)`. Transport duplicates collapse while distinct feed events and provider identities remain distinct.

ZeroMQ and WebSocket delivery are best effort. ZeroMQ does not replay during API downtime; QuestDB is queried once at API startup to recover latest state, never polled as a live bus.

The C++ publisher submits topic and MessagePack body through cppzmq's multipart helper with non-blocking flags. A send exception ends the current publisher socket lifecycle; the implementation never starts a new logical message after an uncertain partial multipart operation. Standard PUB high-water-mark loss is still intentionally lossy and cannot be inferred reliably from a successful send return value.

The C++ PUB explicitly sets `SNDHWM` before bind, using `live.sndhwm` or the overriding `ZMQ_SNDHWM` environment variable. FastAPI sets `RCVHWM` before connecting any subscriber endpoint, using `ZMQ_RCVHWM`. Both default to 1000 and reject zero, negative, or out-of-range values (valid range 1..2147483647). These limits count pending messages per peer, not bytes, total lifetime sends, or topic/body frames independently. TCP and receiver buffering mean the sender's HWM is not an exact end-to-end backlog or drop threshold.

A slow subscriber can lose whole multipart messages while standard PUB sends return success. This does not require `dontwait` and does not reliably produce a would-block warning. `messages_sent_total` counts successful local socket submissions, and `send_failures_total` counts reported failures; neither measures HWM drops or subscriber delivery. Increasing HWM does not guarantee delivery and may delay fresh quotes behind older observations. The separate persistence path is unchanged. No `CONFLATE` option is used because this protocol requires multipart topic/body messages.

The `ZmqHwm.SlowSubscriberDropsWholeMessagesDespiteSuccessfulSendsAndRecovers` regression uses small in-process queues against the pinned C++ libzmq to verify successful sends with receiver loss, intact multipart boundaries, and delivery resumption after draining. It does not establish a production TCP capacity or GC-pause budget; those require representative deployment measurements.

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
