# Persistent development instructions

- Read `REQUIREMENTS.md`, `ARCHITECTURE.md`, and `TASKS.md` completely before changing code.
- Implement only the explicitly requested phase. Do not pre-implement later phases.
- Preserve the separation of Persistence Path and Live Path.
- Never perform database, network, filesystem, sleep, blocking-lock, or heavy logging work in a CTP market-data callback.
- Keep provider SDK structures inside their adapter; core modules consume only provider-neutral `MarketTick`.
- Preserve stable `producer_id`, monotonic global `seq`, QWP Store-and-Forward, and QuestDB DEDUP semantics.
- Preserve feed duplicates while deduplicating only transport replay by stable identity.
- Do not introduce Redis, Kafka, NATS, RabbitMQ, or other infrastructure without an explicit architectural requirement.
- Do not vendor proprietary CTP SDK files. CTP-enabled builds must use an operator-supplied SDK path; default CI builds must work without credentials or SDK.
- Verify uncertain library APIs against the pinned/installed version or supplied headers rather than memory.
- Run focused tests after each module and the full regression suite before completion.
- Keep README, CHANGELOG, architecture, delivery semantics, and task status synchronized with implemented behavior.
- Treat existing uncommitted/untracked files as user work; do not discard them.
- Keep provider selection as a read-side projection over independent cached observations. Default to explicit selection and disabled fallback; never merge, average, overwrite, or republish provider prices unless a future phase explicitly requires it.
- Keep C++ ZeroMQ live publication non-blocking and multipart-safe. Use the pinned cppzmq multipart API, and never reuse a socket after an exception leaves multipart state uncertain.
- Treat standard ZeroMQ PUB delivery as lossy: a successful send is not subscriber delivery and must not be reported as a durability guarantee.
