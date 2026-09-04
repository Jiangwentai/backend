# Architecture through Phase 3

The C++ process owns ingestion, dispatch, QuestDB QWP persistence, and ZeroMQ publication. The Python process owns one ZeroMQ SUB socket, the process-local latest-quote cache, REST routes, and WebSocket connections. No WebSocket or REST operation runs on the persistence path.

```text
Synthetic -> Ingress -> Dispatcher -> PersistenceQueue -> QWP/SF -> QuestDB
                                `----> LiveQueue -> ZMQ PUB -> ZMQ SUB
                                                               |
                                                       LatestQuoteCache
                                                          /          \
                                                       REST       WS Manager
```

FastAPI uses lifespan ownership. Startup creates cache and manager, starts SUB first, waits until its socket is initialized, then loads one latest QuestDB row per `(exchange, instrument)`. Live frames received during the query are already placed in the cache. Recovery rows pass through the same conflict resolver: same-producer higher `seq` wins; across producers timestamps decide. Only then is the API ready. Failure to recover leaves the service available but degraded.

Shutdown rejects new WebSocket clients, stops and closes SUB, closes all WebSockets and sender tasks, then closes the QuestDB HTTP client.

V1 must run exactly one Uvicorn worker because cache, subscriptions, and metrics are process-local. Multi-worker synchronization is intentionally deferred; no Redis or broker is introduced.
