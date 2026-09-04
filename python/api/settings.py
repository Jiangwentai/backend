from __future__ import annotations
from dataclasses import dataclass
import os

@dataclass(frozen=True)
class Settings:
    zmq_endpoint: str = "tcp://127.0.0.1:5556"
    questdb_http_url: str = "http://127.0.0.1:9000"
    websocket_queue_capacity: int = 128
    live_stale_after_seconds: float = 5.0
    recovery_timeout_seconds: float = 10.0

    @classmethod
    def from_env(cls) -> "Settings":
        value=cls(
            zmq_endpoint=os.getenv("ZMQ_SUB_ENDPOINT",os.getenv("ZMQ_PUB_ENDPOINT","tcp://127.0.0.1:5556")),
            questdb_http_url=os.getenv("QDB_HTTP_URL","http://127.0.0.1:9000"),
            websocket_queue_capacity=int(os.getenv("WEBSOCKET_QUEUE_CAPACITY","128")),
            live_stale_after_seconds=float(os.getenv("LIVE_STALE_AFTER_SECONDS","5")),
            recovery_timeout_seconds=float(os.getenv("RECOVERY_TIMEOUT_SECONDS","10")),
        )
        if value.websocket_queue_capacity<1:raise ValueError("WEBSOCKET_QUEUE_CAPACITY must be positive")
        return value
