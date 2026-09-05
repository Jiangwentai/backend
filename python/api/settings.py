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
    postgres_dsn: str = "postgresql://market_data:dev-only-password@127.0.0.1:5432/market_data"
    postgres_timeout_seconds: float = 5.0
    archive_root: str = "data/market"
    akshare_zmq_endpoint: str = ""
    akshare_quote_stale_after_seconds: float = 15.0
    provider_selection_mode: str = "explicit"
    provider_preference: tuple[str,...] = ("ctp","ibkr","synthetic","akshare")
    provider_fallback_enabled: bool = False
    provider_allow_stale: bool = False
    provider_discrepancy_bps: float = 20.0

    @property
    def zmq_endpoints(self) -> list[str]:
        return [value for value in (self.zmq_endpoint,self.akshare_zmq_endpoint) if value]

    @classmethod
    def from_env(cls) -> "Settings":
        value=cls(
            zmq_endpoint=os.getenv("ZMQ_SUB_ENDPOINT",os.getenv("ZMQ_PUB_ENDPOINT","tcp://127.0.0.1:5556")),
            questdb_http_url=os.getenv("QDB_HTTP_URL","http://127.0.0.1:9000"),
            websocket_queue_capacity=int(os.getenv("WEBSOCKET_QUEUE_CAPACITY","128")),
            live_stale_after_seconds=float(os.getenv("LIVE_STALE_AFTER_SECONDS","5")),
            recovery_timeout_seconds=float(os.getenv("RECOVERY_TIMEOUT_SECONDS","10")),
            postgres_dsn=os.getenv("POSTGRES_DSN","postgresql://market_data:dev-only-password@127.0.0.1:5432/market_data"),
            postgres_timeout_seconds=float(os.getenv("POSTGRES_TIMEOUT_SECONDS","5")),
            archive_root=os.getenv("ARCHIVE_ROOT","data/market"),
            akshare_zmq_endpoint=os.getenv("AKSHARE_ZMQ_SUB_ENDPOINT",""),
            akshare_quote_stale_after_seconds=float(os.getenv("AKSHARE_QUOTE_STALE_AFTER_SECONDS","15")),
            provider_selection_mode=os.getenv("PROVIDER_SELECTION_MODE","explicit").lower(),
            provider_preference=tuple(value.strip().lower() for value in os.getenv("PROVIDER_PREFERENCE","ctp,ibkr,synthetic,akshare").split(",") if value.strip()),
            provider_fallback_enabled=os.getenv("PROVIDER_FALLBACK_ENABLED","false").lower() in {"1","true","yes"},
            provider_allow_stale=os.getenv("PROVIDER_ALLOW_STALE","false").lower() in {"1","true","yes"},
            provider_discrepancy_bps=float(os.getenv("PROVIDER_DISCREPANCY_BPS","20")),
        )
        if value.websocket_queue_capacity<1:raise ValueError("WEBSOCKET_QUEUE_CAPACITY must be positive")
        if value.postgres_timeout_seconds<=0:raise ValueError("POSTGRES_TIMEOUT_SECONDS must be positive")
        if value.provider_selection_mode not in {"explicit","preferred","ranked"}:raise ValueError("invalid PROVIDER_SELECTION_MODE")
        if not value.provider_preference:raise ValueError("PROVIDER_PREFERENCE must not be empty")
        return value
