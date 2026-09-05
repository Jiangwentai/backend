from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
import time
from instruments.metrics import ResolutionMetrics


@dataclass
class ProviderMetrics:
    instrument_resolution: ResolutionMetrics = field(default_factory=ResolutionMetrics)
    requests_total: int = 0
    requests_failed_total: int = 0
    request_latency_seconds: float = 0.0
    rows_received_total: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    rows_normalized_total: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    rows_rejected_total: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    schema_errors_total: int = 0
    mapping_errors_total: int = 0
    retries_total: int = 0
    ingestion_runs_total: int = 0
    ingestion_failures_total: int = 0
    last_success_timestamp: float = 0.0
    quote_rows_received_total: int = 0
    quote_requests_total: int = 0
    quote_events_emitted_total: int = 0
    quote_request_failures_total: int = 0
    quote_poll_duration_seconds: float = 0.0
    quote_last_success_timestamp: float = 0.0
    quote_active_subscriptions: int = 0
    quote_schema_errors_total: int = 0
    quote_empty_responses_total: int = 0
    quote_mapping_errors_total: int = 0
    historical_revisions_total: int = 0
    historical_rows_written_total: int = 0

    def success(self, duration: float) -> None:
        self.request_latency_seconds += duration; self.last_success_timestamp = time.time()
