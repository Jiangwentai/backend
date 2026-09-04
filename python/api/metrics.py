from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
import time


@dataclass
class ApiMetrics:
    started_monotonic: float = field(default_factory=time.monotonic)
    requests: dict[tuple[str, str, int], int] = field(default_factory=lambda: defaultdict(int))
    duration_seconds_sum: float = 0.0
    duration_seconds_count: int = 0

    def observe(self, method: str, path: str, status: int, duration: float) -> None:
        self.requests[(method, path, status)] += 1
        self.duration_seconds_sum += duration
        self.duration_seconds_count += 1


def _metric(name: str, value: int | float, labels: str = "") -> str:
    return f"{name}{labels} {value}\n"


def render_metrics(app) -> str:
    api: ApiMetrics = app.state.api_metrics
    subscriber = app.state.subscriber
    websocket = app.state.websocket_manager.metrics
    output = [
        "# HELP market_data_api_uptime_seconds API process uptime.\n",
        "# TYPE market_data_api_uptime_seconds gauge\n",
        _metric("market_data_api_uptime_seconds", f"{time.monotonic()-api.started_monotonic:.6f}"),
        "# HELP market_data_api_requests_total HTTP requests by route and status.\n",
        "# TYPE market_data_api_requests_total counter\n",
    ]
    for (method, path, status), count in sorted(api.requests.items()):
        labels = f'{{method="{method}",path="{path}",status="{status}"}}'
        output.append(_metric("market_data_api_requests_total", count, labels))
    output.extend([
        "# TYPE market_data_api_request_duration_seconds_sum counter\n",
        _metric("market_data_api_request_duration_seconds_sum", f"{api.duration_seconds_sum:.9f}"),
        "# TYPE market_data_api_request_duration_seconds_count counter\n",
        _metric("market_data_api_request_duration_seconds_count", api.duration_seconds_count),
        "# TYPE market_data_live_received_total counter\n",
        _metric("market_data_live_received_total", subscriber.received_total),
        "# TYPE market_data_live_cache_updates_total counter\n",
        _metric("market_data_live_cache_updates_total", subscriber.cache_updates_total),
        "# TYPE market_data_live_decode_failures_total counter\n",
        _metric("market_data_live_decode_failures_total", subscriber.decode_failures_total),
        "# TYPE market_data_live_subscriber_healthy gauge\n",
        _metric("market_data_live_subscriber_healthy", int(subscriber.healthy)),
        "# TYPE market_data_websocket_clients gauge\n",
        _metric("market_data_websocket_clients", websocket.websocket_clients),
        "# TYPE market_data_websocket_subscriptions gauge\n",
        _metric("market_data_websocket_subscriptions", websocket.websocket_subscriptions),
        "# TYPE market_data_websocket_messages_sent_total counter\n",
        _metric("market_data_websocket_messages_sent_total", websocket.websocket_messages_sent_total),
        "# TYPE market_data_websocket_send_failures_total counter\n",
        _metric("market_data_websocket_send_failures_total", websocket.websocket_send_failures_total),
        "# TYPE market_data_websocket_dropped_updates_total counter\n",
        _metric("market_data_websocket_dropped_updates_total", websocket.websocket_dropped_updates_total),
        "# TYPE market_data_component_healthy gauge\n",
    ])
    states = {
        "api": app.state.ready,
        "questdb": app.state.questdb_healthy,
        "postgres": app.state.postgres_healthy,
    }
    for component, healthy in states.items():
        output.append(_metric("market_data_component_healthy", int(healthy is True), f'{{component="{component}"}}'))
    return "".join(output)
