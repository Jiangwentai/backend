from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from .models import ProviderHealth, ProviderHealthState, ProviderId


@dataclass
class HealthTracker:
    last_success: datetime | None = None
    last_error: str | None = None
    consecutive_failures: int = 0

    def success(self) -> None:
        self.last_success = datetime.now(timezone.utc); self.last_error = None; self.consecutive_failures = 0

    def failure(self, error: Exception) -> None:
        self.last_error = f"{type(error).__name__}: {error}"; self.consecutive_failures += 1

    def snapshot(self) -> ProviderHealth:
        state = ProviderHealthState.AVAILABLE if self.last_success and not self.consecutive_failures else ProviderHealthState.DEGRADED
        if self.consecutive_failures >= 3 and self.last_success is None: state = ProviderHealthState.UNAVAILABLE
        return ProviderHealth(ProviderId.AKSHARE, state, self.last_success, self.last_error, self.consecutive_failures)
