from __future__ import annotations

import asyncio
import importlib
import random
import time
from collections.abc import Callable
from typing import Any

from .errors import PermanentProviderError, RateLimitError, TransientProviderError
from .metrics import ProviderMetrics
from .registry import EndpointDefinition


class AkshareClient:
    """Rate-limited, retry-bounded seam around the optional AKShare package."""

    def __init__(self, *, max_concurrency: int = 2, min_interval_ms: int = 500,
                 max_attempts: int = 3, module: Any | None = None,
                 sleep: Callable[[float], Any] = asyncio.sleep):
        if max_concurrency < 1 or min_interval_ms < 0 or max_attempts < 1:
            raise ValueError("invalid AKShare client limits")
        self._module = module
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._interval = min_interval_ms / 1000
        self._attempts = max_attempts
        self._sleep = sleep
        self._pace_lock = asyncio.Lock()
        self._last_request = 0.0
        self.metrics = ProviderMetrics()

    @property
    def version(self) -> str:
        module = self._load(); return str(getattr(module, "__version__", "unknown"))

    def _load(self):
        if self._module is None:
            try: self._module = importlib.import_module("akshare")
            except ImportError as exc:
                raise RuntimeError("AKShare provider enabled but akshare is not installed; install python/requirements-akshare.txt") from exc
        return self._module

    async def _pace(self) -> None:
        async with self._pace_lock:
            delay = self._interval - (time.monotonic() - self._last_request)
            if delay > 0: await self._sleep(delay)
            self._last_request = time.monotonic()

    @staticmethod
    def _transient(error: Exception) -> bool:
        return isinstance(error, (TimeoutError, ConnectionError, RateLimitError, TransientProviderError))

    async def call(self, definition: EndpointDefinition, **parameters: Any) -> list[dict[str, Any]]:
        module = self._load()
        function = getattr(module, definition.function_name, None)
        if function is None: raise PermanentProviderError(f"AKShare {self.version} lacks {definition.function_name}")
        async with self._semaphore:
            for attempt in range(1, self._attempts + 1):
                await self._pace(); started = time.monotonic(); self.metrics.requests_total += 1
                try:
                    frame = await asyncio.to_thread(function, **parameters)
                    records = frame.to_dict(orient="records") if hasattr(frame, "to_dict") else list(frame)
                    self.metrics.success(time.monotonic() - started)
                    return records
                except Exception as exc:
                    self.metrics.requests_failed_total += 1
                    if not self._transient(exc): raise PermanentProviderError(str(exc)) from exc
                    if attempt == self._attempts: raise TransientProviderError(str(exc)) from exc
                    self.metrics.retries_total += 1
                    await self._sleep(min(30.0, 0.25 * (2 ** (attempt - 1))) + random.uniform(0, 0.1))
        raise AssertionError("unreachable")
