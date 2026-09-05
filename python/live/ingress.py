from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol


class LiveTransport(Protocol):
    async def publish(self, event: dict) -> None: ...


class LiveEventIngress:
    """Provider-independent fan-out boundary for normalized live events."""
    def __init__(self, transports: list[LiveTransport]):
        self.transports = transports
        self.accepting = True

    async def emit(self, event: dict) -> None:
        if not self.accepting:
            raise RuntimeError("live ingress is stopping")
        for transport in self.transports:
            await transport.publish(event)

    async def close(self) -> None:
        self.accepting = False
        for transport in self.transports:
            close = getattr(transport, "close", None)
            if close:
                result = close()
                if isinstance(result, Awaitable):
                    await result
