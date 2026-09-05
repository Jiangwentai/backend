from __future__ import annotations

import msgpack
import zmq
import zmq.asyncio

from .protocol import expected_topic, validate_tick


class ZmqLivePublisher:
    def __init__(self, endpoint: str):
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.bind(endpoint)

    async def publish(self, event: dict) -> None:
        validate_tick(event)
        await self.socket.send_multipart([expected_topic(event).encode(), msgpack.packb(event, use_bin_type=True)])

    def close(self) -> None:
        self.socket.close(0)
        self.context.term()
