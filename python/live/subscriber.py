from __future__ import annotations
import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable
import zmq
import zmq.asyncio
from .cache import LatestQuoteCache
from .protocol import decode_tick, expected_topic
TickCallback = Callable[[dict], Awaitable[None] | None]

class LiveSubscriber:
    def __init__(self,endpoint:str,cache:LatestQuoteCache,on_tick:TickCallback|None=None):
        self.endpoint=endpoint;self.cache=cache;self.on_tick=on_tick
        self.received_total=0;self.cache_updates_total=0;self.decode_failures_total=0;self.last_message_monotonic:float|None=None
        self._stop=asyncio.Event();self.ready=asyncio.Event();self.healthy=False
    async def run(self):
        context=zmq.asyncio.Context();socket=context.socket(zmq.SUB);socket.setsockopt(zmq.SUBSCRIBE,b"");socket.connect(self.endpoint)
        self.healthy=True;self.ready.set()
        try:
            while not self._stop.is_set():
                try:topic,payload=await asyncio.wait_for(socket.recv_multipart(),.1)
                except asyncio.TimeoutError:continue
                try:
                    tick=decode_tick(payload)
                    if topic.decode()!=expected_topic(tick):raise ValueError("topic does not match payload")
                    updated=await self.cache.update(tick);self.received_total+=1;self.last_message_monotonic=time.monotonic()
                    if updated:self.cache_updates_total+=1
                    if updated and self.on_tick:
                        result=self.on_tick(tick)
                        if inspect.isawaitable(result):await result
                    # A hot SUB socket can remain continuously readable. Yield so
                    # per-client sender tasks and HTTP handlers stay schedulable.
                    await asyncio.sleep(0)
                # Isolate malformed and forward-incompatible frames rather than
                # terminating the long-running subscriber task.
                except Exception:self.decode_failures_total+=1
        finally:self.healthy=False;self.ready.clear();socket.close(0);context.term()
    def stop(self):self._stop.set()
