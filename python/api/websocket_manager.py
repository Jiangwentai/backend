from __future__ import annotations
import asyncio
from collections import OrderedDict
from dataclasses import dataclass,field
from typing import Any
from fastapi import WebSocket
from .models import quote_from_tick

class LatestPerSymbolBuffer:
    def __init__(self,capacity:int):
        if capacity<1:raise ValueError("capacity must be positive")
        self.capacity=capacity;self._items:OrderedDict[str,dict]=OrderedDict();self._condition=asyncio.Condition();self.dropped=0
    async def put(self,symbol:str,message:dict)->bool:
        async with self._condition:
            dropped=False
            if symbol in self._items:
                del self._items[symbol];self.dropped+=1;dropped=True
            elif len(self._items)>=self.capacity:
                self._items.popitem(last=False);self.dropped+=1;dropped=True
            self._items[symbol]=message;self._condition.notify()
            return dropped
    async def get(self)->dict:
        async with self._condition:
            await self._condition.wait_for(lambda:bool(self._items))
            _,message=self._items.popitem(last=False);return message
    def __len__(self):return len(self._items)

@dataclass(eq=False)
class ClientConnection:
    websocket:WebSocket
    buffer:LatestPerSymbolBuffer
    subscriptions:set[str]=field(default_factory=set)
    subscribe_all:bool=False
    sender_task:asyncio.Task|None=None
    send_lock:asyncio.Lock=field(default_factory=asyncio.Lock)

@dataclass
class WebSocketMetrics:
    websocket_clients:int=0
    websocket_subscriptions:int=0
    websocket_messages_sent_total:int=0
    websocket_send_failures_total:int=0
    websocket_dropped_updates_total:int=0
    websocket_slow_clients_total:int=0

class WebSocketManager:
    def __init__(self,queue_capacity:int=128):
        self.queue_capacity=queue_capacity;self._clients:set[ClientConnection]=set();self._lock=asyncio.Lock();self.accepting=True;self.metrics=WebSocketMetrics()
    async def connect(self,websocket:WebSocket)->ClientConnection:
        if not self.accepting:raise RuntimeError("websocket subsystem is shutting down")
        await websocket.accept();client=ClientConnection(websocket,LatestPerSymbolBuffer(self.queue_capacity))
        async with self._lock:self._clients.add(client);self._refresh_gauges()
        client.sender_task=asyncio.create_task(self._sender(client));return client
    async def disconnect(self,client:ClientConnection):
        async with self._lock:
            existed=client in self._clients;self._clients.discard(client);self._refresh_gauges()
        task=client.sender_task
        if existed and task and task is not asyncio.current_task():task.cancel();await asyncio.gather(task,return_exceptions=True)
    async def subscribe(self,client:ClientConnection,symbols:list[str]):
        client.subscribe_all=False;client.subscriptions.update(symbols)
        async with self._lock:self._refresh_gauges()
    async def unsubscribe(self,client:ClientConnection,symbols:list[str]):
        client.subscriptions.difference_update(symbols)
        async with self._lock:self._refresh_gauges()
    async def set_subscribe_all(self,client:ClientConnection):client.subscribe_all=True
    async def send_control(self,client:ClientConnection,message:dict):
        async with client.send_lock:await client.websocket.send_json(message)
    async def enqueue(self,client:ClientConnection,tick:dict):
        symbol=f'{tick["exchange"]}.{tick["instrument"]}'
        # Keep coalescing cheap. API serialization happens only for the value
        # that the independent sender task actually transmits.
        dropped=await client.buffer.put(symbol,tick)
        if dropped:self.metrics.websocket_dropped_updates_total+=1;self.metrics.websocket_slow_clients_total+=1
    async def publish(self,tick:dict):
        symbol=f'{tick["exchange"]}.{tick["instrument"]}'
        async with self._lock:targets=[c for c in self._clients if c.subscribe_all or symbol in c.subscriptions]
        for client in targets:await self.enqueue(client,tick)
    async def shutdown(self):
        self.accepting=False
        async with self._lock:clients=list(self._clients)
        for client in clients:
            try:await client.websocket.close(code=1001)
            except Exception:pass
            await self.disconnect(client)
    async def _sender(self,client:ClientConnection):
        try:
            while True:
                tick=await client.buffer.get();quote=quote_from_tick(tick).model_dump()
                message={"type":"quote","schema_version":1,"symbol":quote["symbol"],"data":quote}
                async with client.send_lock:await client.websocket.send_json(message)
                self.metrics.websocket_messages_sent_total+=1
        except asyncio.CancelledError:raise
        except Exception:
            self.metrics.websocket_send_failures_total+=1;await self.disconnect(client)
    def _refresh_gauges(self):
        self.metrics.websocket_clients=len(self._clients)
        self.metrics.websocket_subscriptions=sum(len(c.subscriptions) for c in self._clients)
