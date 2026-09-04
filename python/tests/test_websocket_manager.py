import asyncio
import pytest
from api.models import SubscriptionRequest
from api.websocket_manager import LatestPerSymbolBuffer,WebSocketManager
from conftest import make_tick

class FakeWebSocket:
    def __init__(self):self.accepted=False;self.sent=[];self.gate=asyncio.Event();self.closed=False
    async def accept(self):self.accepted=True
    async def send_json(self,value):await self.gate.wait();self.sent.append(value)
    async def close(self,code=1000):self.closed=True

def test_subscription_validation():
    assert SubscriptionRequest(protocol_version=1,action="subscribe",symbols=["SHFE.zn2610"]).symbols==["SHFE.zn2610"]
    assert SubscriptionRequest(protocol_version=1,action="unsubscribe",symbols=["SHFE.zn2610"]).action=="unsubscribe"
    for value in ({"protocol_version":1,"action":"destroy_database","symbols":[]},{"protocol_version":1,"action":"subscribe","symbols":["bad"]},{"protocol_version":2,"action":"subscribe","symbols":["SHFE.zn2610"]}):
        with pytest.raises(Exception):SubscriptionRequest.model_validate(value)

@pytest.mark.asyncio
async def test_latest_per_symbol_is_bounded_and_coalesces():
    buffer=LatestPerSymbolBuffer(2);await buffer.put("SHFE.zn2610",{"seq":1});await buffer.put("SHFE.cu2610",{"seq":1});await buffer.put("SHFE.zn2610",{"seq":2})
    assert len(buffer)==2 and buffer.dropped==1
    values=[await buffer.get(),await buffer.get()];assert {v["seq"] for v in values}=={1,2}

@pytest.mark.asyncio
async def test_manager_routes_independently_and_cleans_up():
    manager=WebSocketManager(1);slow=FakeWebSocket();fast=FakeWebSocket();fast.gate.set()
    slow_client=await manager.connect(slow);fast_client=await manager.connect(fast)
    await manager.subscribe(slow_client,["SHFE.zn2610"]);await manager.subscribe(fast_client,["SHFE.cu2610"])
    await manager.publish(make_tick(seq=1));await manager.publish(make_tick(seq=2));await manager.publish(make_tick("SHFE.cu2610",seq=3));await asyncio.sleep(.01)
    assert fast.sent[-1]["symbol"]=="SHFE.cu2610" and manager.metrics.websocket_clients==2 and len(slow_client.buffer)<=1
    slow.gate.set();await manager.shutdown();assert manager.metrics.websocket_clients==0
