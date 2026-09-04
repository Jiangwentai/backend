import asyncio,os,pytest
from live.cache import LatestQuoteCache
from live.subscriber import LiveSubscriber
@pytest.mark.asyncio
async def test_cpp_pub_to_python_cache():
 fixture=os.environ.get("LIVE_FIXTURE")
 if not fixture:pytest.skip("LIVE_FIXTURE not set")
 endpoint="tcp://127.0.0.1:15556";cache=LatestQuoteCache();sub=LiveSubscriber(endpoint,cache);task=asyncio.create_task(sub.run());await asyncio.sleep(.1)
 proc=await asyncio.create_subprocess_exec(fixture,endpoint);await proc.wait();value=None
 for _ in range(50):
  value=await cache.lookup("SHFE","zn2610")
  if value:break
  await asyncio.sleep(.02)
 sub.stop();await task
 assert proc.returncode==0 and value and value["producer_id"] and value["seq"]>0
 assert value["event_ts"]>0 and value["recv_ts"]>0 and value["last_price"]>0
