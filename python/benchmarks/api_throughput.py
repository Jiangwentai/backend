from __future__ import annotations
import asyncio
import os
import re
import subprocess
import time
import tracemalloc
from live.cache import LatestQuoteCache
from live.subscriber import LiveSubscriber
from api.websocket_manager import WebSocketManager

class SlowSocket:
    async def accept(self):pass
    async def send_json(self,value):await asyncio.sleep(.01)
    async def close(self,code=1000):pass

async def main():
    executable=os.getenv("COMBINED_FIXTURE","./build/dev/combined_throughput");endpoint=os.getenv("ZMQ_BENCH_ENDPOINT","tcp://127.0.0.1:15560")
    cache=LatestQuoteCache();manager=WebSocketManager(64);client=await manager.connect(SlowSocket());await manager.set_subscribe_all(client)
    subscriber=LiveSubscriber(endpoint,cache,manager.publish);task=asyncio.create_task(subscriber.run());await subscriber.ready.wait();tracemalloc.start();started=time.monotonic()
    environment=os.environ|{"ZMQ_PUB_ENDPOINT":endpoint};process=await asyncio.create_subprocess_exec(executable,env=environment,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    output=(await process.communicate())[0].decode();await asyncio.sleep(.1);subscriber.stop();await task;current,peak=tracemalloc.get_traced_memory();await manager.shutdown();elapsed=time.monotonic()-started
    metrics=manager.metrics
    counters={name:int(value) for name,value in re.findall(r"(generated|questdb_accepted|live_sent)=(\d+)",output)};window=3.0
    print(output.strip());print(f"ingress_rate={counters.get('generated',0)/window:.1f} questdb_rate={counters.get('questdb_accepted',0)/window:.1f} zeromq_pub_rate={counters.get('live_sent',0)/window:.1f} zeromq_sub_rate={subscriber.received_total/window:.1f} cache_update_rate={subscriber.cache_updates_total/window:.1f} subscriber_received={subscriber.received_total} cache_updates={subscriber.cache_updates_total} cache_symbols={len(await cache.snapshot())} elapsed={elapsed:.3f} websocket_sent_rate={metrics.websocket_messages_sent_total/window:.1f} websocket_sent={metrics.websocket_messages_sent_total} websocket_dropped={metrics.websocket_dropped_updates_total} websocket_slow={metrics.websocket_slow_clients_total} memory_current_bytes={current} memory_peak_bytes={peak}")
    raise SystemExit(process.returncode)

if __name__=="__main__":asyncio.run(main())
