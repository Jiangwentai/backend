from __future__ import annotations
import asyncio
class LatestQuoteCache:
    def __init__(self): self._values: dict[tuple[str,str],dict]={}; self._lock=asyncio.Lock()
    async def update(self,tick:dict)->bool:
        key=(tick["exchange"],tick["instrument"])
        async with self._lock:
            old=self._values.get(key)
            if old and old["producer_id"]==tick["producer_id"] and tick["seq"]<=old["seq"]: return False
            if old and old["producer_id"]!=tick["producer_id"] and (tick["event_ts"],tick["recv_ts"])<(old["event_ts"],old["recv_ts"]): return False
            self._values[key]=dict(tick);return True
    async def lookup(self,exchange:str,instrument:str):
        async with self._lock:return dict(self._values[(exchange,instrument)]) if (exchange,instrument) in self._values else None
    async def snapshot(self):
        async with self._lock:return {k:dict(v) for k,v in self._values.items()}
