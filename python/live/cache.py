from __future__ import annotations
import asyncio
class LatestQuoteCache:
    def __init__(self): self._values: dict[tuple[str,str,str],dict]={}; self._lock=asyncio.Lock()
    async def update(self,tick:dict)->bool:
        tick=dict(tick);tick.setdefault("provider","ctp");tick.setdefault("event_type","quote_snapshot");tick.setdefault("instrument_id",f'{tick["exchange"]}.{tick["instrument"]}');tick.setdefault("quality","UNKNOWN")
        key=(tick["provider"],tick["exchange"],tick["instrument"])
        async with self._lock:
            old=self._values.get(key)
            if old and old["producer_id"]==tick["producer_id"] and tick["seq"]<=old["seq"]: return False
            if old and old["producer_id"]!=tick["producer_id"] and (tick["event_ts"],tick["recv_ts"])<(old["event_ts"],old["recv_ts"]): return False
            self._values[key]=tick;return True
    async def lookup(self,exchange:str,instrument:str,provider:str|None=None):
        async with self._lock:
            if provider is not None:
                value=self._values.get((provider,exchange,instrument));return dict(value) if value else None
            matches=[value for (source,venue,symbol),value in self._values.items() if venue==exchange and symbol==instrument]
            return dict(matches[0]) if len(matches)==1 else None
    async def snapshot(self):
        async with self._lock:return {k:dict(v) for k,v in self._values.items()}
    async def provider_states(self):
        async with self._lock:return {provider:"READY" for provider,_,_ in self._values}
