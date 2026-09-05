from __future__ import annotations
import asyncio
from datetime import datetime,timezone
class LatestQuoteCache:
    def __init__(self,stale_after_by_provider:dict[str,float]|None=None): self._values: dict[tuple[str,str,str],dict]={}; self._lock=asyncio.Lock();self._stale={key.lower():value for key,value in (stale_after_by_provider or {}).items()}
    async def update(self,tick:dict)->bool:
        tick=dict(tick);tick.setdefault("provider","ctp");tick.setdefault("event_type","quote_snapshot");tick.setdefault("instrument_id",f'{tick["exchange"]}.{tick["instrument"]}');tick.setdefault("quality","UNKNOWN")
        key=(tick["provider"].lower(),tick["exchange"],tick["instrument"])
        async with self._lock:
            old=self._values.get(key)
            if old and old["producer_id"]==tick["producer_id"] and tick["seq"]<=old["seq"]: return False
            if old and old["producer_id"]!=tick["producer_id"] and (tick["event_ts"],tick["recv_ts"])<(old["event_ts"],old["recv_ts"]): return False
            self._values[key]=tick;return True
    async def lookup(self,exchange:str,instrument:str,provider:str|None=None):
        async with self._lock:
            if provider is not None:
                value=self._values.get((provider.lower(),exchange,instrument));return self._copy(value)
            matches=[value for (source,venue,symbol),value in self._values.items() if venue==exchange and symbol==instrument]
            return self._copy(matches[0]) if len(matches)==1 else None
    async def snapshot(self):
        async with self._lock:return {k:self._copy(v) for k,v in self._values.items()}
    async def provider_states(self):
        async with self._lock:return {provider:"READY" for provider,_,_ in self._values}
    async def candidates(self,exchange:str,instrument:str):
        async with self._lock:return [self._copy(value) for (provider,venue,symbol),value in self._values.items() if venue==exchange and symbol==instrument]
    def _copy(self,value):
        if value is None:return None
        result=dict(value);threshold=self._stale.get(str(result.get("provider","")).lower())
        if threshold is not None:
            recv=datetime.fromtimestamp(result["recv_ts"]/1_000_000_000,tz=timezone.utc)
            result["age_seconds"]=max(0.0,(datetime.now(timezone.utc)-recv).total_seconds())
            result["stale"]=result["age_seconds"]>threshold
        return result
