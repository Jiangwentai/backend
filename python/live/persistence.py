from __future__ import annotations

import math

import httpx


def _tag(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")


def _string(value: object) -> str:
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'


class QuestDbLivePersistence:
    """Provider-independent optional live-event persistence transport."""
    def __init__(self, base_url: str, table: str = "ctp_market_data", timeout: float = 10.0):
        self.client=httpx.AsyncClient(base_url=base_url.rstrip("/"),timeout=timeout);self.table=table

    async def publish(self,event:dict)->None:
        tags=",".join(f"{name}={_tag(event[name])}" for name in
            ("provider","event_type","instrument_id","quality","producer_id","exchange","instrument"))
        fields=[f"recv_ts={event['recv_ts']}t",f"seq={event['seq']}i",
            f"trading_day={_string(event['trading_day'])}",f"action_day={_string(event['action_day'])}"]
        for name in ("last_price","turnover","open_interest","upper_limit_price","lower_limit_price"):
            value=event.get(name)
            if value is not None and math.isfinite(value):fields.append(f"{name}={value}")
        if event.get("volume") is not None:fields.append(f"volume={int(event['volume'])}i")
        for side in ("bid","ask"):
            for level,(price,volume) in enumerate(zip(event[f"{side}_price"],event[f"{side}_volume"]),1):
                if price is not None and math.isfinite(price):fields.append(f"{side}_price{level}={price}")
                if volume is not None:fields.append(f"{side}_volume{level}={int(volume)}i")
        line=f"{self.table},{tags} {','.join(fields)} {event['event_ts']}t"
        response=await self.client.post("/write",params={"precision":"us"},content=line);response.raise_for_status()

    async def close(self)->None:await self.client.aclose()
