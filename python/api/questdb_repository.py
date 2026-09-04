from __future__ import annotations
from datetime import datetime,timezone
import httpx

LATEST_QUOTES_SQL="""SELECT event_ts, recv_ts, provider, event_type, instrument_id, quality, producer_id, seq, exchange, instrument, trading_day, action_day, last_price, volume, turnover, open_interest, upper_limit_price, lower_limit_price, bid_price1, bid_volume1, ask_price1, ask_volume1, bid_price2, bid_volume2, ask_price2, ask_volume2, bid_price3, bid_volume3, ask_price3, ask_volume3, bid_price4, bid_volume4, ask_price4, ask_volume4, bid_price5, bid_volume5, ask_price5, ask_volume5 FROM ctp_market_data LATEST ON event_ts PARTITION BY provider, exchange, instrument"""

def _epoch(value:str|int,scale:int)->int:
    if isinstance(value,int):return value
    normalized=value.removesuffix("Z");whole,separator,fraction=normalized.partition(".")
    dt=datetime.fromisoformat(whole)
    if dt.tzinfo is None:dt=dt.replace(tzinfo=timezone.utc)
    digits=6 if scale==1_000_000 else 9
    subsecond=int((fraction[:digits]).ljust(digits,"0")) if separator else 0
    return int(dt.timestamp())*scale+subsecond

class QuestDBQuoteRepository:
    def __init__(self,base_url:str,timeout:float=10,client:httpx.AsyncClient|None=None):self._client=client or httpx.AsyncClient(base_url=base_url,timeout=timeout)
    async def close(self):await self._client.aclose()
    async def load_latest_quotes(self)->list[dict]:
        response=await self._client.get("/exec",params={"query":LATEST_QUOTES_SQL});response.raise_for_status();payload=response.json()
        names=[column["name"] for column in payload["columns"]]
        result=[]
        for row in payload.get("dataset",[]):
            value=dict(zip(names,row));tick={k:value.get(k) for k in ("provider","event_type","instrument_id","quality","producer_id","seq","exchange","instrument","trading_day","action_day","last_price","volume","turnover","open_interest","upper_limit_price","lower_limit_price")}
            tick["provider"]=tick["provider"] or "ctp";tick["event_type"]=tick["event_type"] or "quote_snapshot";tick["instrument_id"]=tick["instrument_id"] or f'{tick["exchange"]}.{tick["instrument"]}';tick["quality"]=tick["quality"] or "UNKNOWN"
            tick["schema_version"]=2;tick["event_ts"]=_epoch(value["event_ts"],1_000_000);tick["recv_ts"]=_epoch(value["recv_ts"],1_000_000_000)
            tick["bid_price"]=[value[f"bid_price{i}"] for i in range(1,6)];tick["bid_volume"]=[value[f"bid_volume{i}"] for i in range(1,6)]
            tick["ask_price"]=[value[f"ask_price{i}"] for i in range(1,6)];tick["ask_volume"]=[value[f"ask_volume{i}"] for i in range(1,6)];result.append(tick)
        return result
