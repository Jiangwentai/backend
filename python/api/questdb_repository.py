from __future__ import annotations
from datetime import datetime,timezone
from .models import validate_symbol
import httpx

LATEST_QUOTES_SQL="""SELECT event_ts, recv_ts, provider, event_type, instrument_id, quality, provider_symbol, raw_provider_symbol, instrument_kind, source, upstream_source, producer_id, seq, exchange, instrument, trading_day, action_day, last_price, volume, turnover, open_interest, upper_limit_price, lower_limit_price, bid_price1, bid_volume1, ask_price1, ask_volume1, bid_price2, bid_volume2, ask_price2, ask_volume2, bid_price3, bid_volume3, ask_price3, ask_volume3, bid_price4, bid_volume4, ask_price4, ask_volume4, bid_price5, bid_volume5, ask_price5, ask_volume5 FROM ctp_market_data LATEST ON event_ts PARTITION BY provider, exchange, instrument"""

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
            value=dict(zip(names,row));tick={k:value.get(k) for k in ("provider","event_type","instrument_id","quality","provider_symbol","raw_provider_symbol","instrument_kind","source","upstream_source","producer_id","seq","exchange","instrument","trading_day","action_day","last_price","volume","turnover","open_interest","upper_limit_price","lower_limit_price")}
            tick["provider"]=tick["provider"] or "ctp";tick["event_type"]=tick["event_type"] or "quote_snapshot";tick["instrument_id"]=tick["instrument_id"] or f'{tick["exchange"]}.{tick["instrument"]}';tick["quality"]=tick["quality"] or "UNKNOWN"
            tick["schema_version"]=2;tick["event_ts"]=_epoch(value["event_ts"],1_000_000);tick["recv_ts"]=_epoch(value["recv_ts"],1_000_000_000)
            tick["bid_price"]=[value[f"bid_price{i}"] for i in range(1,6)];tick["bid_volume"]=[value[f"bid_volume{i}"] for i in range(1,6)]
            tick["ask_price"]=[value[f"ask_price{i}"] for i in range(1,6)];tick["ask_volume"]=[value[f"ask_volume{i}"] for i in range(1,6)];result.append(tick)
        return result

    async def load_historical_bars(self,exchange:str,instrument:str,interval:str,start_day:str|None=None,end_day:str|None=None,provider:str|None="akshare")->list[dict]:
        allowed=lambda value:all(character.isalnum() or character in "_-" for character in value)
        validate_symbol(f"{exchange}.{instrument}")
        if not all(allowed(value) for value in (exchange,interval) if value) or (provider and not allowed(provider)):raise ValueError("invalid historical bar query")
        clauses=[f"exchange='{exchange}'",f"instrument_id IN ('{exchange}.{instrument}','{instrument}')",f"interval='{interval}'"]
        if provider:clauses.insert(0,f"provider='{provider.upper()}'")
        if start_day:clauses.append(f"trading_day>='{start_day[:4]}-{start_day[4:6]}-{start_day[6:]}'")
        if end_day:clauses.append(f"trading_day<='{end_day[:4]}-{end_day[4:6]}-{end_day[6:]}'")
        query="SELECT exchange,instrument_id instrument,trading_day,interval,bar_start,open,high,low,close,volume,open_interest,settlement,provider,provider_symbol,raw_provider_symbol,instrument_kind,quality,source,upstream_source FROM historical_bars WHERE "+" AND ".join(clauses)+" ORDER BY bar_start,provider"
        response=await self._client.get("/exec",params={"query":query});response.raise_for_status();payload=response.json();names=[column["name"] for column in payload["columns"]]
        result = {}
        for row in payload.get("dataset", []):
            value = dict(zip(names, row))
            stored_id = value["instrument"]
            value["instrument"] = instrument
            value["instrument_id"] = f"{exchange}.{instrument}"
            key = (value["provider"], value["interval"], value["bar_start"])
            # Read compatibility for legacy local IDs. Canonical rows take
            # precedence when both spellings exist; stored rows are not mutated.
            if key not in result or stored_id == value["instrument_id"]:
                result[key] = value
        return list(result.values())
