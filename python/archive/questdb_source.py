from __future__ import annotations
from datetime import datetime,timezone
import re
import httpx
from .schema import COLUMN_NAMES

CODE=re.compile(r"^[A-Za-z0-9_-]{1,32}$")
DAY=re.compile(r"^[0-9]{8}$")

def _literal(value:str)->str:
    if not CODE.fullmatch(value):raise ValueError("invalid archive partition identifier")
    return "'"+value+"'"

def _epoch(value:str|int,scale:int)->int:
    if isinstance(value,int):return value
    normalized=value.removesuffix("Z");whole,separator,fraction=normalized.partition(".");dt=datetime.fromisoformat(whole)
    if dt.tzinfo is None:dt=dt.replace(tzinfo=timezone.utc)
    digits=6 if scale==1_000_000 else 9;subsecond=int(fraction[:digits].ljust(digits,"0")) if separator else 0
    return int(dt.timestamp())*scale+subsecond

class QuestDBArchiveSource:
    def __init__(self,base_url:str,timeout:float=60,batch_size:int=10000,client:httpx.Client|None=None):self._client=client or httpx.Client(base_url=base_url,timeout=timeout);self._batch_size=batch_size
    def close(self):self._client.close()
    def iter_partition(self,exchange:str,instrument:str,trading_day:str):
        if not DAY.fullmatch(trading_day):raise ValueError("trading_day must use YYYYMMDD")
        base=f"SELECT {','.join(COLUMN_NAMES)} FROM ctp_market_data WHERE exchange={_literal(exchange)} AND instrument={_literal(instrument)} AND trading_day='{trading_day}' ORDER BY event_ts,producer_id,seq";offset=0
        while True:
            query=f"{base} LIMIT {offset},{offset+self._batch_size}";response=self._client.get("/api/v1/sql/execute",params={"query":query});response.raise_for_status();payload=response.json();names=[column["name"] for column in payload["columns"]];dataset=payload.get("dataset",[])
            rows=[]
            for raw in dataset:
                row=dict(zip(names,raw));row["event_ts"]=_epoch(row["event_ts"],1_000_000);row["recv_ts"]=_epoch(row["recv_ts"],1_000_000_000);rows.append(row)
            if rows:yield rows
            if len(dataset)<self._batch_size:break
            offset+=self._batch_size
