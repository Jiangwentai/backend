from __future__ import annotations
from datetime import datetime,timezone
import math
import re
from typing import Literal
from pydantic import BaseModel,Field,field_validator,model_validator

SYMBOL_RE=re.compile(r"^[A-Z][A-Z0-9_]{1,15}\.[A-Za-z0-9][A-Za-z0-9_-]{0,31}$")

def validate_symbol(value:str)->str:
    if not SYMBOL_RE.fullmatch(value):raise ValueError("symbol must use EXCHANGE.instrument form")
    return value

def _timestamp(value:int,scale:int)->str:
    seconds=value/scale
    dt=datetime.fromtimestamp(seconds,tz=timezone.utc)
    fraction=value%scale
    digits=6 if scale==1_000_000 else 9
    return f"{dt:%Y-%m-%dT%H:%M:%S}.{fraction:0{digits}d}Z"

def _day(value:str)->str:
    return f"{value[:4]}-{value[4:6]}-{value[6:8]}" if len(value)==8 and value.isdigit() else value

class BookLevel(BaseModel):
    price:float
    volume:int

class QuoteResponse(BaseModel):
    schema_version:int=2
    provider:str
    event_type:str
    instrument_id:str
    quality:str
    symbol:str
    exchange:str
    instrument:str
    event_ts:str
    recv_ts:str
    producer_id:str
    seq:int
    trading_day:str
    action_day:str
    last_price:float|None
    volume:int
    turnover:float
    open_interest:float
    upper_limit_price:float|None=None
    lower_limit_price:float|None=None
    bid:list[BookLevel]
    ask:list[BookLevel]
    stale:bool|None=None
    age_seconds:float|None=None
    timestamp_source:str|None=None
    selection_reason:str|None=None
    fallback:bool=False
    preferred_provider:str|None=None

def quote_from_tick(tick:dict)->QuoteResponse:
    def price(value:float|None)->float|None:return value if value is not None and math.isfinite(value) else None
    bids=[BookLevel(price=p,volume=v) for p,v in zip(tick["bid_price"],tick["bid_volume"]) if price(p) is not None]
    asks=[BookLevel(price=p,volume=v) for p,v in zip(tick["ask_price"],tick["ask_volume"]) if price(p) is not None]
    return QuoteResponse(provider=tick.get("provider","ctp"),event_type=tick.get("event_type","quote_snapshot"),instrument_id=tick.get("instrument_id",f'{tick["exchange"]}.{tick["instrument"]}'),quality=tick.get("quality","UNKNOWN"),symbol=f'{tick["exchange"]}.{tick["instrument"]}',exchange=tick["exchange"],instrument=tick["instrument"],event_ts=_timestamp(tick["event_ts"],1_000_000),recv_ts=_timestamp(tick["recv_ts"],1_000_000_000),producer_id=tick["producer_id"],seq=tick["seq"],trading_day=_day(tick["trading_day"]),action_day=_day(tick["action_day"]),last_price=price(tick["last_price"]),volume=tick["volume"] or 0,turnover=tick["turnover"] or 0,open_interest=tick["open_interest"] or 0,upper_limit_price=price(tick["upper_limit_price"]),lower_limit_price=price(tick["lower_limit_price"]),bid=bids,ask=asks,stale=tick.get("stale"),age_seconds=tick.get("age_seconds"),timestamp_source=tick.get("timestamp_source"),selection_reason=tick.get("selection_reason"),fallback=tick.get("fallback",False),preferred_provider=tick.get("preferred_provider"))

class SubscriptionRequest(BaseModel):
    protocol_version:Literal[1]
    action:Literal["subscribe","unsubscribe","subscribe_all"]
    symbols:list[str]=Field(default_factory=list,max_length=1000)
    @field_validator("symbols")
    @classmethod
    def symbols_valid(cls,values:list[str])->list[str]:return list(dict.fromkeys(validate_symbol(v) for v in values))
    @model_validator(mode="after")
    def required_symbols(self):
        if self.action in {"subscribe","unsubscribe"} and not self.symbols:raise ValueError("symbols are required")
        if self.action=="subscribe_all" and self.symbols:raise ValueError("subscribe_all does not accept symbols")
        return self

class HealthResponse(BaseModel):
    status:Literal["HEALTHY","DEGRADED","UNHEALTHY"]
    components:dict[str,Literal["HEALTHY","DEGRADED","UNHEALTHY"]]
    websocket_clients:int
    last_live_message_time:str|None
    providers:dict[str,Literal["STOPPED","STARTING","CONNECTING","AUTHENTICATING","READY","DEGRADED","RECONNECTING","ERROR","STOPPING"]]=Field(default_factory=dict)

class InstrumentResponse(BaseModel):
    symbol:str
    exchange:str
    instrument:str
    product:str
    product_name:str
    delivery_month:str|None
    listed_date:str|None
    last_trading_date:str|None
    status:Literal["prelisted","active","expired","suspended"]
    contract_multiplier:float|None
    price_tick:float|None
    currency:str

class BarResponse(BaseModel):
    exchange:str
    instrument:str
    trading_day:str
    interval:Literal["1m","5m","1h","1d"]
    bar_start:datetime
    open:float
    high:float
    low:float
    close:float
    volume:int
    open_interest:float|None
    snapshot_count:int|None=None
    provider:str|None=None
    source:str|None=None
    upstream_source:str|None=None
    settlement:float|None=None
