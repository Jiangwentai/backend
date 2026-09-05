from __future__ import annotations

from datetime import datetime

from ..normalizers import normalize_contract_reference, normalize_futures_daily
from ..normalizers.futures_daily import normalize_eastmoney_foreign_daily
from ..normalizers.futures_minute import normalize_futures_minute
from ..normalizers.quote import normalize_quotes
from ..normalizers.reference import normalize_foreign_product_reference
from ..registry import endpoint


class FuturesDailyAdapter:
    definition=endpoint("futures_daily_sina")
    def __init__(self,client):self.client=client
    async def fetch_native(self,symbol:str):return await self.client.call(self.definition,symbol=symbol)
    def normalize(self,rows,*,raw_symbol:str,exchange:str,instrument_id:str,fetch_id:str,fetched_at:datetime):
        return normalize_futures_daily(rows,definition=self.definition,raw_symbol=raw_symbol,
          exchange=exchange,instrument_id=instrument_id,fetch_id=fetch_id,fetched_at=fetched_at)


class FuturesForeignDailyAdapter(FuturesDailyAdapter):
    definition=endpoint("futures_foreign_daily_sina")


class FuturesForeignDailyEastmoneyAdapter(FuturesDailyAdapter):
    definition=endpoint("futures_foreign_daily_eastmoney")
    def normalize(self,rows,*,raw_symbol:str,exchange:str,instrument_id:str,fetch_id:str,fetched_at:datetime):
        return normalize_eastmoney_foreign_daily(rows,definition=self.definition,raw_symbol=raw_symbol,
          exchange=exchange,instrument_id=instrument_id,fetch_id=fetch_id,fetched_at=fetched_at)


class FuturesMinuteBarAdapter:
    definition=endpoint("futures_1m_sina")
    def __init__(self,client):self.client=client
    async def fetch_native(self,symbol:str):return await self.client.call(self.definition,symbol=symbol,period="1")
    def normalize(self,rows,*,raw_symbol:str,exchange:str,instrument_id:str,fetch_id:str,fetched_at:datetime):
        return normalize_futures_minute(rows,definition=self.definition,raw_symbol=raw_symbol,
          exchange=exchange,instrument_id=instrument_id,fetch_id=fetch_id,fetched_at=fetched_at)


class RealtimeQuoteAdapter:
    definition=endpoint("futures_realtime_quote")
    def __init__(self,client):self.client=client
    async def fetch_native(self,symbols:list[str],market:str="CF"):
        return await self.client.call(self.definition,symbol=",".join(symbols),market=market,adjust="0")
    def normalize(self,rows,subscriptions,*,recv_at:datetime):
        return normalize_quotes(rows,definition=self.definition,subscriptions=subscriptions,recv_at=recv_at)


class FuturesContractReferenceAdapter:
    def __init__(self,client,definition=None):self.client=client;self.definition=definition or endpoint("futures_contracts_qihuo")
    async def fetch_native(self,parameters):return await self.client.call(self.definition,**parameters)
    def normalize(self,rows,*,fetch_id:str,fetched_at:datetime):
        normalizer = normalize_foreign_product_reference if self.definition.name == "futures_foreign_products" else normalize_contract_reference
        return normalizer(rows,definition=self.definition,fetch_id=fetch_id,fetched_at=fetched_at)
