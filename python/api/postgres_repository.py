from __future__ import annotations
from typing import Any
import asyncpg

class PostgresMetadataRepository:
    def __init__(self,dsn:str,timeout:float=5.0):self._dsn=dsn;self._timeout=timeout;self._pool:asyncpg.Pool|None=None

    async def start(self)->None:
        if self._pool is None:self._pool=await asyncpg.create_pool(self._dsn,min_size=1,max_size=5,command_timeout=self._timeout,timeout=self._timeout)

    async def list_instruments(self,*,exchange:str|None=None,product:str|None=None,active_only:bool=True,limit:int=500,offset:int=0)->list[dict[str,Any]]:
        if self._pool is None:raise RuntimeError("PostgreSQL metadata repository is not connected")
        rows=await self._pool.fetch("""
            SELECT c.exchange_code || '.' || c.instrument_id AS symbol,
                   c.exchange_code AS exchange, c.instrument_id AS instrument,
                   c.product_code AS product, p.name AS product_name,
                   c.delivery_month, c.listed_date, c.last_trading_date, c.status,
                   p.contract_multiplier, p.price_tick, p.currency
            FROM futures_contracts c
            JOIN products p ON p.exchange_code=c.exchange_code AND p.code=c.product_code
            WHERE ($1::text IS NULL OR c.exchange_code=$1)
              AND ($2::text IS NULL OR c.product_code=$2)
              AND (NOT $3::boolean OR c.status='active')
            ORDER BY c.exchange_code,c.instrument_id
            LIMIT $4 OFFSET $5
        """,exchange,product,active_only,limit,offset)
        result=[]
        for row in rows:
            item=dict(row)
            for key in ("listed_date","last_trading_date"):
                if item[key] is not None:item[key]=item[key].isoformat()
            for key in ("contract_multiplier","price_tick"):
                if item[key] is not None:item[key]=float(item[key])
            result.append(item)
        return result

    async def close(self)->None:
        if self._pool is not None:await self._pool.close();self._pool=None

    async def provider_health(self,provider:str)->dict[str,Any]:
        if self._pool is None:raise RuntimeError("PostgreSQL metadata repository is not connected")
        row=await self._pool.fetchrow("""WITH recent AS (
          SELECT status,completed_at,error_code,error_message FROM provider_ingestion_runs
          WHERE provider_code=$1 ORDER BY started_at DESC LIMIT 10)
          SELECT (SELECT status FROM recent LIMIT 1) last_status,
                 (SELECT completed_at FROM recent WHERE status='SUCCESS' ORDER BY completed_at DESC LIMIT 1) last_success,
                 count(*) FILTER(WHERE status='FAILED') failures,
                 (SELECT error_code FROM recent LIMIT 1) error_code,
                 (SELECT error_message FROM recent LIMIT 1) error_message FROM recent""",provider)
        failures=int(row["failures"] or 0);last_status=row["last_status"]
        state="AVAILABLE" if last_status=="SUCCESS" else "UNAVAILABLE" if failures>=3 and row["last_success"] is None else "DEGRADED"
        return {"provider":provider.upper(),"state":state,"last_success":row["last_success"],
                "recent_failures":failures,"error_code":row["error_code"],"error_message":row["error_message"],
                "capabilities":{"historical_bars":True,"intraday_bars":True,"reference_data":True,
                                "best_effort_quotes":True,"realtime_quotes":False,"market_depth":False,"trade_ticks":False}}
