import os
from datetime import date
import pytest
from api.postgres_repository import PostgresMetadataRepository
from historical import AcquisitionMode,HistoricalEnsureRequest

pytestmark=pytest.mark.skipif(not os.getenv("POSTGRES_TEST_DSN"),reason="POSTGRES_TEST_DSN is not configured")

@pytest.mark.asyncio
async def test_postgres_metadata_repository_round_trip():
    import asyncpg
    dsn=os.environ["POSTGRES_TEST_DSN"]
    connection=await asyncpg.connect(dsn)
    try:
        await connection.execute("INSERT INTO exchanges(code,name) VALUES('TSTX','Test Exchange') ON CONFLICT(code) DO UPDATE SET name=EXCLUDED.name")
        await connection.execute("INSERT INTO products(exchange_code,code,name,contract_multiplier,price_tick) VALUES('TSTX','px','Product X',10,0.5) ON CONFLICT(exchange_code,code) DO UPDATE SET name=EXCLUDED.name")
        await connection.execute("INSERT INTO futures_contracts(exchange_code,instrument_id,product_code,delivery_month,status) VALUES('TSTX','px2612','px','202612','active') ON CONFLICT(exchange_code,instrument_id) DO UPDATE SET status=EXCLUDED.status")
        repository=PostgresMetadataRepository(dsn)
        await repository.start()
        rows=await repository.list_instruments(exchange="TSTX",product="px")
        await repository.close()
        assert rows==[{"symbol":"TSTX.px2612","exchange":"TSTX","instrument":"px2612","product":"px","product_name":"Product X","delivery_month":"202612","listed_date":None,"last_trading_date":None,"status":"active","contract_multiplier":10.0,"price_tick":0.5,"currency":"CNY"}]
    finally:
        await connection.execute("DELETE FROM futures_contracts WHERE exchange_code='TSTX'")
        await connection.execute("DELETE FROM products WHERE exchange_code='TSTX'")
        await connection.execute("DELETE FROM exchanges WHERE code='TSTX'")
        await connection.close()

@pytest.mark.asyncio
async def test_postgres_historical_schedule_is_loaded_in_one_range_snapshot():
    import asyncpg
    connection=await asyncpg.connect(os.environ["POSTGRES_TEST_DSN"])
    repository=PostgresMetadataRepository(os.environ["POSTGRES_TEST_DSN"])
    try:
        await connection.execute("INSERT INTO exchanges(code,name) VALUES('HSTX','History Test') ON CONFLICT DO NOTHING")
        await connection.execute("INSERT INTO products(exchange_code,code,name) VALUES('HSTX','hx','History Product') ON CONFLICT DO NOTHING")
        await connection.execute("INSERT INTO futures_contracts(exchange_code,instrument_id,product_code,delivery_month) VALUES('HSTX','hx2612','hx','202612') ON CONFLICT DO NOTHING")
        await connection.execute("INSERT INTO trading_calendar(exchange_code,trading_day,is_trading_day) VALUES('HSTX','2026-09-01',true) ON CONFLICT DO NOTHING")
        await connection.execute("""INSERT INTO trading_sessions(exchange_code,product_code,name,session_order,start_time,end_time,effective_from)
          VALUES('HSTX','hx','day',1,'09:00','10:00','2020-01-01') ON CONFLICT DO NOTHING""")
        await repository.start()
        result=await repository.historical_schedule("HSTX","HSTX.hx2612",date(2026,9,1),date(2026,9,1))
        assert len(result["calendar"])==1 and result["sessions"][0]["product_code"]=="hx"
    finally:
        await repository.close()
        await connection.execute("DELETE FROM trading_sessions WHERE exchange_code='HSTX'")
        await connection.execute("DELETE FROM trading_calendar WHERE exchange_code='HSTX'")
        await connection.execute("DELETE FROM futures_contracts WHERE exchange_code='HSTX'")
        await connection.execute("DELETE FROM products WHERE exchange_code='HSTX'")
        await connection.execute("DELETE FROM exchanges WHERE code='HSTX'")
        await connection.close()

@pytest.mark.asyncio
async def test_postgres_fetch_queue_dedup_claim_and_cooldown_state():
    import asyncpg,uuid
    from datetime import datetime,timezone,timedelta
    dsn=os.environ["POSTGRES_TEST_DSN"];connection=await asyncpg.connect(dsn)
    repository=PostgresMetadataRepository(dsn);await repository.start()
    start=datetime(2026,9,1,tzinfo=timezone.utc);end=start+timedelta(days=1)
    request=HistoricalEnsureRequest("SHFE.rb2610","1m",start,end,trigger=AcquisitionMode.ON_DEMAND)
    first,second=str(uuid.uuid4()),str(uuid.uuid4())
    try:
        assert await repository.acquisition.enqueue(first,"AKSHARE","SINA_DOMESTIC",request,start,end,{"coverage_ratio":0})
        assert not await repository.acquisition.enqueue(second,"AKSHARE","SINA_DOMESTIC",request,start,end,{"coverage_ratio":0})
        assert str(await repository.acquisition.active_covering("AKSHARE","SINA_DOMESTIC",request.instrument_id,"1m",start+timedelta(hours=1),end-timedelta(hours=1)))==first
        job=await repository.acquisition.claim({"AKSHARE":1});assert str(job["id"])==first and job["status"]=="RUNNING"
        assert await repository.acquisition.claim({"AKSHARE":1}) is None
        now=datetime.now(timezone.utc);await repository.acquisition.record_failure("AKSHARE","SINA_DOMESTIC","1m",now,now+timedelta(minutes=1),"RATE_LIMIT")
        state=await repository.acquisition.refresh_state("AKSHARE","SINA_DOMESTIC","1m")
        assert state["consecutive_failures"]==1 and state["last_error_code"]=="RATE_LIMIT"
    finally:
        await repository.close()
        await connection.execute("DELETE FROM historical_fetch_requests WHERE id=ANY($1::uuid[])",[uuid.UUID(first),uuid.UUID(second)])
        await connection.execute("DELETE FROM historical_instrument_access WHERE instrument_id=$1",request.instrument_id)
        await connection.execute("DELETE FROM historical_provider_refresh_state WHERE provider_code='akshare' AND interval='1m'")
        await connection.close()
