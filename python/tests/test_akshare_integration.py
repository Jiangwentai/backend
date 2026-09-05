from __future__ import annotations

from datetime import date, datetime, timezone
import os
import uuid

import pytest

from providers.akshare.models import HistoricalBar, ProviderId, ReferenceRecord
from providers.akshare.repositories import AkshareMetadataRepository, QuestDbHistoricalBarRepository


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv("POSTGRES_TEST_DSN"),reason="POSTGRES_TEST_DSN is not configured")
async def test_akshare_postgres_mapping_runs_reference_and_revision():
    import asyncpg
    dsn=os.environ["POSTGRES_TEST_DSN"];connection=await asyncpg.connect(dsn);repository=AkshareMetadataRepository(dsn);await repository.start()
    first=str(uuid.uuid4());second=str(uuid.uuid4());now=datetime.now(timezone.utc)
    try:
        await connection.execute("INSERT INTO exchanges(code,name) VALUES('AKTX','AK Test') ON CONFLICT DO NOTHING")
        await connection.execute("INSERT INTO products(exchange_code,code,name) VALUES('AKTX','ak','AK Product') ON CONFLICT DO NOTHING")
        await connection.execute("INSERT INTO futures_contracts(exchange_code,instrument_id,product_code,delivery_month) VALUES('AKTX','ak2610','ak','202610') ON CONFLICT DO NOTHING")
        await connection.execute("""INSERT INTO provider_instruments(provider_code,exchange_code,instrument_id,provider_symbol)
          VALUES('akshare','AKTX','ak2610','AK2610') ON CONFLICT DO NOTHING""")
        assert await repository.resolve("ak2610")==('AKTX','ak2610')
        await repository.begin_run(first,"futures_daily_sina","futures_zh_daily_sina",{"symbol":"AK2610"},"1.18.74")
        bar=HistoricalBar(ProviderId.AKSHARE,"ak2610","AKTX","AK2610","AK2610","1d",datetime(2026,9,1,tzinfo=timezone.utc),date(2026,9,1),1,2,0.5,1.5,10,20,None,1.4,now,"futures_zh_daily_sina","SINA",first)
        assert await repository.record_versions((bar,))==0
        await repository.upsert_reference((ReferenceRecord(ProviderId.AKSHARE,"contracts","AK0",{"name":"test"},"futures_display_main_sina","SINA",first,now),))
        await repository.finish_run(first,status="SUCCESS",received=1,normalized=1,rejected=0,written=1)
        await repository.begin_run(second,"futures_daily_sina","futures_zh_daily_sina",{"symbol":"AK2610"},"1.18.74")
        assert await repository.record_versions((HistoricalBar(**{**bar.__dict__,"fetch_id":second}),))==0
        assert await repository.record_versions((HistoricalBar(**{**bar.__dict__,"close":1.6,"fetch_id":second}),))==1
        assert await connection.fetchval("SELECT count(*) FROM historical_bar_revisions WHERE new_fetch_id=$1",uuid.UUID(second))==1
    finally:
        await repository.close()
        await connection.execute("DELETE FROM historical_bar_revisions WHERE instrument_id='ak2610'")
        await connection.execute("DELETE FROM historical_bar_versions WHERE instrument_id='ak2610'")
        await connection.execute("DELETE FROM provider_reference_records WHERE provider_code='akshare' AND provider_key='AK0'")
        await connection.execute("DELETE FROM provider_unresolved_instruments WHERE provider_code='akshare' AND fetch_id IN ($1,$2)",uuid.UUID(first),uuid.UUID(second))
        await connection.execute("DELETE FROM provider_ingestion_runs WHERE id IN ($1,$2)",uuid.UUID(first),uuid.UUID(second))
        await connection.execute("DELETE FROM provider_instruments WHERE provider_code='akshare' AND exchange_code='AKTX'")
        await connection.execute("DELETE FROM futures_contracts WHERE exchange_code='AKTX'");await connection.execute("DELETE FROM products WHERE exchange_code='AKTX'");await connection.execute("DELETE FROM exchanges WHERE code='AKTX'");await connection.close()


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv("QDB_TEST_HTTP"),reason="QDB_TEST_HTTP is not configured")
async def test_akshare_questdb_historical_bar_idempotency():
    import asyncio,httpx
    base=os.environ["QDB_TEST_HTTP"].rstrip("/");repository=QuestDbHistoricalBarRepository(base);fetch=str(uuid.uuid4());now=datetime.now(timezone.utc)
    bar=HistoricalBar(ProviderId.AKSHARE,"SHFE.rb2610","SHFE","RB2610","RB2610","1d",datetime(2026,9,1,tzinfo=timezone.utc),date(2026,9,1),10,12,9,11,5,7,None,10.5,now,"futures_zh_daily_sina","SINA",fetch)
    async with httpx.AsyncClient() as client:await client.get(f"{base}/exec",params={"query":"TRUNCATE TABLE historical_bars"})
    await repository.upsert((bar,));await repository.upsert((bar,))
    async with httpx.AsyncClient() as client:
        for _ in range(30):
            result=(await client.get(f"{base}/exec",params={"query":"SELECT count() FROM historical_bars"})).json()
            if result.get("dataset",[[0]])[0][0]==1:break
            await asyncio.sleep(.05)
    assert result["dataset"][0][0]==1
