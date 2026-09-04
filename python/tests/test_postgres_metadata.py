import os
import pytest
from api.postgres_repository import PostgresMetadataRepository

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
