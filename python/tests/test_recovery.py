import httpx
import pytest
from live.cache import LatestQuoteCache
from api.questdb_repository import LATEST_QUOTES_SQL,QuestDBQuoteRepository
from api.recovery import reconcile_recovery
from conftest import make_tick

@pytest.mark.asyncio
async def test_recovery_cannot_overwrite_newer_buffered_live_tick():
    cache=LatestQuoteCache();await cache.update(make_tick(seq=20));applied=await reconcile_recovery(cache,[make_tick(seq=10)])
    assert applied==0 and (await cache.lookup("SHFE","zn2610"))["seq"]==20

@pytest.mark.asyncio
async def test_latest_query_and_row_mapping():
    tick=make_tick();names=["event_ts","recv_ts","producer_id","seq","exchange","instrument","trading_day","action_day","last_price","volume","turnover","open_interest","upper_limit_price","lower_limit_price"]
    names += [field for i in range(1,6) for field in (f"bid_price{i}",f"bid_volume{i}",f"ask_price{i}",f"ask_volume{i}")]
    values=["2026-09-04T03:31:25.500000Z","2026-09-04T03:31:25.503421123Z",tick["producer_id"],tick["seq"],tick["exchange"],tick["instrument"],tick["trading_day"],tick["action_day"],tick["last_price"],tick["volume"],tick["turnover"],tick["open_interest"],tick["upper_limit_price"],tick["lower_limit_price"]]
    values += [value for i in range(5) for value in (tick["bid_price"][i],tick["bid_volume"][i],tick["ask_price"][i],tick["ask_volume"][i])]
    async def handler(request):
        assert "LATEST" in str(request.url);return httpx.Response(200,json={"columns":[{"name":name} for name in names],"dataset":[values]})
    client=httpx.AsyncClient(base_url="http://test",transport=httpx.MockTransport(handler));repo=QuestDBQuoteRepository("http://test",client=client)
    result=await repo.load_latest_quotes();await repo.close()
    assert result[0]["seq"]==tick["seq"] and len(result[0]["bid_price"])==5
    assert "LATEST ON event_ts PARTITION BY exchange, instrument" in LATEST_QUOTES_SQL
