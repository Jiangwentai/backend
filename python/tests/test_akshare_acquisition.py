from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from providers.akshare.acquisition import AkshareHistoricalExecutor


class Metadata:
    async def provider_symbol(self,instrument_id,as_of,provider_source="SINA"):
        return "LME","LME.zn.3m","LZNT" if provider_source=="EASTMONEY" else "ZSD"


class Service:
    def __init__(self):self.requests=[]
    async def ingest_bars(self,request):
        self.requests.append(request)
        return SimpleNamespace(rows_received=0,rows=(),lineage={}),0,None


@pytest.mark.asyncio
async def test_foreign_daily_uses_confirmed_sina_history_endpoint():
    service=Service();executor=AkshareHistoricalExecutor(service,Metadata())
    await executor({"id":"request","provider_code":"akshare","instrument_id":"LME.zn.3m",
      "interval":"1d","range_start":datetime(2026,1,1,tzinfo=timezone.utc),
      "range_end":datetime(2026,9,1,tzinfo=timezone.utc),"trigger":"ON_DEMAND"})
    assert service.requests[0].endpoint=="futures_foreign_daily_sina"


@pytest.mark.asyncio
async def test_foreign_daily_eastmoney_source_uses_its_alias_and_endpoint():
    service=Service();executor=AkshareHistoricalExecutor(service,Metadata())
    await executor({"id":"request","provider_code":"akshare","provider_source":"EASTMONEY",
      "instrument_id":"LME.zn.3m","interval":"1d",
      "range_start":datetime(2026,1,1,tzinfo=timezone.utc),
      "range_end":datetime(2026,9,1,tzinfo=timezone.utc),"trigger":"ON_DEMAND"})
    assert service.requests[0].provider_symbol=="LZNT"
    assert service.requests[0].endpoint=="futures_foreign_daily_eastmoney"


@pytest.mark.asyncio
async def test_foreign_minute_executor_has_defensive_rejection_and_no_provider_call():
    service=Service();executor=AkshareHistoricalExecutor(service,Metadata())
    with pytest.raises(ValueError,match="foreign 1m endpoint unavailable"):
        await executor({"id":"request","provider_code":"akshare","instrument_id":"LME.zn.3m",
          "interval":"1m","range_start":datetime(2026,1,1,tzinfo=timezone.utc),
          "range_end":datetime(2026,9,1,tzinfo=timezone.utc),"trigger":"ON_DEMAND"})
    assert service.requests==[]
