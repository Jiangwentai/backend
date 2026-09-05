from __future__ import annotations

import asyncio
from datetime import date, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from providers.akshare.client import AkshareClient
from providers.akshare.duckdb import query_raw
from providers.akshare.errors import EmptyDatasetError, MappingError, PermanentProviderError, SchemaError, ValidationError
from providers.akshare.ingestion import AkshareIngestionService
from providers.akshare.models import HistoricalBarRequest, ProviderHealthState, QuoteSubscription, ReferenceDataRequest
from providers.akshare.quote_poller import AkshareQuotePoller, snapshot_to_live_event
from providers.akshare.normalizers.futures_minute import trading_day_for
from live.cache import LatestQuoteCache
from providers.akshare.metrics_export import render as render_metrics
from providers.akshare.normalizers import normalize_symbol
from providers.akshare.provider import AkshareProvider
from providers.akshare.raw_archive import RawArchiveRepository
from providers.akshare.registry import ENDPOINTS, endpoint
from providers.akshare.scheduler import BackfillState, backfill


class Frame:
    def __init__(self, rows): self.rows=rows
    def to_dict(self, *, orient): assert orient=="records";return self.rows


class Module:
    __version__="1.18.74"
    def __init__(self, daily=None):self.daily=daily if daily is not None else [{"date":"2026-09-01","open":10.0,"high":12.0,"low":9.0,"close":11.0,"volume":5,"hold":7,"settle":10.5}]
    def futures_zh_daily_sina(self, symbol="RB0"):return Frame(self.daily)
    def futures_comm_info(self,symbol="所有"):return Frame([{"交易所名称":"上海期货交易所","合约名称":"螺纹钢","合约代码":"RB2610"}])
    def futures_zh_minute_sina(self,symbol="RB2610",period="1"):return Frame([
        {"datetime":"2026-09-04 21:01:00","open":10,"high":12,"low":9,"close":11,"volume":5,"hold":7}])
    def futures_zh_spot(self,symbol="RB2610",market="CF",adjust="0"):return Frame([
        {"symbol":"螺纹钢2610","time":"21:01:02","current_price":3500,"bid_price":3499,
         "ask_price":3501,"buy_vol":3,"sell_vol":4,"hold":7,"volume":5} for _ in symbol.split(",")])
    def futures_foreign_hist(self,symbol="ZSD"):return Frame([
        {"date":"2026-09-01","open":2800,"high":2820,"low":2790,"close":2810,"volume":5,"position":7,"s":0}])
    def futures_global_hist_em(self,symbol="LZNT"):return Frame([
        {"日期":"2026-09-01","代码":"LZNT","名称":"综合锌03","开盘":2801,"最新价":2811,
         "最高":2821,"最低":2791,"总量":6,"涨幅":1.2,"持仓":8,"日增":1}])


from instruments import ProviderInstrumentResolver
from instruments.metadata import MemoryInstrumentMetadata

class Resolver(ProviderInstrumentResolver):
    def __init__(self): super().__init__(MemoryInstrumentMetadata())


class Metadata:
    def __init__(self):self.runs={};self.unresolved_rows=[];self.references={};self.versions={};self.revisions=[]
    async def begin_run(self,fetch_id,dataset,endpoint,parameters,version):self.runs[fetch_id]={"status":"RUNNING"}
    async def finish_run(self,fetch_id,**values):self.runs[fetch_id].update(values)
    async def unresolved(self,*values):self.unresolved_rows.append(values)
    async def upsert_reference(self,rows):self.references.update({row.provider_key:row for row in rows});return len(rows)
    async def record_versions(self,bars):
        revisions=0
        for bar in bars:
            old=self.versions.get(bar.identity)
            if old and old.close!=bar.close:self.revisions.append((old,bar));revisions+=1
            self.versions[bar.identity]=bar
        return revisions


class Bars:
    def __init__(self):self.values={}
    async def upsert(self,bars):
        for bar in bars:self.values[bar.identity]=bar
        return len(bars)


def make_service(tmp_path,module=None,resolver=None):
    client=AkshareClient(module=module or Module(),min_interval_ms=0)
    provider=AkshareProvider(client,resolver or Resolver(),RawArchiveRepository(tmp_path))
    metadata=Metadata();bars=Bars();return AkshareIngestionService(provider,metadata,bars),metadata,bars


def test_capabilities_registry_and_optional_dependency_boundary(tmp_path):
    service,_,_=make_service(tmp_path)
    assert service.provider.capabilities.historical_bars and service.provider.capabilities.reference_data
    assert not service.provider.capabilities.realtime_quotes and service.provider.capabilities.best_effort_quotes
    assert service.provider.capabilities.intraday_bars
    assert endpoint("futures_daily_sina").function_name=="futures_zh_daily_sina"
    assert not ENDPOINTS["futures_inventory_99"].enabled


@pytest.mark.parametrize("symbol",["rb2610"," I2610 ","TA609","IF2612","si2610","sc2610"])
def test_symbol_normalization_covers_supported_exchange_conventions(symbol):
    assert normalize_symbol(symbol)==symbol.strip().upper()


@pytest.mark.asyncio
async def test_daily_normalization_lineage_archive_and_duckdb(tmp_path):
    service,metadata,bars=make_service(tmp_path)
    batch,written,revisions=await service.ingest_bars(HistoricalBarRequest("rb2610",exchange="SHFE"))
    assert written==1 and revisions==0 and batch.rows[0].instrument_id=="SHFE.rb2610"
    assert batch.rows[0].bar_start.isoformat()=="2026-08-31T16:00:00+00:00"
    assert batch.lineage["provider_version"]=="1.18.74" and batch.lineage["upstream_source"]=="SINA"
    raw=Path(batch.raw_archive)/"raw.parquet";assert raw.exists()
    table=pq.read_table(raw);assert {"date","_fetch_id","_akshare_version","_request_parameters"}<=set(table.column_names)
    assert query_raw(tmp_path,dataset="futures_daily_sina").num_rows==1
    assert metadata.runs[batch.fetch_id]["status"]=="SUCCESS" and len(bars.values)==1


@pytest.mark.asyncio
async def test_sina_and_eastmoney_foreign_daily_coexist_with_semantic_columns(tmp_path):
    service,_,bars=make_service(tmp_path)
    sina,_,_=await service.ingest_bars(HistoricalBarRequest("ZSD",endpoint="futures_foreign_daily_sina",exchange="LME"))
    eastmoney,_,_=await service.ingest_bars(HistoricalBarRequest("LZNT",endpoint="futures_foreign_daily_eastmoney",exchange="LME"))
    assert sina.rows[0].instrument_id==eastmoney.rows[0].instrument_id=="LME.zn.3m"
    assert sina.rows[0].provider_symbol=="ZSD" and sina.rows[0].upstream_source=="SINA"
    assert eastmoney.rows[0].provider_symbol=="LZNT" and eastmoney.rows[0].upstream_source=="EASTMONEY"
    assert (eastmoney.rows[0].open,eastmoney.rows[0].high,eastmoney.rows[0].low,eastmoney.rows[0].close)==(2801,2821,2791,2811)
    assert len(bars.values)==2
    assert Path(eastmoney.raw_archive,"raw.parquet").exists()


@pytest.mark.asyncio
async def test_idempotency_and_revision_preserve_two_raw_fetches(tmp_path):
    service,metadata,bars=make_service(tmp_path)
    first,_,_=await service.ingest_bars(HistoricalBarRequest("RB2610"))
    service.provider.client._module.daily[0]["close"]=11.5
    second,written,revisions=await service.ingest_bars(HistoricalBarRequest("RB2610"))
    assert first.fetch_id!=second.fetch_id and written==1 and revisions==1
    assert len(list(tmp_path.glob("provider=akshare/dataset=futures_daily_sina/fetch_date=*/fetch_id=*/raw.parquet")))==2
    assert len(bars.values)==1 and next(iter(bars.values.values())).close==11.5 and len(metadata.revisions)==1


@pytest.mark.asyncio
async def test_unresolved_mapping_is_archived_and_reported(tmp_path):
    service,metadata,bars=make_service(tmp_path)
    batch,written,_=await service.ingest_bars(HistoricalBarRequest("ZZ0"))
    assert written==0 and batch.unresolved_symbols==("ZZ0",) and metadata.unresolved_rows
    assert Path(batch.raw_archive,"raw.parquet").exists() and not bars.values
    assert service.provider.client.metrics.mapping_errors_total==1


@pytest.mark.asyncio
async def test_schema_drift_archives_before_failing(tmp_path):
    service,metadata,_=make_service(tmp_path,Module([{"date":"2026-09-01","open":1,"high":2,"low":0}]))
    with pytest.raises(SchemaError):await service.ingest_bars(HistoricalBarRequest("RB2610"))
    assert len(list(tmp_path.glob("provider=akshare/dataset=futures_daily_sina/fetch_date=*/fetch_id=*/raw.parquet")))==1
    assert service.provider.client.metrics.schema_errors_total==1
    assert next(iter(metadata.runs.values()))["status"]=="FAILED"


@pytest.mark.asyncio
@pytest.mark.parametrize("change",[
    {"high":8.0,"low":9.0},{"close":13.0},{"volume":-1},
])
async def test_invalid_bars_fail_visible(tmp_path,change):
    module=Module();module.daily[0].update(change);service,_,_=make_service(tmp_path,module)
    with pytest.raises(ValidationError):await service.ingest_bars(HistoricalBarRequest("RB2610"))


@pytest.mark.asyncio
async def test_reference_ingestion(tmp_path):
    service,metadata,_=make_service(tmp_path)
    batch,written=await service.ingest_reference(ReferenceDataRequest())
    assert written==1 and batch.rows[0].provider_key=="RB2610" and "RB2610" in metadata.references


@pytest.mark.asyncio
async def test_retry_is_bounded_and_health_changes(tmp_path):
    class Flaky(Module):
        def __init__(self):super().__init__();self.calls=0
        def futures_zh_daily_sina(self,symbol="RB0"):
            self.calls+=1
            if self.calls<3:raise ConnectionError("temporary")
            return Frame(self.daily)
    module=Flaky();client=AkshareClient(module=module,min_interval_ms=0,max_attempts=3,sleep=lambda _:asyncio.sleep(0))
    provider=AkshareProvider(client,Resolver(),RawArchiveRepository(tmp_path))
    batch=await provider.fetch_bars(HistoricalBarRequest("RB2610"))
    assert batch.rows and client.metrics.retries_total==2 and provider.health.state==ProviderHealthState.AVAILABLE


@pytest.mark.asyncio
async def test_empty_response_is_not_silently_successful(tmp_path):
    service,metadata,_=make_service(tmp_path,Module([]))
    with pytest.raises(EmptyDatasetError):await service.ingest_bars(HistoricalBarRequest("RB2610"))
    assert next(iter(metadata.runs.values()))["status"]=="FAILED"


@pytest.mark.asyncio
async def test_permanent_error_is_not_retried_and_metrics_are_exportable(tmp_path):
    class Broken(Module):
        def __init__(self):super().__init__();self.calls=0
        def futures_zh_daily_sina(self,symbol="RB0"):self.calls+=1;raise ValueError("bad symbol")
    module=Broken();client=AkshareClient(module=module,min_interval_ms=0,max_attempts=3)
    provider=AkshareProvider(client,Resolver(),RawArchiveRepository(tmp_path))
    with pytest.raises(PermanentProviderError):await provider.fetch_bars(HistoricalBarRequest("RB2610"))
    assert module.calls==1 and client.metrics.retries_total==0
    assert "akshare_requests_failed_total 1" in render_metrics(client.metrics)


@pytest.mark.asyncio
async def test_backfill_resume(tmp_path):
    calls=[]
    async def operation(symbol):calls.append(symbol)
    store=BackfillState(tmp_path/"state.json")
    await backfill(["A","B"],operation,store);await backfill(["A","B","C"],operation,store)
    assert calls==["A","B","C"] and store.load()["completed"]==["A","B","C"]


@pytest.mark.asyncio
async def test_minute_bar_normalization_timezone_night_day_and_range(tmp_path):
    service,_,_=make_service(tmp_path)
    batch,written,_=await service.ingest_bars(HistoricalBarRequest(
        "RB2610",date(2026,9,7),date(2026,9,7),"futures_1m_sina","SHFE"))
    assert written==1 and batch.rows[0].interval=="1m"
    assert batch.rows[0].bar_start.isoformat()=="2026-09-04T13:01:00+00:00"
    assert batch.rows[0].trading_day==date(2026,9,7)
    assert trading_day_for(datetime.fromisoformat("2026-09-04 21:00:00"))==date(2026,9,7)


@pytest.mark.asyncio
async def test_quote_poller_emits_best_effort_snapshots_without_trades(tmp_path):
    class Transport:
        def __init__(self):self.events=[];self.cache=LatestQuoteCache()
        async def publish(self,event):self.events.append(event);await self.cache.update(event)
    from live.ingress import LiveEventIngress
    transport=Transport();client=AkshareClient(module=Module(),min_interval_ms=0)
    subscription=QuoteSubscription("SHFE.rb2610","RB2610","SHFE")
    poller=AkshareQuotePoller(client,LiveEventIngress([transport]),[subscription],poll_interval_seconds=3)
    assert await poller.poll_once()==1
    event=transport.events[0]
    assert event["provider"]=="AKSHARE" and event["quality"]=="BEST_EFFORT"
    assert event["event_type"]=="quote_snapshot" and poller.seq==1
    assert event["bid_price"]==[3499.0,None,None,None,None]
    from api.models import quote_from_tick
    response=quote_from_tick(await transport.cache.lookup("SHFE","rb2610","akshare"))
    assert response.provider=="AKSHARE" and response.quality=="BEST_EFFORT"


@pytest.mark.asyncio
async def test_provider_aware_cache_and_akshare_staleness():
    from datetime import timezone
    cache=LatestQuoteCache({"akshare":0})
    base={"schema_version":2,"event_type":"quote_snapshot","instrument_id":"SHFE.rb2610","quality":"UNKNOWN",
      "exchange":"SHFE","instrument":"rb2610","event_ts":1,"recv_ts":1,"producer_id":"p","seq":1}
    await cache.update({**base,"provider":"ctp"});await cache.update({**base,"provider":"AKSHARE","quality":"BEST_EFFORT"})
    assert (await cache.lookup("SHFE","rb2610","ctp"))["provider"]=="ctp"
    quote=await cache.lookup("SHFE","rb2610","akshare")
    assert quote["provider"]=="AKSHARE" and quote["stale"] is True
