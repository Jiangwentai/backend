from fastapi.testclient import TestClient
from api.app import create_app
from api.settings import Settings
from conftest import FakeMetadataRepository,FakeRepository
import pyarrow as pa
from datetime import date,time

def app_for(ticks,metadata=None,settings=None):return create_app(settings=settings or Settings(recovery_timeout_seconds=.2),repository=FakeRepository(ticks),metadata_repository=metadata or FakeMetadataRepository(),start_subscriber=False)

def test_rest_serializes_quote_without_internal_shape(tick):
    with TestClient(app_for([tick])) as client:
        response=client.get("/v1/quotes/SHFE.zn2610");assert response.status_code==200
        value=response.json();assert value["symbol"]=="SHFE.zn2610" and value["provider"]=="ctp" and value["event_type"]=="quote_snapshot" and value["event_ts"].endswith("Z") and value["recv_ts"].endswith("Z")
        assert value["bid"][0]=={"price":22575.0,"volume":42} and "bid_price" not in value
        assert len(client.get("/v1/quotes").json())==1

def test_rest_not_found_and_invalid_symbol():
    with TestClient(app_for([])) as client:
        assert client.get("/v1/quotes/SHFE.unknown").status_code==404
        assert client.get("/v1/quotes/not-valid").status_code==422

def test_health_is_degraded_without_live_message():
    with TestClient(app_for([])) as client:
        value=client.get("/health").json();assert value["status"]=="DEGRADED"
        assert value["components"]["api"]=="HEALTHY" and value["components"]["live_feed"]=="DEGRADED"

def test_instruments_are_filtered_and_serialized():
    metadata=FakeMetadataRepository([{"symbol":"SHFE.zn2610","exchange":"SHFE","instrument":"zn2610","product":"zn","product_name":"Zinc","delivery_month":"202610","listed_date":"2025-10-16","last_trading_date":"2026-10-15","status":"active","contract_multiplier":5.0,"price_tick":5.0,"currency":"CNY"}])
    with TestClient(app_for([],metadata)) as client:
        response=client.get("/v1/instruments?exchange=SHFE&product=zn&limit=20&offset=2")
        assert response.status_code==200 and response.json()[0]["symbol"]=="SHFE.zn2610"
        assert metadata.calls==[{"exchange":"SHFE","product":"zn","active_only":True,"limit":20,"offset":2}]

def test_instruments_validate_query_and_report_database_failure():
    with TestClient(app_for([],FakeMetadataRepository(fail=True))) as client:
        assert client.get("/v1/instruments?exchange=bad").status_code==422
        assert client.get("/v1/instruments").status_code==503
        assert client.get("/health").json()["components"]["postgres"]=="UNHEALTHY"

def test_akshare_diagnostic_health_uses_metadata_not_live_path():
    with TestClient(app_for([])) as client:
        response=client.get("/v1/providers/akshare/health")
        assert response.status_code==200 and response.json()["state"]=="AVAILABLE"

def test_bars_are_loaded_off_event_loop_and_serialized():
    calls=[]
    def loader(root,exchange,instrument,interval,**filters):
        calls.append((root,exchange,instrument,interval,filters))
        return pa.Table.from_pylist([{"exchange":"SHFE","instrument":"zn2610","trading_day":"20260904","interval":"1m","bar_start":"2026-09-03T13:00:00Z","open":10.0,"high":12.0,"low":9.0,"close":11.0,"volume":5,"open_interest":100.0,"snapshot_count":3}])
    app=create_app(settings=Settings(recovery_timeout_seconds=.2,archive_root="/archive"),repository=FakeRepository(),metadata_repository=FakeMetadataRepository(),bar_loader=loader,start_subscriber=False)
    with TestClient(app) as client:
        response=client.get("/v1/bars/SHFE.zn2610?interval=1m&start_day=20260904")
        assert response.status_code==200 and response.json()[0]["volume"]==5
        assert calls==[("/archive","SHFE","zn2610","1m",{"start_day":"20260904","end_day":None})]

def test_bars_validate_requests_and_report_missing_archive():
    def missing(*args,**kwargs):raise FileNotFoundError
    app=create_app(settings=Settings(recovery_timeout_seconds=.2),repository=FakeRepository(),metadata_repository=FakeMetadataRepository(),bar_loader=missing,start_subscriber=False)
    with TestClient(app) as client:
        assert client.get("/v1/bars/SHFE.zn2610?interval=30s").status_code==422
        assert client.get("/v1/bars/SHFE.zn2610?start_day=2026-09-04").status_code==422
        assert client.get("/v1/bars/SHFE.zn2610").status_code==404

def test_akshare_bars_use_canonical_storage_and_expose_provenance():
    row={"exchange":"SHFE","instrument":"zn2610","trading_day":"2026-09-04","interval":"1d","bar_start":"2026-09-03T16:00:00Z","open":10,"high":12,"low":9,"close":11,"volume":5,"open_interest":7,"settlement":10.5,"provider":"AKSHARE","source":"futures_zh_daily_sina","upstream_source":"SINA"}
    app=create_app(settings=Settings(recovery_timeout_seconds=.2),repository=FakeRepository(bars=[row]),metadata_repository=FakeMetadataRepository(),start_subscriber=False)
    with TestClient(app) as client:
        response=client.get("/v1/bars/SHFE.zn2610?interval=1d&provider=akshare")
        assert response.status_code==200 and response.json()[0]["provider"]=="AKSHARE" and response.json()[0]["source"]=="futures_zh_daily_sina"

def test_historical_composite_api_and_coverage_metadata():
    schedule={"calendar":[{"trading_day":date(2026,9,1),"is_trading_day":True,"night_session_open":None}],
              "sessions":[{"start_time":time(9),"end_time":time(9,4),"crosses_midnight":False,
                           "effective_from":date(2020,1,1),"effective_to":None}]}
    def bar(minute,provider):return {"exchange":"SHFE","instrument":"rb2610","instrument_id":"SHFE.rb2610",
      "trading_day":"2026-09-01","interval":"1m","bar_start":f"2026-09-01T01:0{minute}:00Z",
      "open":1,"high":1,"low":1,"close":1,"volume":None,"open_interest":None,"provider":provider}
    rows=[bar(0,"X"),bar(1,"X"),bar(3,"X"),bar(1,"AKSHARE"),bar(2,"AKSHARE")]
    settings=Settings(recovery_timeout_seconds=.2,historical_provider_policy="X:100:BROKER,AKSHARE:50:PUBLIC")
    app=create_app(settings=settings,repository=FakeRepository(bars=rows),
                   metadata_repository=FakeMetadataRepository(schedule=schedule),start_subscriber=False)
    with TestClient(app) as client:
        response=client.get("/v1/bars/SHFE.rb2610?interval=1m&start_day=20260901&end_day=20260901&selection=composite")
        assert response.status_code==200
        value=response.json();assert value["complete"] and value["providers_used"]=={"X":3,"AKSHARE":1}
        assert [row["provider"] for row in value["bars"]]==["X","X","AKSHARE","X"]
        coverage=client.get("/v1/historical-coverage?instrument=SHFE.rb2610&interval=1m&start_day=20260901&end_day=20260901").json()
        assert {row["provider"]:row["coverage_ratio"] for row in coverage["providers"]}=={"AKSHARE":.5,"X":.75}

def test_historical_strict_incomplete_is_structured():
    schedule={"calendar":[{"trading_day":date(2026,9,1),"is_trading_day":True,"night_session_open":None}],
              "sessions":[{"start_time":time(9),"end_time":time(9,2),"crosses_midnight":False}]}
    row={"provider":"X","bar_start":"2026-09-01T01:00:00Z"}
    app=create_app(settings=Settings(recovery_timeout_seconds=.2,historical_provider_policy="X:1:UNKNOWN"),
      repository=FakeRepository(bars=[row]),metadata_repository=FakeMetadataRepository(schedule=schedule),start_subscriber=False)
    with TestClient(app) as client:
        response=client.get("/v1/bars/SHFE.rb2610?interval=1m&start_day=20260901&end_day=20260901&selection=single&require_complete=true")
        assert response.status_code==409 and response.json()["detail"]["code"]=="HISTORICAL_INCOMPLETE"

def test_history_ensure_only_queues_and_get_remains_local():
    class Queue:
        def __init__(self):self.jobs=[]
        async def active_covering(self,*args):return None
        async def refresh_state(self,*args):return None
        async def enqueue(self,rid,provider,source,request,start,end,coverage):self.jobs.append(rid);return True
        async def status(self,rid):return {"id":rid,"status":"QUEUED"} if rid in self.jobs else None
    queue=Queue()
    schedule={"calendar":[{"trading_day":date(2026,9,1),"is_trading_day":True,"night_session_open":None}],
              "sessions":[{"start_time":time(9),"end_time":time(9,2),"crosses_midnight":False}]}
    repository=FakeRepository(bars=[])
    app=create_app(settings=Settings(recovery_timeout_seconds=.2),repository=repository,
      metadata_repository=FakeMetadataRepository(schedule=schedule,acquisition=queue),start_subscriber=False,
      bar_loader=lambda *args,**kwargs:pa.Table.from_pylist([]))
    with TestClient(app) as client:
        response=client.post("/v1/history/ensure",json={"instrument":"SHFE.rb2610","interval":"1m",
          "start":"2026-09-01T01:00:00Z","end":"2026-09-01T01:02:00Z"})
        assert response.status_code==202 and response.json()["status"]=="QUEUED" and len(queue.jobs)==1
        status=client.get(f"/v1/history/requests/{queue.jobs[0]}")
        assert status.status_code==200 and status.json()["status"]=="QUEUED"
        # GET uses only the supplied local loader/repository; no provider executor exists in the API.
        assert client.get("/v1/bars/SHFE.rb2610").status_code==200

def test_prometheus_metrics_expose_api_live_database_and_websocket_state():
    with TestClient(app_for([])) as client:
        client.get("/health")
        client.get("/not-a-real-symbol-specific-route")
        response=client.get("/metrics")
        assert response.status_code==200
        assert response.headers["content-type"].startswith("text/plain; version=0.0.4")
        body=response.text
        assert 'market_data_api_requests_total{method="GET",path="/health",status="200"} 1' in body
        assert 'market_data_api_requests_total{method="GET",path="__unmatched__",status="404"} 1' in body
        assert "market_data_live_received_total 0" in body
        assert "market_data_websocket_clients 0" in body
        assert 'market_data_component_healthy{component="questdb"} 1' in body

def test_provider_selection_requires_explicit_choice_by_default(tick):
    other={**tick,"provider":"AKSHARE","quality":"BEST_EFFORT","producer_id":"ak","seq":1}
    with TestClient(app_for([tick,other])) as client:
        assert client.get("/v1/quotes/SHFE.zn2610").status_code==409
        response=client.get("/v1/quotes/SHFE.zn2610?provider=akshare")
        assert response.status_code==200 and response.json()["selection_reason"]=="EXPLICIT_PROVIDER"

def test_configured_provider_failover_and_diagnostic_are_transparent(tick):
    from datetime import datetime,timezone
    now=int(datetime.now(timezone.utc).timestamp()*1_000_000)
    stale={**tick,"event_ts":now-10_000_000,"recv_ts":now*1000-10_000_000_000,"provider":"ctp","quality":"REALTIME"}
    fresh={**tick,"event_ts":now,"recv_ts":now*1000,"provider":"AKSHARE","quality":"BEST_EFFORT","producer_id":"ak"}
    settings=Settings(recovery_timeout_seconds=.2,provider_selection_mode="preferred",provider_fallback_enabled=True)
    with TestClient(app_for([stale,fresh],settings=settings)) as client:
        value=client.get("/v1/quotes/SHFE.zn2610").json()
        assert value["provider"]=="AKSHARE" and value["fallback"] and value["selection_reason"]=="FAILOVER"
        diagnostic=client.get("/v1/provider-selection/SHFE.zn2610").json()
        assert diagnostic["selected_provider"]=="AKSHARE" and len(diagnostic["observations"])==2


def test_receive_hwm_env_reaches_api_subscriber(monkeypatch):
    monkeypatch.setenv("ZMQ_RCVHWM", "4096")
    settings = Settings.from_env()
    app = app_for([], settings=settings)
    with TestClient(app):
        assert app.state.subscriber.rcvhwm == 4096


def test_receive_hwm_defaults_and_invalid_env(monkeypatch):
    import pytest
    monkeypatch.delenv("ZMQ_RCVHWM", raising=False)
    assert Settings.from_env().zmq_rcvhwm == 1000
    for value in ("0", "-1", "2147483648", "bad", "1.5"):
        monkeypatch.setenv("ZMQ_RCVHWM", value)
        with pytest.raises(ValueError):
            Settings.from_env()
