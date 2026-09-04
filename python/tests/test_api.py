from fastapi.testclient import TestClient
from api.app import create_app
from api.settings import Settings
from conftest import FakeMetadataRepository,FakeRepository
import pyarrow as pa

def app_for(ticks,metadata=None):return create_app(settings=Settings(recovery_timeout_seconds=.2),repository=FakeRepository(ticks),metadata_repository=metadata or FakeMetadataRepository(),start_subscriber=False)

def test_rest_serializes_quote_without_internal_shape(tick):
    with TestClient(app_for([tick])) as client:
        response=client.get("/v1/quotes/SHFE.zn2610");assert response.status_code==200
        value=response.json();assert value["symbol"]=="SHFE.zn2610" and value["event_ts"].endswith("Z") and value["recv_ts"].endswith("Z")
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
