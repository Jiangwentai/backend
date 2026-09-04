from fastapi.testclient import TestClient
from api.app import create_app
from api.settings import Settings
from conftest import FakeRepository

def app_for(ticks):return create_app(settings=Settings(recovery_timeout_seconds=.2),repository=FakeRepository(ticks),start_subscriber=False)

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
