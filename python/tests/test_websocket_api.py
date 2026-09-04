from fastapi.testclient import TestClient
from api.app import create_app
from api.settings import Settings
from conftest import FakeRepository,make_tick

def make_app(ticks):return create_app(settings=Settings(recovery_timeout_seconds=.2),repository=FakeRepository(ticks),start_subscriber=False)

def receive_until(socket,message_type):
    for _ in range(4):
        value=socket.receive_json()
        if value["type"]==message_type:return value
    raise AssertionError(f"no {message_type} message")

def test_initial_snapshot_and_invalid_request():
    with TestClient(make_app([make_tick()])) as client:
        with client.websocket_connect("/v1/stream/quotes") as socket:
            socket.send_json({"protocol_version":1,"action":"destroy_database","symbols":[]})
            assert socket.receive_json()["code"]=="INVALID_REQUEST"
            socket.send_json({"protocol_version":1,"action":"subscribe","symbols":["SHFE.zn2610"]})
            quote=receive_until(socket,"quote");assert quote["symbol"]=="SHFE.zn2610" and quote["data"]["seq"]==10

def test_multi_symbol_subscription_then_unsubscribe():
    with TestClient(make_app([make_tick(),make_tick("SHFE.cu2610",seq=11)])) as client:
        with client.websocket_connect("/v1/stream/quotes") as socket:
            socket.send_json({"protocol_version":1,"action":"subscribe","symbols":["SHFE.zn2610","SHFE.cu2610"]})
            messages=[socket.receive_json() for _ in range(3)];assert {m.get("symbol") for m in messages if m["type"]=="quote"}=={"SHFE.zn2610","SHFE.cu2610"}
            socket.send_json({"protocol_version":1,"action":"unsubscribe","symbols":["SHFE.zn2610"]})
            receive_until(socket,"subscription_ack")
            client.portal.call(client.app.state.websocket_manager.publish,make_tick("SHFE.zn2610",seq=20))
            client.portal.call(client.app.state.websocket_manager.publish,make_tick("SHFE.cu2610",seq=21))
            value=receive_until(socket,"quote");assert value["symbol"]=="SHFE.cu2610" and value["data"]["seq"]==21
