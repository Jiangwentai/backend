import os
import signal
import subprocess
import time
import pytest
from fastapi.testclient import TestClient
from api.app import create_app
from api.questdb_repository import QuestDBQuoteRepository
from api.settings import Settings
from conftest import FakeRepository

def test_cpp_zmq_fastapi_websocket_requested_symbol_only():
    fixture=os.getenv("LIVE_FIXTURE")
    if not fixture:pytest.skip("LIVE_FIXTURE not set")
    endpoint="tcp://127.0.0.1:15558";settings=Settings(zmq_endpoint=endpoint,recovery_timeout_seconds=2)
    with TestClient(create_app(settings=settings,repository=FakeRepository(),start_subscriber=True)) as client:
        with client.websocket_connect("/v1/stream/quotes") as socket:
            socket.send_json({"protocol_version":1,"action":"subscribe","symbols":["SHFE.zn2610"]})
            process=subprocess.Popen([fixture,endpoint])
            symbols=[]
            while not symbols:
                message=socket.receive_json()
                if message["type"]=="quote":symbols.append(message["symbol"])
            process.wait(timeout=5)
            assert process.returncode==0 and symbols==["SHFE.zn2610"]

def test_fastapi_restart_recovers_while_cpp_continues():
    collector=os.getenv("COLLECTOR_FIXTURE");qdb_url=os.getenv("PHASE3_E2E_QDB_URL")
    if not collector or not qdb_url:pytest.skip("set COLLECTOR_FIXTURE and PHASE3_E2E_QDB_URL")
    endpoint="tcp://127.0.0.1:15559";environment=os.environ|{"QDB_HOST":"questdb","QDB_SF_DIR":"/tmp/qwp-restart","ZMQ_PUB_ENDPOINT":endpoint,"SYNTHETIC_RATE":"10000"}
    config=os.getenv("COLLECTOR_CONFIG","/src/config/app.yaml")
    process=subprocess.Popen([collector,config],env=environment)
    settings=Settings(zmq_endpoint=endpoint,questdb_http_url=qdb_url,recovery_timeout_seconds=5)
    try:
        with TestClient(create_app(settings=settings,repository=QuestDBQuoteRepository(qdb_url),start_subscriber=True)) as client:
            for _ in range(100):
                response=client.get("/v1/quotes/SHFE.zn2610")
                if response.status_code==200:break
                time.sleep(.02)
            assert response.status_code==200;before=response.json()["event_ts"]
        time.sleep(.25)
        with TestClient(create_app(settings=settings,repository=QuestDBQuoteRepository(qdb_url),start_subscriber=True)) as restarted:
            recovered=restarted.get("/v1/quotes/SHFE.zn2610");assert recovered.status_code==200
            assert recovered.json()["event_ts"]>=before
    finally:
        process.send_signal(signal.SIGTERM);process.wait(timeout=10)
