import time
from api.health import calculate_health

def test_health_states():
    assert calculate_health(ready=False,zeromq_healthy=False,last_message_monotonic=None,questdb_healthy=False,websocket_clients=0,stale_after=5).status=="UNHEALTHY"
    assert calculate_health(ready=True,zeromq_healthy=True,last_message_monotonic=time.monotonic(),questdb_healthy=True,websocket_clients=1,stale_after=5).status=="HEALTHY"
    assert calculate_health(ready=True,zeromq_healthy=True,last_message_monotonic=time.monotonic()-10,questdb_healthy=True,websocket_clients=0,stale_after=5).status=="DEGRADED"
