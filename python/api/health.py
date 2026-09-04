from __future__ import annotations
from datetime import datetime,timedelta,timezone
import time
from .models import HealthResponse

def calculate_health(*,ready:bool,zeromq_healthy:bool,last_message_monotonic:float|None,questdb_healthy:bool|None,postgres_healthy:bool|None=True,websocket_clients:int,stale_after:float,providers:dict[str,str]|None=None)->HealthResponse:
    recent=last_message_monotonic is not None and time.monotonic()-last_message_monotonic<=stale_after
    components={"api":"HEALTHY" if ready else "UNHEALTHY","zeromq":"HEALTHY" if zeromq_healthy else "UNHEALTHY","live_feed":"HEALTHY" if recent else "DEGRADED","questdb":"HEALTHY" if questdb_healthy is True else "DEGRADED" if questdb_healthy is None else "UNHEALTHY","postgres":"HEALTHY" if postgres_healthy is True else "DEGRADED" if postgres_healthy is None else "UNHEALTHY"}
    if not ready:overall="UNHEALTHY"
    elif zeromq_healthy and recent and questdb_healthy is not False and postgres_healthy is not False:overall="HEALTHY"
    else:overall="DEGRADED"
    last=None if last_message_monotonic is None else (datetime.now(timezone.utc)-timedelta(seconds=max(0,time.monotonic()-last_message_monotonic))).isoformat().replace("+00:00","Z")
    return HealthResponse(status=overall,components=components,websocket_clients=websocket_clients,last_live_message_time=last,providers=providers or {})
