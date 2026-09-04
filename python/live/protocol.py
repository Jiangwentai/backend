from __future__ import annotations
import msgpack
SCHEMA_VERSION = 1
REQUIRED = {"schema_version","event_ts","recv_ts","producer_id","seq","exchange","instrument","trading_day","action_day","last_price","volume","turnover","open_interest","upper_limit_price","lower_limit_price","bid_price","bid_volume","ask_price","ask_volume"}
def decode_tick(payload: bytes) -> dict:
    value = msgpack.unpackb(payload, raw=False)
    if not isinstance(value, dict): raise ValueError("live payload must be a map")
    missing = REQUIRED-value.keys()
    if missing: raise ValueError(f"missing live fields: {sorted(missing)}")
    if value["schema_version"] != SCHEMA_VERSION: raise ValueError(f"unsupported schema_version: {value['schema_version']}")
    for name in ("bid_price","bid_volume","ask_price","ask_volume"):
        if len(value[name]) != 5: raise ValueError(f"{name} must contain five levels")
    return value
def expected_topic(tick: dict) -> str: return f"{tick['exchange']}.{tick['instrument']}"
