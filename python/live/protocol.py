from __future__ import annotations
import msgpack
SCHEMA_VERSION = 2
REQUIRED = {"schema_version","event_ts","recv_ts","producer_id","seq","exchange","instrument","trading_day","action_day","last_price","volume","turnover","open_interest","upper_limit_price","lower_limit_price","bid_price","bid_volume","ask_price","ask_volume"}
def validate_tick(value: dict) -> dict:
    if not isinstance(value, dict): raise ValueError("live payload must be a map")
    missing = REQUIRED-value.keys()
    if missing: raise ValueError(f"missing live fields: {sorted(missing)}")
    if value["schema_version"] not in (1,SCHEMA_VERSION): raise ValueError(f"unsupported schema_version: {value['schema_version']}")
    if value["schema_version"] == 1:
        value.update(provider="ctp",event_type="quote_snapshot",instrument_id=f'{value["exchange"]}.{value["instrument"]}',quality="UNKNOWN")
    else:
        missing={"provider","event_type","instrument_id","quality"}-value.keys()
        if missing:raise ValueError(f"missing live fields: {sorted(missing)}")
    for name in ("bid_price","bid_volume","ask_price","ask_volume"):
        if len(value[name]) != 5: raise ValueError(f"{name} must contain five levels")
    return value
def decode_tick(payload: bytes) -> dict:
    return validate_tick(msgpack.unpackb(payload, raw=False))
def expected_topic(tick: dict) -> str: return f"{tick['exchange']}.{tick['instrument']}"
