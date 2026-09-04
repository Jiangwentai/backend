import msgpack,pytest
from live.cache import LatestQuoteCache
from live.protocol import decode_tick,expected_topic
def tick(seq=1,producer="p",event=1):return {"schema_version":1,"event_ts":event,"recv_ts":event,"producer_id":producer,"seq":seq,"exchange":"SHFE","instrument":"zn2610","trading_day":"20260904","action_day":"20260904","last_price":1.0,"volume":1,"turnover":1.0,"open_interest":1.0,"upper_limit_price":2.0,"lower_limit_price":.5,"bid_price":[1.0]*5,"bid_volume":[1]*5,"ask_price":[1.1]*5,"ask_volume":[1]*5}
def test_decode_and_topic():
 t=decode_tick(msgpack.packb(tick(),use_bin_type=True));assert expected_topic(t)=="SHFE.zn2610"
def test_schema_version_visible_failure():
 t=tick();t["schema_version"]=2
 with pytest.raises(ValueError,match="unsupported schema_version"):decode_tick(msgpack.packb(t,use_bin_type=True))
@pytest.mark.asyncio
async def test_cache_sequence_ordering():
 c=LatestQuoteCache();assert await c.update(tick(2));assert not await c.update(tick(1));assert (await c.lookup("SHFE","zn2610"))["seq"]==2
