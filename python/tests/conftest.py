import copy
import pytest

def make_tick(symbol="SHFE.zn2610",seq=10,producer="550e8400-e29b-41d4-a716-446655440000",event_ts=1788489085500000):
    exchange,instrument=symbol.split(".",1)
    return {"schema_version":1,"event_ts":event_ts,"recv_ts":event_ts*1000+3_421_123,"producer_id":producer,"seq":seq,"exchange":exchange,"instrument":instrument,"trading_day":"20260904","action_day":"20260904","last_price":22580.0,"volume":185621,"turnover":4182000000.0,"open_interest":121035.0,"upper_limit_price":24000.0,"lower_limit_price":21000.0,"bid_price":[22575.0]*5,"bid_volume":[42]*5,"ask_price":[22580.0]*5,"ask_volume":[27]*5}

@pytest.fixture
def tick():return make_tick()

class FakeRepository:
    def __init__(self,ticks=None):self.ticks=ticks or [];self.closed=False
    async def load_latest_quotes(self):return copy.deepcopy(self.ticks)
    async def close(self):self.closed=True
