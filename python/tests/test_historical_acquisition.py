import asyncio
from datetime import date,datetime,time,timedelta,timezone

import pytest

from historical import (AcquisitionMode, HistoricalEnsureRequest,
                        HistoricalFetchCoordinator, HistoricalFetchWorker,
                        HistoricalRefreshPolicy)

UTC=timezone.utc
NOW=datetime(2026,9,6,4,tzinfo=UTC)


class Queue:
    def __init__(self):self.jobs=[];self.state={};self.completed=[];self.enqueues=0
    async def active_covering(self,p,source,i,v,s,e):
        return next((job["id"] for job in self.jobs if job["provider_code"]==p and job["provider_source"]==source and job["instrument_id"]==i and job["interval"]==v and job["status"] in {"QUEUED","RUNNING"} and job["range_start"]<=s and job["range_end"]>=e),None)
    async def refresh_state(self,p,source,i):return self.state.get((p,source,i)) or self.state.get((p,i))
    async def enqueue(self,rid,p,source,r,s,e,c):
        if await self.active_covering(p,source,r.instrument_id,r.interval,s,e):return False
        self.enqueues+=1;self.jobs.append({"id":rid,"provider_code":p,"instrument_id":r.instrument_id,
          "provider_source":source,
          "interval":r.interval,"range_start":s,"range_end":e,"trigger":r.trigger.value,"reason":r.reason,
          "force":r.force,"status":"QUEUED"});return True
    async def claim(self,limits):
        running={}
        for job in self.jobs:
            if job["status"]=="RUNNING":running[job["provider_code"]]=running.get(job["provider_code"],0)+1
        for job in self.jobs:
            if job["status"]=="QUEUED" and running.get(job["provider_code"],0)<limits.get(job["provider_code"],1):
                job["status"]="RUNNING";return dict(job)
    async def complete(self,*args,**kwargs):self.completed.append((args,kwargs));next(job for job in self.jobs if job["id"]==args[0])["status"]=args[1]
    async def record_success(self,p,source,i,w,n):self.state[(p,source,i)]={"last_success_at":w,"next_allowed_at":n,"consecutive_failures":0}
    async def record_failure(self,p,source,i,w,n,e):self.state[(p,source,i)]={"next_allowed_at":n,"consecutive_failures":self.state.get((p,source,i),{}).get("consecutive_failures",0)+1,"last_error_code":e}


class History:
    def __init__(self,bars=()):self.bars=list(bars);self.calls=0
    async def load_historical_bars(self,*args):self.calls+=1;provider=args[-1];return [bar for bar in self.bars if bar["provider"]==provider]


class Metadata:
    def __init__(self,known=True):self.known=known
    async def historical_schedule(self,*args):
        return ({"calendar":[{"trading_day":date(2026,9,6),"is_trading_day":True,"night_session_open":None}],
                 "sessions":[{"start_time":time(9),"end_time":time(9,3),"crosses_midnight":False}]}
                if self.known else {"calendar":[],"sessions":[]})


def request(force=False,trigger=AcquisitionMode.ON_DEMAND):
    return HistoricalEnsureRequest("SHFE.rb2610","1m",datetime(2026,9,6,1,tzinfo=UTC),
      datetime(2026,9,6,1,3,tzinfo=UTC),reason="TEST",force=force,trigger=trigger)


def coordinator(queue=None,history=None,**kwargs):
    return HistoricalFetchCoordinator(queue or Queue(),history or History(),kwargs.pop("metadata",Metadata()),
      (HistoricalRefreshPolicy("AKSHARE","1m",acquisition_priority=50,**kwargs),),clock=lambda:NOW,jitter=lambda:0)


@pytest.mark.asyncio
async def test_complete_skips_provider_work_and_partial_queues_missing_range():
    complete=History([{"provider":"AKSHARE","bar_start":datetime(2026,9,6,1,minute,tzinfo=UTC)} for minute in range(3)])
    assert (await coordinator(history=complete).ensure_history(request())).status=="ALREADY_COMPLETE"
    queue=Queue();partial=History([{"provider":"AKSHARE","bar_start":datetime(2026,9,6,1,0,tzinfo=UTC)}])
    result=await coordinator(queue,partial).ensure_history(request())
    assert result.status=="QUEUED" and queue.jobs[0]["range_start"].minute==1 and queue.jobs[0]["range_end"].minute==3


@pytest.mark.asyncio
async def test_identical_and_contained_requests_are_deduplicated():
    queue=Queue();value=coordinator(queue)
    first,second=await asyncio.gather(value.ensure_history(request()),value.ensure_history(request()))
    assert {first.status,second.status}=={"QUEUED","ALREADY_RUNNING"} and queue.enqueues==1
    contained=HistoricalEnsureRequest("SHFE.rb2610","1m",datetime(2026,9,6,1,1,tzinfo=UTC),datetime(2026,9,6,1,2,tzinfo=UTC))
    assert (await value.ensure_history(contained)).status=="ALREADY_RUNNING"


@pytest.mark.asyncio
async def test_cooldown_force_and_failure_backoff():
    queue=Queue();queue.state[("AKSHARE","1m")]={"next_allowed_at":NOW+timedelta(minutes=3),"consecutive_failures":0}
    value=coordinator(queue)
    assert (await value.ensure_history(request())).status=="COOLDOWN"
    assert (await value.ensure_history(request(True))).status=="QUEUED"
    async def fail(job):raise ConnectionError("down")
    worker=HistoricalFetchWorker(value,fail,{"AKSHARE":1})
    assert await worker.run_once()=="FAILED"
    state=queue.state[("AKSHARE","DEFAULT","1m")];assert state["consecutive_failures"]==1 and state["next_allowed_at"]==NOW+timedelta(seconds=60)


@pytest.mark.asyncio
async def test_bounded_recent_unknown_session_and_preferred_provider_rules():
    old=HistoricalEnsureRequest("LME.zn.3m","1m",NOW-timedelta(days=40),NOW-timedelta(days=39))
    assert (await coordinator(bounded_recent_days=30).ensure_history(old)).status=="NO_ELIGIBLE_PROVIDER"
    assert (await coordinator(metadata=Metadata(False)).ensure_history(request())).status=="UNKNOWN_FRESHNESS"
    value=HistoricalFetchCoordinator(Queue(),History(),Metadata(),(
      HistoricalRefreshPolicy("X","1m",enabled=False,acquisition_priority=100),
      HistoricalRefreshPolicy("AKSHARE","1m",acquisition_priority=50)),acquisition_fallback=False,clock=lambda:NOW)
    preferred=HistoricalEnsureRequest(**{**request().__dict__,"preferred_provider":"X"})
    assert (await value.ensure_history(preferred)).status=="NO_ELIGIBLE_PROVIDER"


@pytest.mark.asyncio
async def test_scheduled_failure_isolation_and_recent_complete_revision_window():
    class Mixed(HistoricalFetchCoordinator):
        async def ensure_history(self,value):
            if "zn" in value.instrument_id:raise RuntimeError("broken")
            return await super().ensure_history(value)
    value=Mixed(Queue(),History(),Metadata(),(HistoricalRefreshPolicy("AKSHARE","1m",recent_refresh_days=3),),clock=lambda:NOW)
    result=await value.schedule_refresh(["SHFE.rb2610","SHFE.zn2610","LME.gc.3m"],"1m")
    assert result["status"]=="PARTIAL" and len(result["results"])==3


@pytest.mark.asyncio
async def test_scheduled_window_uses_last_relevant_trading_days():
    class TradingMetadata(Metadata):
        async def recent_trading_days(self,exchange,on_or_before,count):
            assert count==3;return [date(2026,9,2),date(2026,9,3),date(2026,9,4)]
    queue=Queue();value=coordinator(queue,metadata=TradingMetadata(),recent_refresh_days=3,supports_arbitrary_range=False)
    await value.schedule_refresh(["SHFE.rb2610"],"1m")
    assert queue.jobs[0]["range_start"]==datetime(2026,9,1,16,tzinfo=UTC)


@pytest.mark.asyncio
async def test_market_closed_latest_expected_bar_is_fresh():
    history=History([{"provider":"AKSHARE","bar_start":datetime(2026,9,6,1,minute,tzinfo=UTC)} for minute in range(3)])
    result=await coordinator(history=history).ensure_history(request())
    assert result.freshness.stale is False and result.freshness.lag_seconds==0


@pytest.mark.asyncio
async def test_akshare_foreign_minute_is_rejected_before_history_or_provider_work():
    history=History();queue=Queue()
    value=HistoricalFetchCoordinator(queue,history,Metadata(),(
      HistoricalRefreshPolicy("AKSHARE","1m",supported_markets=("DOMESTIC",)),),clock=lambda:NOW)
    foreign=HistoricalEnsureRequest("LME.zn.3m","1m",NOW-timedelta(days=1),NOW)
    result=await value.ensure_history(foreign)
    assert result.status=="NO_ELIGIBLE_PROVIDER"
    assert result.reason=="INTERVAL_NOT_SUPPORTED"
    assert history.calls==0 and queue.enqueues==0


@pytest.mark.asyncio
async def test_foreign_minute_falls_back_to_capable_provider_or_reports_none():
    foreign=HistoricalEnsureRequest("LME.zn.3m","1m",NOW-timedelta(minutes=3),NOW,
                                    preferred_provider="AKSHARE")
    capable=HistoricalFetchCoordinator(Queue(),History(),Metadata(),(
      HistoricalRefreshPolicy("AKSHARE","1m",acquisition_priority=100,supported_markets=("DOMESTIC",)),
      HistoricalRefreshPolicy("X","1m",acquisition_priority=50,supported_markets=("FOREIGN",))),
      acquisition_fallback=True,clock=lambda:NOW)
    assert (await capable.ensure_history(foreign)).provider=="X"
    unavailable=HistoricalFetchCoordinator(Queue(),History(),Metadata(),(
      HistoricalRefreshPolicy("AKSHARE","1m",supported_markets=("DOMESTIC",)),
      HistoricalRefreshPolicy("X","1m",enabled=False,supported_markets=("FOREIGN",))),
      acquisition_fallback=True,clock=lambda:NOW)
    result=await unavailable.ensure_history(foreign)
    assert result.status=="NO_ELIGIBLE_PROVIDER" and result.reason=="INTERVAL_NOT_SUPPORTED"
