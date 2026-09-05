from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from enum import StrEnum
import random
from typing import Awaitable, Callable, Protocol
import uuid
from collections import Counter

from .coverage import CoverageEngine, ExpectedBarGenerator, _utc
from .models import historical_market

UTC=timezone.utc


class AcquisitionMode(StrEnum):
    MANUAL="MANUAL"
    SCHEDULED="SCHEDULED"
    ON_DEMAND="ON_DEMAND"


class HistoricalFetchStatus(StrEnum):
    QUEUED="QUEUED"
    RUNNING="RUNNING"
    SUCCESS="SUCCESS"
    PARTIAL="PARTIAL"
    FAILED="FAILED"
    SKIPPED="SKIPPED"
    COOLDOWN="COOLDOWN"


class FailureCategory(StrEnum):
    NETWORK_ERROR="NETWORK_ERROR"
    RATE_LIMIT="RATE_LIMIT"
    PROVIDER_ERROR="PROVIDER_ERROR"
    SCHEMA_ERROR="SCHEMA_ERROR"
    MAPPING_ERROR="MAPPING_ERROR"
    EMPTY_RESPONSE="EMPTY_RESPONSE"
    UNSUPPORTED_RANGE="UNSUPPORTED_RANGE"


@dataclass(frozen=True)
class HistoricalEnsureRequest:
    instrument_id:str
    interval:str
    start:datetime
    end:datetime
    preferred_provider:str|None=None
    reason:str="MISSING_HISTORY"
    force:bool=False
    trigger:AcquisitionMode=AcquisitionMode.ON_DEMAND


@dataclass(frozen=True)
class HistoricalRefreshPolicy:
    provider:str
    interval:str
    scheduled_enabled:bool=True
    request_triggered:bool=True
    min_refresh_interval_seconds:int=300
    stale_after_seconds:int|None=300
    publication_delay_seconds:int=0
    recent_refresh_days:int=3
    immutable_after_days:int=7
    max_concurrency:int=1
    supports_arbitrary_range:bool=True
    bounded_recent_days:int|None=None
    supports_latest_bars:bool=True
    acquisition_priority:int=0
    enabled:bool=True
    upstream_source:str="DEFAULT"
    supported_markets:tuple[str,...]=("DOMESTIC","FOREIGN")
    supported_instrument_kinds:tuple[str,...]=()

    def eligibility(self,instrument_id:str,interval:str,instrument_kind:str|None=None):
        if not self.enabled:return False,"PROVIDER_DISABLED"
        if interval!=self.interval:return False,"INTERVAL_NOT_SUPPORTED"
        if historical_market(instrument_id) not in self.supported_markets:return False,"MARKET_NOT_SUPPORTED"
        if self.supported_instrument_kinds and instrument_kind not in self.supported_instrument_kinds:
            return False,"INSTRUMENT_KIND_NOT_SUPPORTED"
        return True,None


@dataclass(frozen=True)
class HistoricalFreshness:
    latest_bar_start:datetime|None
    expected_latest_bar_start:datetime|None
    lag_seconds:float|None
    stale:bool|None


@dataclass(frozen=True)
class EnsureResult:
    status:str
    request_id:str|None
    provider:str|None
    coverage_before:dict|None=None
    freshness:HistoricalFreshness|None=None
    next_allowed_at:datetime|None=None
    reason:str|None=None


class AcquisitionMetrics:
    def __init__(self):self.counts=Counter()
    def increment(self,name):self.counts[name]+=1


class AcquisitionRepository(Protocol):
    async def active_covering(self,provider,source,instrument,interval,start,end):...
    async def refresh_state(self,provider,source,interval):...
    async def enqueue(self,request_id,provider,source,request,start,end,coverage):...
    async def claim(self,limits):...
    async def complete(self,request_id,status,rows_received,rows_written,coverage,error_code=None,error_message=None,result_metadata=None):...
    async def record_success(self,provider,source,interval,when,next_allowed):...
    async def record_failure(self,provider,source,interval,when,next_allowed,error_code):...


class HistoricalFetchCoordinator:
    def __init__(self,queue:AcquisitionRepository,history,metadata,
                 policies:tuple[HistoricalRefreshPolicy,...],*,acquisition_fallback:bool=False,
                 clock:Callable[[],datetime]|None=None,jitter:Callable[[],float]|None=None):
        self.queue=queue;self.history=history;self.metadata=metadata
        self.policies=tuple(policies);self.acquisition_fallback=acquisition_fallback
        self.clock=clock or (lambda:datetime.now(UTC));self.jitter=jitter or random.random
        self.coverage=CoverageEngine();self.expected=ExpectedBarGenerator()
        self.metrics=AcquisitionMetrics()

    def eligible_policies(self,request):
        values=[policy for policy in self.policies if policy.eligibility(request.instrument_id,request.interval)[0]
                and (request.trigger!=AcquisitionMode.SCHEDULED or policy.scheduled_enabled)
                and (request.trigger!=AcquisitionMode.ON_DEMAND or policy.request_triggered)]
        values.sort(key=lambda value:(-value.acquisition_priority,value.provider))
        if request.preferred_provider:
            preferred=[value for value in values if value.provider.upper()==request.preferred_provider.upper()]
            return preferred if preferred or not self.acquisition_fallback else values
        return values

    def ineligible_reason(self,request):
        relevant=[policy for policy in self.policies if policy.provider.upper()==(request.preferred_provider or policy.provider).upper()]
        if relevant and all(policy.interval!=request.interval for policy in relevant):return "INTERVAL_NOT_SUPPORTED"
        reasons=[policy.eligibility(request.instrument_id,request.interval)[1] for policy in relevant]
        if "MARKET_NOT_SUPPORTED" in reasons:return "INTERVAL_NOT_SUPPORTED" if any(
            policy.provider.upper()=="AKSHARE" and request.interval=="1m" and historical_market(request.instrument_id)=="FOREIGN"
            for policy in relevant) else "MARKET_NOT_SUPPORTED"
        return next((reason for reason in reasons if reason),"NO_CAPABLE_PROVIDER")

    async def _snapshot(self,request,provider):
        exchange,instrument=request.instrument_id.split(".",1)
        first=_utc(request.start).astimezone(timezone(timedelta(hours=8))).date()
        last=(_utc(request.end)-timedelta(microseconds=1)).astimezone(timezone(timedelta(hours=8))).date()
        schedule=await self.metadata.historical_schedule(exchange,request.instrument_id,first,last)
        if not schedule["calendar"] or (request.interval!="1d" and not schedule["sessions"]):
            return None,None,HistoricalFreshness(None,None,None,None),[]
        expected=self.expected.generate(request.interval,request.start,request.end,schedule["calendar"],schedule["sessions"])
        bars=await self.history.load_historical_bars(exchange,instrument,request.interval,
            first.strftime("%Y%m%d"),last.strftime("%Y%m%d"),provider)
        cov=self.coverage.calculate(provider,request.instrument_id,request.interval,request.start,request.end,expected,bars)
        expected_set=set(expected)
        latest=max((_utc(row["bar_start"]) for row in bars if _utc(row["bar_start"]) in expected_set),default=None)
        expected_latest=max(expected,default=None)
        lag=(expected_latest-latest).total_seconds() if latest and expected_latest else None
        return expected,cov,HistoricalFreshness(latest,expected_latest,lag,None),bars

    async def ensure_history(self,request:HistoricalEnsureRequest)->EnsureResult:
        self.metrics.increment("historical_fetch_requests_total")
        self.metrics.increment(f"historical_refresh_trigger_total:{request.trigger.value}")
        if request.interval not in {"1m","5m","1h","1d"} or _utc(request.start)>=_utc(request.end):
            raise ValueError("invalid historical ensure range")
        policies=self.eligible_policies(request)
        if not policies:return EnsureResult("NO_ELIGIBLE_PROVIDER",None,None,reason=self.ineligible_reason(request))
        now=self.clock()
        for policy in policies:
            if policy.bounded_recent_days is not None and request.end < now-timedelta(days=policy.bounded_recent_days):
                continue
            expected,cov,freshness,bars=await self._snapshot(request,policy.provider)
            if cov is None:
                return EnsureResult("UNKNOWN_FRESHNESS",None,policy.provider,freshness=freshness,reason="authoritative session metadata unavailable")
            freshness=replace(freshness,stale=(freshness.lag_seconds is not None and
                freshness.lag_seconds>policy.stale_after_seconds+policy.publication_delay_seconds)
                if policy.stale_after_seconds is not None else None)
            recent=request.end>=now-timedelta(days=policy.immutable_after_days)
            if cov.coverage_ratio==1 and not request.force:
                if request.trigger!=AcquisitionMode.SCHEDULED or not recent:
                    self.metrics.increment("historical_fetch_skipped_complete_total")
                    return EnsureResult("ALREADY_COMPLETE",None,policy.provider,asdict(cov),freshness)
            active=await self.queue.active_covering(policy.provider,policy.upstream_source,request.instrument_id,request.interval,request.start,request.end)
            if active:
                self.metrics.increment("historical_fetch_deduplicated_total")
                return EnsureResult("ALREADY_RUNNING",str(active),policy.provider,asdict(cov),freshness)
            state=await self.queue.refresh_state(policy.provider,policy.upstream_source,request.interval)
            if state and state.get("next_allowed_at") and state["next_allowed_at"]>now:
                failure=bool(state.get("consecutive_failures"))
                if failure or not request.force:
                    self.metrics.increment("historical_fetch_cooldown_total")
                    return EnsureResult("COOLDOWN",None,policy.provider,asdict(cov),freshness,state["next_allowed_at"])
            start,end=request.start,request.end
            if expected and cov.coverage_ratio<1 and policy.supports_arbitrary_range:
                observed={_utc(row["bar_start"]) for row in bars}
                missing=[value for value in expected if value not in observed]
                if missing:start,end=missing[0],missing[-1]+({"1m":timedelta(minutes=1),"5m":timedelta(minutes=5),"1h":timedelta(hours=1),"1d":timedelta(days=1)}[request.interval])
            request_id=str(uuid.uuid4())
            created=await self.queue.enqueue(request_id,policy.provider,policy.upstream_source,request,start,end,asdict(cov))
            if created:return EnsureResult("QUEUED",request_id,policy.provider,asdict(cov),freshness)
        return EnsureResult("NO_ELIGIBLE_PROVIDER",None,None,reason="range unsupported")

    async def request_refresh(self,request):return await self.ensure_history(request)

    async def schedule_refresh(self,instruments:list[str],interval:str,*,now:datetime|None=None):
        self.metrics.increment("historical_scheduled_runs_total")
        now=now or self.clock();results=[]
        policies=[value for value in self.policies if value.interval==interval and value.scheduled_enabled]
        days=max((value.recent_refresh_days for value in policies),default=0)
        for instrument in instruments:
            try:
                start=now-timedelta(days=days+2)
                if hasattr(self.metadata,"recent_trading_days"):
                    exchange=instrument.split('.',1)[0]
                    relevant=await self.metadata.recent_trading_days(exchange,now.astimezone(timezone(timedelta(hours=8))).date(),days)
                    if relevant:start=datetime.combine(relevant[0],datetime.min.time(),timezone(timedelta(hours=8))).astimezone(UTC)
                results.append(await self.ensure_history(HistoricalEnsureRequest(instrument,interval,
                    start,now,reason="SCHEDULED_RECENT",trigger=AcquisitionMode.SCHEDULED)))
            except Exception as exc:results.append(EnsureResult("FAILED",None,None,reason=f"{type(exc).__name__}: {exc}"))
        return {"status":"PARTIAL" if any(value.status=="FAILED" for value in results) else "SUCCESS",
                "results":[asdict(value) for value in results]}

    def backoff(self,failures:int,base_seconds:int=60,cap_seconds:int=3600):
        return min(cap_seconds,base_seconds*(2**max(0,failures-1)))+self.jitter()*min(10,base_seconds)


class HistoricalFetchWorker:
    def __init__(self,coordinator:HistoricalFetchCoordinator,executor:Callable[[dict],Awaitable[dict]],limits:dict[str,int],*,clock=None):
        self.coordinator=coordinator;self.executor=executor;self.limits=limits;self.clock=clock or coordinator.clock

    async def run_once(self):
        job=await self.coordinator.queue.claim(self.limits)
        if not job:return None
        self.coordinator.metrics.increment("historical_fetch_started_total")
        now=self.clock()
        try:
            outcome=await self.executor(job)
            request=HistoricalEnsureRequest(job["instrument_id"],job["interval"],job["range_start"],job["range_end"],job["provider_code"],job["reason"],job["force"],AcquisitionMode(job["trigger"]))
            _,coverage,_,_=await self.coordinator._snapshot(request,job["provider_code"])
            status=HistoricalFetchStatus.SUCCESS if coverage and coverage.coverage_ratio==1 else HistoricalFetchStatus.PARTIAL
            await self.coordinator.queue.complete(job["id"],status.value,outcome.get("rows_received",0),outcome.get("rows_written",0),asdict(coverage) if coverage else None,result_metadata=outcome)
            policy=next(value for value in self.coordinator.policies if value.provider==job["provider_code"] and value.interval==job["interval"] and value.upstream_source==job.get("provider_source","DEFAULT"))
            await self.coordinator.queue.record_success(job["provider_code"],job.get("provider_source","DEFAULT"),job["interval"],now,now+timedelta(seconds=policy.min_refresh_interval_seconds))
            self.coordinator.metrics.increment("historical_fetch_success_total" if status==HistoricalFetchStatus.SUCCESS else "historical_fetch_partial_total")
            return status.value
        except Exception as exc:
            category=self._category(exc);source=job.get("provider_source","DEFAULT");state=await self.coordinator.queue.refresh_state(job["provider_code"],source,job["interval"]);failures=(state or {}).get("consecutive_failures",0)+1
            delay=self.coordinator.backoff(failures)
            await self.coordinator.queue.complete(job["id"],HistoricalFetchStatus.FAILED.value,0,0,None,category.value,str(exc))
            await self.coordinator.queue.record_failure(job["provider_code"],source,job["interval"],now,now+timedelta(seconds=delay),category.value)
            self.coordinator.metrics.increment("historical_fetch_failed_total")
            self.coordinator.metrics.increment("historical_provider_backoff_total")
            if category==FailureCategory.RATE_LIMIT:self.coordinator.metrics.increment("historical_provider_rate_limit_total")
            return HistoricalFetchStatus.FAILED.value

    @staticmethod
    def _category(exc):
        name=(type(exc).__name__+" "+str(exc)).upper()
        if "RATE" in name:return FailureCategory.RATE_LIMIT
        if "SCHEMA" in name:return FailureCategory.SCHEMA_ERROR
        if "MAPPING" in name:return FailureCategory.MAPPING_ERROR
        if "EMPTY" in name:return FailureCategory.EMPTY_RESPONSE
        if "UNSUPPORTED" in name:return FailureCategory.UNSUPPORTED_RANGE
        if isinstance(exc,(TimeoutError,ConnectionError)):return FailureCategory.NETWORK_ERROR
        return FailureCategory.PROVIDER_ERROR
