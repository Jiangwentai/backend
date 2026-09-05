from __future__ import annotations
import asyncio
import time
import uuid
from datetime import date,datetime,timedelta,timezone
from contextlib import asynccontextmanager
from typing import Any
from fastapi import FastAPI,HTTPException,Query,Request,WebSocket,WebSocketDisconnect
from fastapi.responses import PlainTextResponse
from starlette.concurrency import run_in_threadpool
from pydantic import ValidationError
from live.cache import LatestQuoteCache
from live.subscriber import LiveSubscriber
from live.selection import ProviderSelector,SelectionPolicy
from historical import (ExpectedBarGenerator, HistoricalIncompleteError,
                        HistoricalProviderPolicy, HistoricalQuality,
                        HistoricalSelector, SelectionMode, HistoricalEnsureRequest,
                        HistoricalFetchCoordinator, HistoricalRefreshPolicy,
                        AcquisitionMode)
from historical.config import parse_refresh_policies
from .health import calculate_health
from .metrics import ApiMetrics,render_metrics
from research import load_bars
from .models import BarResponse,HealthResponse,HistoricalEnsureBody,InstrumentResponse,QuoteResponse,SubscriptionRequest,quote_from_tick,validate_symbol
from .postgres_repository import PostgresMetadataRepository
from .questdb_repository import QuestDBQuoteRepository
from .recovery import reconcile_recovery
from .settings import Settings
from .websocket_manager import WebSocketManager

def create_app(*,settings:Settings|None=None,repository:Any|None=None,metadata_repository:Any|None=None,bar_loader:Any=load_bars,start_subscriber:bool=True)->FastAPI:
    config=settings or Settings.from_env()
    policies=[]
    for item in config.historical_provider_policy.split(","):
        provider_name,priority,quality=item.strip().split(":")
        policies.append(HistoricalProviderPolicy(provider_name.upper(),int(priority),HistoricalQuality(quality.upper())))
    refresh_policies=parse_refresh_policies(config.historical_refresh_policy)
    @asynccontextmanager
    async def lifespan(app:FastAPI):
        cache=LatestQuoteCache({"akshare":config.akshare_quote_stale_after_seconds});manager=WebSocketManager(config.websocket_queue_capacity)
        repo=repository or QuestDBQuoteRepository(config.questdb_http_url,config.recovery_timeout_seconds)
        metadata=metadata_repository or PostgresMetadataRepository(config.postgres_dsn,config.postgres_timeout_seconds)
        subscriber=LiveSubscriber(config.zmq_endpoints,cache,manager.publish,rcvhwm=config.zmq_rcvhwm)
        selector=ProviderSelector(SelectionPolicy(mode=config.provider_selection_mode,
          preferred_providers=config.provider_preference,fallback_enabled=config.provider_fallback_enabled,
          allow_stale=config.provider_allow_stale,discrepancy_bps=config.provider_discrepancy_bps,
          freshness_seconds=(("ctp",config.live_stale_after_seconds),("ibkr",config.live_stale_after_seconds),
            ("synthetic",config.live_stale_after_seconds),("akshare",config.akshare_quote_stale_after_seconds))))
        app.state.cache=cache;app.state.selector=selector;app.state.websocket_manager=manager;app.state.repository=repo;app.state.metadata_repository=metadata;app.state.subscriber=subscriber
        app.state.historical_selector=HistoricalSelector(tuple(policies),config.historical_minimum_coverage)
        app.state.fetch_coordinator=(HistoricalFetchCoordinator(metadata.acquisition,repo,metadata,tuple(refresh_policies),
          acquisition_fallback=config.historical_acquisition_fallback) if hasattr(metadata,"acquisition") else None)
        app.state.ready=False;app.state.questdb_healthy=None;app.state.postgres_healthy=None;subscriber_task=None
        try:
            if start_subscriber:
                subscriber_task=asyncio.create_task(subscriber.run());await asyncio.wait_for(subscriber.ready.wait(),config.recovery_timeout_seconds)
            try:
                recovered=await asyncio.wait_for(repo.load_latest_quotes(),config.recovery_timeout_seconds)
                await reconcile_recovery(cache,recovered)
                app.state.questdb_healthy=True
            except Exception:app.state.questdb_healthy=False
            try:
                await asyncio.wait_for(metadata.start(),config.postgres_timeout_seconds);app.state.postgres_healthy=True
            except Exception:app.state.postgres_healthy=False
            app.state.ready=True
            yield
        finally:
            app.state.ready=False;manager.accepting=False
            subscriber.stop()
            if subscriber_task:
                await asyncio.gather(subscriber_task,return_exceptions=True)
            await manager.shutdown()
            await repo.close()
            await metadata.close()
    app=FastAPI(title="Financial Market Data API",version="1.0.0",lifespan=lifespan)
    app.state.api_metrics=ApiMetrics()

    @app.middleware("http")
    async def observe_http(request:Request,call_next):
        started=time.perf_counter();status=500
        try:
            response=await call_next(request);status=response.status_code;return response
        finally:
            route=request.scope.get("route");path=getattr(route,"path","__unmatched__")
            app.state.api_metrics.observe(request.method,path,status,time.perf_counter()-started)

    @app.get("/metrics",response_class=PlainTextResponse,include_in_schema=False)
    async def metrics():
        return PlainTextResponse(render_metrics(app),media_type="text/plain; version=0.0.4; charset=utf-8")

    @app.get("/health",response_model=HealthResponse)
    async def health():
        sub=app.state.subscriber;metrics=app.state.websocket_manager.metrics
        return calculate_health(ready=app.state.ready,zeromq_healthy=sub.healthy if start_subscriber else True,last_message_monotonic=sub.last_message_monotonic,questdb_healthy=app.state.questdb_healthy,postgres_healthy=app.state.postgres_healthy,websocket_clients=metrics.websocket_clients,stale_after=config.live_stale_after_seconds,providers=await app.state.cache.provider_states())

    @app.get("/v1/instruments",response_model=list[InstrumentResponse])
    async def instruments(exchange:str|None=Query(None,pattern=r"^[A-Z][A-Z0-9_]{1,15}$"),product:str|None=Query(None,pattern=r"^[A-Za-z0-9_-]{1,16}$"),active_only:bool=True,limit:int=Query(500,ge=1,le=2000),offset:int=Query(0,ge=0)):
        try:
            values=await app.state.metadata_repository.list_instruments(exchange=exchange,product=product,active_only=active_only,limit=limit,offset=offset);app.state.postgres_healthy=True;return values
        except Exception as exc:
            app.state.postgres_healthy=False;raise HTTPException(503,"metadata database unavailable") from exc

    @app.get("/v1/providers/akshare/health")
    async def akshare_health():
        try:return await app.state.metadata_repository.provider_health("akshare")
        except Exception as exc:raise HTTPException(503,"AKShare health metadata unavailable") from exc

    @app.get("/v1/quotes/{symbol}",response_model=QuoteResponse)
    async def quote(symbol:str,provider:str|None=Query(None,pattern=r"^(ctp|synthetic|ibkr|akshare)$")):
        try:validate_symbol(symbol)
        except ValueError as exc:raise HTTPException(422,str(exc)) from exc
        exchange,instrument=symbol.split(".",1)
        if provider:
            tick=await app.state.cache.lookup(exchange,instrument,provider)
            if tick is not None:
                tick=app.state.selector.assess([tick])[0];tick["selection_reason"]="EXPLICIT_PROVIDER"
        else:
            result=app.state.selector.select(await app.state.cache.candidates(exchange,instrument))
            tick=result.quote
            if tick is not None:
                tick["selection_reason"]=result.reason;tick["fallback"]=result.fallback;tick["preferred_provider"]=result.preferred_provider
                app.state.api_metrics.provider_selections_total+=1
                if result.fallback:app.state.api_metrics.provider_failovers_total+=1
            elif result.reason=="EXPLICIT_PROVIDER_REQUIRED":
                app.state.api_metrics.provider_selection_failures_total+=1
                raise HTTPException(409,"multiple providers available; specify provider or configure selection policy")
        if tick is None:
            app.state.api_metrics.provider_selection_failures_total+=1;raise HTTPException(404,"quote not found or no eligible provider")
        return quote_from_tick(tick)

    @app.get("/v1/provider-selection/{symbol}")
    async def provider_selection(symbol:str):
        try:validate_symbol(symbol)
        except ValueError as exc:raise HTTPException(422,str(exc)) from exc
        exchange,instrument=symbol.split(".",1)
        value=app.state.selector.diagnose(await app.state.cache.candidates(exchange,instrument))
        if value["discrepancy"]:app.state.api_metrics.provider_discrepancies_total+=1
        return {"symbol":symbol,**value}

    @app.get("/v1/quotes",response_model=list[QuoteResponse])
    async def quotes():
        snapshot=await app.state.cache.snapshot();return [quote_from_tick(tick) for _,tick in sorted(snapshot.items())]

    async def selected_history(symbol,interval,start_day,end_day,provider,selection,require_complete):
        exchange,instrument=symbol.split(".",1)
        if not start_day or not end_day:raise HTTPException(422,"start_day and end_day are required for coverage selection")
        first=date.fromisoformat(f"{start_day[:4]}-{start_day[4:6]}-{start_day[6:]}")
        last=date.fromisoformat(f"{end_day[:4]}-{end_day[4:6]}-{end_day[6:]}")
        if first>last:raise HTTPException(422,"start_day must not follow end_day")
        start=datetime.combine(first,datetime.min.time(),timezone.utc)-timedelta(hours=8)
        end=datetime.combine(last+timedelta(days=1),datetime.min.time(),timezone.utc)-timedelta(hours=8)
        try:schedule=await app.state.metadata_repository.historical_schedule(exchange,symbol,first,last)
        except Exception as exc:raise HTTPException(503,"historical calendar metadata unavailable") from exc
        if not schedule["calendar"] or (interval != "1d" and not schedule["sessions"]):
            raise HTTPException(409,{"code":"HISTORICAL_SCHEDULE_UNKNOWN",
                                     "instrument_id":symbol,"start_day":start_day,"end_day":end_day})
        expected=ExpectedBarGenerator().generate(interval,start,end,schedule["calendar"],schedule["sessions"])
        observations=await app.state.repository.load_historical_bars(exchange,instrument,interval,start_day,end_day,None)
        mode=SelectionMode.EXPLICIT if provider else SelectionMode(selection.upper())
        try:
            return app.state.historical_selector.select(mode,symbol,interval,start,end,expected,observations,
                                                        provider=provider,require_complete=require_complete)
        except HistoricalIncompleteError as exc:
            app.state.api_metrics.historical_incomplete_queries_total+=1
            raise HTTPException(409,{"code":"HISTORICAL_INCOMPLETE",**exc.details}) from exc

    @app.get("/v1/bars/{symbol}")
    async def bars(symbol:str,interval:str=Query("1m",pattern=r"^(1m|5m|1h|1d)$"),start_day:str|None=Query(None,pattern=r"^[0-9]{8}$"),end_day:str|None=Query(None,pattern=r"^[0-9]{8}$"),provider:str|None=Query(None,pattern=r"^[A-Za-z][A-Za-z0-9_-]{1,31}$"),selection:str|None=Query(None,pattern=r"^(single|composite)$"),require_complete:bool=False):
        try:validate_symbol(symbol)
        except ValueError as exc:raise HTTPException(422,str(exc)) from exc
        exchange,instrument=symbol.split(".",1)
        try:
            if provider and selection is None and not require_complete:
                return await app.state.repository.load_historical_bars(exchange,instrument,interval,start_day,end_day,provider)
            if provider or selection:
                app.state.api_metrics.historical_query_total+=1
                result=await selected_history(symbol,interval,start_day,end_day,provider,selection or "single",require_complete)
                app.state.api_metrics.historical_selection_total+=1
                if not result["bars"]:app.state.api_metrics.historical_selection_no_data_total+=1
                app.state.api_metrics.historical_composite_fallback_total+=sum(
                    bar.get("selection_reason")=="FALLBACK" for bar in result["bars"])
                return result
            table=await run_in_threadpool(bar_loader,config.archive_root,exchange,instrument,interval,start_day=start_day,end_day=end_day)
            return table.to_pylist()
        except FileNotFoundError as exc:raise HTTPException(404,"archive not found") from exc
        except ValueError as exc:raise HTTPException(422,str(exc)) from exc

    @app.get("/v1/historical-coverage")
    async def historical_coverage(instrument:str,interval:str=Query("1m",pattern=r"^(1m|5m|1h|1d)$"),start_day:str=Query(pattern=r"^[0-9]{8}$"),end_day:str=Query(pattern=r"^[0-9]{8}$"),provider:str|None=Query(None,pattern=r"^[A-Za-z][A-Za-z0-9_-]{1,31}$")):
        try:validate_symbol(instrument)
        except ValueError as exc:raise HTTPException(422,str(exc)) from exc
        app.state.api_metrics.historical_coverage_queries_total+=1
        result=await selected_history(instrument,interval,start_day,end_day,provider,"single",False)
        return {"instrument_id":instrument,"interval":interval,"providers":result["providers"]}

    @app.post("/v1/history/ensure",status_code=202)
    async def ensure_history(body:HistoricalEnsureBody):
        coordinator=app.state.fetch_coordinator
        if coordinator is None:raise HTTPException(503,"historical acquisition queue unavailable")
        result=await coordinator.ensure_history(HistoricalEnsureRequest(body.instrument,body.interval,
          body.start,body.end,body.preferred_provider,body.reason,body.force,AcquisitionMode.ON_DEMAND))
        app.state.api_metrics.historical_fetch_requests_total+=1
        if result.status in {"ALREADY_RUNNING"}:app.state.api_metrics.historical_fetch_deduplicated_total+=1
        if result.status=="COOLDOWN":app.state.api_metrics.historical_fetch_cooldown_total+=1
        if result.status=="ALREADY_COMPLETE":app.state.api_metrics.historical_fetch_skipped_complete_total+=1
        return result

    @app.get("/v1/history/requests/{request_id}")
    async def history_request_status(request_id:str):
        try:uuid.UUID(request_id)
        except ValueError as exc:raise HTTPException(422,"invalid request id") from exc
        coordinator=app.state.fetch_coordinator
        if coordinator is None:raise HTTPException(503,"historical acquisition queue unavailable")
        value=await coordinator.queue.status(request_id)
        if value is None:raise HTTPException(404,"historical fetch request not found")
        return value

    @app.websocket("/v1/stream/quotes")
    async def quote_stream(websocket:WebSocket):
        manager:WebSocketManager=app.state.websocket_manager
        try:client=await manager.connect(websocket)
        except RuntimeError:
            await websocket.close(code=1013);return
        try:
            while True:
                try:request=SubscriptionRequest.model_validate(await websocket.receive_json())
                except ValidationError as exc:
                    await manager.send_control(client,{"type":"error","schema_version":1,"code":"INVALID_REQUEST","details":exc.errors(include_url=False)});continue
                if request.action=="subscribe":
                    await manager.subscribe(client,request.symbols)
                    for symbol in request.symbols:
                        exchange,instrument=symbol.split(".",1);tick=await app.state.cache.lookup(exchange,instrument)
                        if tick is not None:await manager.enqueue(client,tick)
                elif request.action=="unsubscribe":await manager.unsubscribe(client,request.symbols)
                else:await manager.set_subscribe_all(client)
                await manager.send_control(client,{"type":"subscription_ack","schema_version":1,"action":request.action,"symbols":request.symbols})
        except (WebSocketDisconnect,RuntimeError):pass
        finally:await manager.disconnect(client)
    return app

app=create_app()
