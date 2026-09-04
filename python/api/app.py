from __future__ import annotations
import asyncio
import time
from contextlib import asynccontextmanager
from typing import Any
from fastapi import FastAPI,HTTPException,Query,Request,WebSocket,WebSocketDisconnect
from fastapi.responses import PlainTextResponse
from starlette.concurrency import run_in_threadpool
from pydantic import ValidationError
from live.cache import LatestQuoteCache
from live.subscriber import LiveSubscriber
from .health import calculate_health
from .metrics import ApiMetrics,render_metrics
from research import load_bars
from .models import BarResponse,HealthResponse,InstrumentResponse,QuoteResponse,SubscriptionRequest,quote_from_tick,validate_symbol
from .postgres_repository import PostgresMetadataRepository
from .questdb_repository import QuestDBQuoteRepository
from .recovery import reconcile_recovery
from .settings import Settings
from .websocket_manager import WebSocketManager

def create_app(*,settings:Settings|None=None,repository:Any|None=None,metadata_repository:Any|None=None,bar_loader:Any=load_bars,start_subscriber:bool=True)->FastAPI:
    config=settings or Settings.from_env()
    @asynccontextmanager
    async def lifespan(app:FastAPI):
        cache=LatestQuoteCache();manager=WebSocketManager(config.websocket_queue_capacity)
        repo=repository or QuestDBQuoteRepository(config.questdb_http_url,config.recovery_timeout_seconds)
        metadata=metadata_repository or PostgresMetadataRepository(config.postgres_dsn,config.postgres_timeout_seconds)
        subscriber=LiveSubscriber(config.zmq_endpoint,cache,manager.publish)
        app.state.cache=cache;app.state.websocket_manager=manager;app.state.repository=repo;app.state.metadata_repository=metadata;app.state.subscriber=subscriber
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

    @app.get("/v1/quotes/{symbol}",response_model=QuoteResponse)
    async def quote(symbol:str,provider:str|None=Query(None,pattern=r"^(ctp|synthetic|ibkr|akshare)$")):
        try:validate_symbol(symbol)
        except ValueError as exc:raise HTTPException(422,str(exc)) from exc
        exchange,instrument=symbol.split(".",1);tick=await app.state.cache.lookup(exchange,instrument,provider)
        if tick is None:raise HTTPException(404,"quote not found")
        return quote_from_tick(tick)

    @app.get("/v1/quotes",response_model=list[QuoteResponse])
    async def quotes():
        snapshot=await app.state.cache.snapshot();return [quote_from_tick(tick) for _,tick in sorted(snapshot.items())]

    @app.get("/v1/bars/{symbol}",response_model=list[BarResponse])
    async def bars(symbol:str,interval:str=Query("1m",pattern=r"^(1m|5m|1h|1d)$"),start_day:str|None=Query(None,pattern=r"^[0-9]{8}$"),end_day:str|None=Query(None,pattern=r"^[0-9]{8}$")):
        try:validate_symbol(symbol)
        except ValueError as exc:raise HTTPException(422,str(exc)) from exc
        exchange,instrument=symbol.split(".",1)
        try:
            table=await run_in_threadpool(bar_loader,config.archive_root,exchange,instrument,interval,start_day=start_day,end_day=end_day)
            return table.to_pylist()
        except FileNotFoundError as exc:raise HTTPException(404,"archive not found") from exc
        except ValueError as exc:raise HTTPException(422,str(exc)) from exc

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
