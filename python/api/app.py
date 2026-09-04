from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
from typing import Any
from fastapi import FastAPI,HTTPException,WebSocket,WebSocketDisconnect
from pydantic import ValidationError
from live.cache import LatestQuoteCache
from live.subscriber import LiveSubscriber
from .health import calculate_health
from .models import HealthResponse,QuoteResponse,SubscriptionRequest,quote_from_tick,validate_symbol
from .questdb_repository import QuestDBQuoteRepository
from .recovery import reconcile_recovery
from .settings import Settings
from .websocket_manager import WebSocketManager

def create_app(*,settings:Settings|None=None,repository:Any|None=None,start_subscriber:bool=True)->FastAPI:
    config=settings or Settings.from_env()
    @asynccontextmanager
    async def lifespan(app:FastAPI):
        cache=LatestQuoteCache();manager=WebSocketManager(config.websocket_queue_capacity)
        repo=repository or QuestDBQuoteRepository(config.questdb_http_url,config.recovery_timeout_seconds)
        subscriber=LiveSubscriber(config.zmq_endpoint,cache,manager.publish)
        app.state.cache=cache;app.state.websocket_manager=manager;app.state.repository=repo;app.state.subscriber=subscriber
        app.state.ready=False;app.state.questdb_healthy=None;subscriber_task=None
        try:
            if start_subscriber:
                subscriber_task=asyncio.create_task(subscriber.run());await asyncio.wait_for(subscriber.ready.wait(),config.recovery_timeout_seconds)
            try:
                recovered=await asyncio.wait_for(repo.load_latest_quotes(),config.recovery_timeout_seconds)
                await reconcile_recovery(cache,recovered)
                app.state.questdb_healthy=True
            except Exception:app.state.questdb_healthy=False
            app.state.ready=True
            yield
        finally:
            app.state.ready=False;manager.accepting=False
            subscriber.stop()
            if subscriber_task:
                await asyncio.gather(subscriber_task,return_exceptions=True)
            await manager.shutdown()
            await repo.close()
    app=FastAPI(title="Financial Market Data API",version="1.0.0",lifespan=lifespan)

    @app.get("/health",response_model=HealthResponse)
    async def health():
        sub=app.state.subscriber;metrics=app.state.websocket_manager.metrics
        return calculate_health(ready=app.state.ready,zeromq_healthy=sub.healthy if start_subscriber else True,last_message_monotonic=sub.last_message_monotonic,questdb_healthy=app.state.questdb_healthy,websocket_clients=metrics.websocket_clients,stale_after=config.live_stale_after_seconds)

    @app.get("/v1/quotes/{symbol}",response_model=QuoteResponse)
    async def quote(symbol:str):
        try:validate_symbol(symbol)
        except ValueError as exc:raise HTTPException(422,str(exc)) from exc
        exchange,instrument=symbol.split(".",1);tick=await app.state.cache.lookup(exchange,instrument)
        if tick is None:raise HTTPException(404,"quote not found")
        return quote_from_tick(tick)

    @app.get("/v1/quotes",response_model=list[QuoteResponse])
    async def quotes():
        snapshot=await app.state.cache.snapshot();return [quote_from_tick(tick) for _,tick in sorted(snapshot.items())]

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
