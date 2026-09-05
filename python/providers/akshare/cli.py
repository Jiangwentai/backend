from __future__ import annotations

import argparse
import asyncio
from datetime import date
import json
import os
from pathlib import Path
import signal

from live.ingress import LiveEventIngress
from live.publisher import ZmqLivePublisher
from live.persistence import QuestDbLivePersistence
from .client import AkshareClient
from .errors import MappingError
from .ingestion import AkshareIngestionService
from .models import HistoricalBarRequest, QuoteSubscription, ReferenceDataRequest
from .quote_poller import AkshareQuotePoller
from .metrics_export import render as render_metrics
from .provider import AkshareProvider
from .raw_archive import RawArchiveRepository
from .registry import ENDPOINTS
from .repositories import AkshareMetadataRepository, QuestDbHistoricalBarRepository
from .scheduler import BackfillState, DatasetScheduler, ScheduledJob, backfill


def parser() -> argparse.ArgumentParser:
    root=argparse.ArgumentParser(prog="akshare-provider"); sub=root.add_subparsers(dest="command",required=True)
    sub.add_parser("health"); sub.add_parser("metrics"); sub.add_parser("list-datasets"); sub.add_parser("unresolved-symbols");sub.add_parser("schedule")
    fetch=sub.add_parser("fetch"); fetch.add_argument("dataset",choices=("futures-daily","futures-1m"));fetch.add_argument("--instrument",required=True);fetch.add_argument("--start");fetch.add_argument("--end")
    back=sub.add_parser("backfill");back.add_argument("dataset",choices=("futures-daily","futures-1m"));back.add_argument("--instrument",action="append",required=True);back.add_argument("--start");back.add_argument("--end");back.add_argument("--state",default="/raw/backfill-state.json")
    quote=sub.add_parser("quote");quote.add_argument("instrument")
    sub.add_parser("serve-quotes")
    sub.add_parser("refresh-reference")
    return root


def _service():
    dsn=os.environ.get("POSTGRES_DSN"); qdb=os.environ.get("QDB_HTTP_URL")
    if not dsn or not qdb: raise RuntimeError("POSTGRES_DSN and QDB_HTTP_URL are required")
    metadata=AkshareMetadataRepository(dsn); client=AkshareClient(
        max_concurrency=int(os.getenv("AKSHARE_MAX_CONCURRENCY","2")),
        min_interval_ms=int(os.getenv("AKSHARE_MIN_INTERVAL_MS","500")),
        max_attempts=int(os.getenv("AKSHARE_MAX_ATTEMPTS","3")))
    provider=AkshareProvider(client,metadata,RawArchiveRepository(os.getenv("AKSHARE_RAW_ROOT","/raw")))
    return AkshareIngestionService(provider,metadata,QuestDbHistoricalBarRepository(qdb)),metadata


async def run(args) -> int:
    if args.command=="list-datasets":
        print(json.dumps([{"name":value.name,"function":value.function_name,"type":value.dataset_type,
            "source":value.upstream_source,"stability":value.stability,"enabled":value.enabled} for value in ENDPOINTS.values()],default=str));return 0
    service,metadata=_service();await metadata.start()
    try:
        if args.command=="health":
            print(json.dumps({"provider":"AKSHARE","state":service.provider.health.state,
                              "version":service.provider.client.version},default=str));return 0
        if args.command=="metrics":print(render_metrics(service.provider.client.metrics),end="");return 0
        if args.command=="unresolved-symbols": print(json.dumps(await metadata.list_unresolved(),default=str));return 0
        if args.command=="refresh-reference":
            batch,written=await service.ingest_reference(ReferenceDataRequest());print(json.dumps({"fetch_id":batch.fetch_id,"written":written}));return 0
        if args.command=="schedule":
            symbols=[value.strip() for value in os.getenv("AKSHARE_SYMBOLS","").split(",") if value.strip()]
            if not symbols:raise RuntimeError("AKSHARE_SYMBOLS is required for scheduled collection")
            async def collect(endpoint_name):
                for name in symbols:
                    exchange,_,symbol=await metadata.provider_symbol(name)
                    await service.ingest_bars(HistoricalBarRequest(symbol,endpoint=endpoint_name,exchange=exchange))
            async def daily():await collect("futures_daily_sina")
            jobs=[ScheduledJob("daily-futures",int(os.getenv("AKSHARE_DAILY_INTERVAL_SECONDS","86400")),daily)]
            if os.getenv("AKSHARE_INTRADAY_REFRESH_ENABLED","false").lower() in {"1","true","yes"}:
                async def minute():await collect("futures_1m_sina")
                jobs.append(ScheduledJob("minute-futures",int(os.getenv("AKSHARE_INTRADAY_REFRESH_INTERVAL_SECONDS","300")),minute))
            scheduler=DatasetScheduler(jobs)
            await scheduler.run();return 0
        if args.command in {"quote","serve-quotes"}:
            names=[args.instrument] if args.command=="quote" else [value.strip() for value in os.getenv("AKSHARE_QUOTE_INSTRUMENTS","").split(",") if value.strip()]
            if args.command=="serve-quotes" and os.getenv("AKSHARE_REALTIME_ENABLED","false").lower() not in {"1","true","yes"}:
                print(json.dumps({"provider":"AKSHARE","status":"DISABLED"}));return 0
            if not names:raise RuntimeError("AKSHARE_QUOTE_INSTRUMENTS is required")
            subscriptions=[]
            for name in names:
                try:exchange,instrument,symbol=await metadata.provider_symbol(name)
                except MappingError:
                    service.provider.client.metrics.mapping_errors_total+=1
                    service.provider.client.metrics.quote_mapping_errors_total+=1
                    raise
                market="FF" if exchange=="CFFEX" else "CF"
                subscriptions.append(QuoteSubscription(f"{exchange}.{instrument}",symbol,exchange,market))
            transports=[ZmqLivePublisher(os.getenv("AKSHARE_ZMQ_PUB_ENDPOINT","tcp://0.0.0.0:5557"))]
            if os.getenv("AKSHARE_REALTIME_PERSIST","false").lower() in {"1","true","yes"}:
                transports.append(QuestDbLivePersistence(os.environ["QDB_HTTP_URL"]))
            ingress=LiveEventIngress(transports)
            poller=AkshareQuotePoller(service.provider.client,ingress,subscriptions,
                poll_interval_seconds=float(os.getenv("AKSHARE_QUOTE_POLL_INTERVAL_SECONDS","5")),
                request_timeout_seconds=float(os.getenv("AKSHARE_QUOTE_REQUEST_TIMEOUT_SECONDS","10")),
                batch_size=int(os.getenv("AKSHARE_QUOTE_BATCH_SIZE","20")))
            if args.command=="quote":
                emitted=await poller.poll_once();print(json.dumps({"producer_id":poller.producer_id,"emitted":emitted}));await ingress.close();return 0
            loop=asyncio.get_running_loop()
            for name in (signal.SIGINT,signal.SIGTERM):loop.add_signal_handler(name,poller.stop)
            try:await poller.run()
            finally:await ingress.close()
            return 0
        async def request(instrument):
            exchange,canonical,symbol=await metadata.provider_symbol(instrument)
            endpoint_name="futures_1m_sina" if args.dataset=="futures-1m" else "futures_daily_sina"
            return HistoricalBarRequest(symbol,date.fromisoformat(args.start) if args.start else None,
                date.fromisoformat(args.end) if args.end else None,endpoint_name,exchange)
        if args.command=="fetch":
            if args.start and args.end and args.start>args.end:raise ValueError("start must be <= end")
            batch,written,revisions=await service.ingest_bars(await request(args.instrument));print(json.dumps({"fetch_id":batch.fetch_id,"written":written,"revisions":revisions,"unresolved":batch.unresolved_symbols,"coverage_complete":batch.lineage.get("coverage_complete",True)}));return 0
        async def ingest(instrument):return await service.ingest_bars(await request(instrument))
        state=await backfill(args.instrument,ingest,BackfillState(Path(args.state)))
        print(json.dumps(state));return 0 if not state["failed"] else 2
    finally: await metadata.close()


def main(argv=None) -> int:
    try:return asyncio.run(run(parser().parse_args(argv)))
    except Exception as exc: print(json.dumps({"status":"FAILED","error":f"{type(exc).__name__}: {exc}"}));return 1


if __name__=="__main__": raise SystemExit(main())
