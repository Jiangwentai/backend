from __future__ import annotations

import argparse
import asyncio
from datetime import date,datetime,timedelta,timezone
from dataclasses import asdict
from instruments import ProviderInstrumentResolver, ExplicitMapping, InstrumentKind
from instruments.metadata import MemoryInstrumentMetadata
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
from .acquisition import build_acquisition
from historical import AcquisitionMode,HistoricalEnsureRequest


def parser() -> argparse.ArgumentParser:
    root=argparse.ArgumentParser(prog="historical-worker"); sub=root.add_subparsers(dest="command",required=True)
    sub.add_parser("health"); sub.add_parser("metrics"); sub.add_parser("list-datasets"); sub.add_parser("unresolved-symbols");sub.add_parser("schedule")
    fetch=sub.add_parser("fetch"); fetch.add_argument("dataset",help="futures-daily, futures-1m, futures-foreign-daily, or legacy raw symbol");fetch.add_argument("--instrument");fetch.add_argument("--exchange");fetch.add_argument("--start");fetch.add_argument("--end");fetch.add_argument("--source",choices=("sina","eastmoney"),default="sina")
    back=sub.add_parser("backfill");back.add_argument("dataset",choices=("futures-daily","futures-1m"));back.add_argument("--instrument",action="append",required=True);back.add_argument("--start");back.add_argument("--end");back.add_argument("--state",default="/raw/backfill-state.json")
    quote=sub.add_parser("quote");quote.add_argument("instrument")
    sub.add_parser("serve-quotes")
    reference=sub.add_parser("refresh-reference")
    reference.add_argument("--dataset",choices=("contracts","foreign-products"),default="contracts")
    resolve=sub.add_parser("resolve-instrument")
    resolve.add_argument("symbol");resolve.add_argument("--exchange")
    reverse=sub.add_parser("provider-symbol");reverse.add_argument("instrument")
    audit=sub.add_parser("audit-instruments")
    listing=sub.add_parser("list-instruments")
    for command in (resolve,reverse,audit,listing):
        command.add_argument("--json",action="store_true")
        command.add_argument("--as-of",type=date.fromisoformat)
    for command in (resolve,reverse):
        command.add_argument("--offline",action="store_true",help="deterministic rules only; explicit database mappings are not checked")
    add=sub.add_parser("add-instrument")
    add.add_argument("instrument");add.add_argument("--provider-symbol",required=True)
    add.add_argument("--json",action="store_true")
    ensure=sub.add_parser("ensure");ensure.add_argument("--instrument",required=True);ensure.add_argument("--interval",choices=("1m","1d"),required=True);ensure.add_argument("--start",type=datetime.fromisoformat,required=True);ensure.add_argument("--end",type=datetime.fromisoformat,required=True);ensure.add_argument("--provider");ensure.add_argument("--force",action="store_true")
    scheduled=sub.add_parser("run-scheduled-refresh");scheduled.add_argument("--interval",choices=("1m","1d"),default="1m")
    worker=sub.add_parser("run-fetch-worker");worker.add_argument("--once",action="store_true")
    status=sub.add_parser("fetch-status");status.add_argument("request_id")
    return root


def _service():
    dsn=os.environ.get("POSTGRES_DSN"); qdb=os.environ.get("QDB_HTTP_URL")
    if not dsn or not qdb: raise RuntimeError("POSTGRES_DSN and QDB_HTTP_URL are required")
    metadata=AkshareMetadataRepository(dsn); client=AkshareClient(
        max_concurrency=int(os.getenv("AKSHARE_MAX_CONCURRENCY","2")),
        min_interval_ms=int(os.getenv("AKSHARE_MIN_INTERVAL_MS","500")),
        max_attempts=int(os.getenv("AKSHARE_MAX_ATTEMPTS","3")))
    provider=AkshareProvider(client,metadata.instrument_resolver,RawArchiveRepository(os.getenv("AKSHARE_RAW_ROOT","/raw")))
    return AkshareIngestionService(provider,metadata,QuestDbHistoricalBarRepository(qdb)),metadata


def _print_identity(value, json_output):
    if json_output:
        print(json.dumps(value, default=str))
    elif isinstance(value, dict):
        for name, item in value.items():
            print(f"{name}: {item}")
    else:
        print(json.dumps(value, default=str, indent=2))


async def _identity_command(args):
    dsn = os.getenv("POSTGRES_DSN")
    offline = getattr(args, "offline", False) or not dsn
    if offline and args.command not in {"resolve-instrument", "provider-symbol"}:
        raise RuntimeError("POSTGRES_DSN is required for mapping administration")
    metadata = None if offline else AkshareMetadataRepository(dsn)
    resolver = ProviderInstrumentResolver(MemoryInstrumentMetadata(registration_known=False)) if offline else metadata.instrument_resolver
    if metadata:
        await metadata.start()
    try:
        if args.command == "resolve-instrument":
            result = await resolver.resolve_raw("akshare", args.symbol, exchange_hint=args.exchange, as_of=args.as_of)
            _print_identity({**asdict(result), "mapping_source": "offline_rules_only" if offline else "database_and_rules"}, args.json)
            return 0 if result.resolved else 2
        if args.command == "provider-symbol":
            result = await resolver.format_provider_symbol("akshare", args.instrument, as_of=args.as_of)
            _print_identity({**asdict(result), "mapping_source": "offline_rules_only" if offline else "database_and_rules"}, args.json)
            return 0 if result.resolved else 2
        if args.command == "list-instruments":
            rows = await metadata.instrument_metadata.list_explicit_mappings("akshare", args.as_of or date.today())
            _print_identity({"scope": "explicit provider mappings", "mappings": [asdict(row) for row in rows]}, args.json)
            return 0
        if args.command == "audit-instruments":
            rows = await resolver.audit_mappings("akshare", as_of=args.as_of)
            _print_identity({"scope": "explicit provider mappings", "audit": rows}, args.json)
            return 2 if any(row["status"] == "CONFLICT" for row in rows) else 0
        # Resolve the canonical target through a formatter and parser without
        # existing raw aliases so a new override can point to a different month.
        target_resolver = ProviderInstrumentResolver(aliases=await resolver._aliases("akshare"))
        native = await target_resolver.format_provider_symbol("akshare", args.instrument)
        if not native.resolved:
            raise MappingError(f"INVALID_MAPPING_TARGET: {native.reason}")
        target = await target_resolver.resolve_raw("akshare", native.provider_symbol,
            exchange_hint=args.instrument.partition(".")[0])
        mapping = ExplicitMapping("akshare", args.provider_symbol, target.canonical_instrument, target.kind,
                                  target.product, target.delivery_month, target.tenor)
        status = await metadata.instrument_metadata.add_explicit_mapping(mapping)
        _print_identity({"status": status, "scope": "explicit provider mapping", **asdict(mapping)}, args.json)
        return 0
    finally:
        if metadata:
            await metadata.close()


async def run(args) -> int:
    if args.command in {"resolve-instrument","provider-symbol","list-instruments","audit-instruments","add-instrument"}:
        return await _identity_command(args)
    if args.command=="list-datasets":
        print(json.dumps([{"name":value.name,"function":value.function_name,"type":value.dataset_type,
            "source":value.upstream_source,"stability":value.stability,"enabled":value.enabled} for value in ENDPOINTS.values()],default=str));return 0
    service,metadata=_service();await metadata.start()
    try:
        async def acquisition():
            return await build_acquisition(service,metadata,os.environ["POSTGRES_DSN"],os.environ["QDB_HTTP_URL"],
              os.getenv("HISTORICAL_REFRESH_POLICY","akshare:1m:50:300:300:3:7:1:false:30:SINA_DOMESTIC:DOMESTIC,akshare:1d:50:3600:86400:3:7:1:true:0:SINA:DOMESTIC|FOREIGN"),
              os.getenv("HISTORICAL_ACQUISITION_FALLBACK","false").lower() in {"1","true","yes"})
        if args.command in {"ensure","run-scheduled-refresh","run-fetch-worker","fetch-status"}:
            coordinator,worker,coordinator_metadata,history=await acquisition()
            try:
                if args.command=="ensure":
                    result=await coordinator.ensure_history(HistoricalEnsureRequest(args.instrument,args.interval,
                      args.start if args.start.tzinfo else args.start.replace(tzinfo=timezone.utc),
                      args.end if args.end.tzinfo else args.end.replace(tzinfo=timezone.utc),args.provider,"MANUAL",args.force,AcquisitionMode.MANUAL))
                    print(json.dumps(asdict(result),default=str));return 0 if result.status not in {"NO_ELIGIBLE_PROVIDER","UNKNOWN_FRESHNESS"} else 2
                if args.command=="fetch-status":
                    value=await coordinator.queue.status(args.request_id);print(json.dumps(value,default=str));return 0 if value else 2
                if args.command=="run-scheduled-refresh":
                    pinned=[value.strip() for value in os.getenv("HISTORICAL_REFRESH_INSTRUMENTS",os.getenv("AKSHARE_SYMBOLS","")).split(",") if value.strip()]
                    symbols=await coordinator.queue.scheduled_universe(pinned,datetime.now(timezone.utc)-timedelta(days=int(os.getenv("HISTORICAL_RECENT_ACCESS_DAYS","30"))))
                    result=await coordinator.schedule_refresh(symbols,args.interval);print(json.dumps(result,default=str));return 0 if result["status"]=="SUCCESS" else 2
                if args.once:
                    result=await worker.run_once();print(json.dumps({"status":result or "IDLE"}));return 0
                stop=asyncio.Event();loop=asyncio.get_running_loop()
                for name in (signal.SIGINT,signal.SIGTERM):loop.add_signal_handler(name,stop.set)
                while not stop.is_set():
                    if await worker.run_once() is None:
                        try:await asyncio.wait_for(stop.wait(),1)
                        except TimeoutError:pass
                return 0
            finally:await history.close();await coordinator_metadata.close()
        if args.command=="health":
            print(json.dumps({"provider":"AKSHARE","state":service.provider.health.state,
                              "version":service.provider.client.version},default=str));return 0
        if args.command=="metrics":print(render_metrics(service.provider.client.metrics),end="");return 0
        if args.command=="unresolved-symbols": print(json.dumps(await metadata.list_unresolved(),default=str));return 0
        if args.command=="refresh-reference":
            request = ReferenceDataRequest("futures_foreign_products", {}) if args.dataset == "foreign-products" else ReferenceDataRequest()
            batch,written=await service.ingest_reference(request);print(json.dumps({"fetch_id":batch.fetch_id,"written":written}));return 0
        if args.command=="schedule":
            pinned=[value.strip() for value in os.getenv("HISTORICAL_REFRESH_INSTRUMENTS",os.getenv("AKSHARE_SYMBOLS","")).split(",") if value.strip()]
            coordinator,_,coordinator_metadata,history=await acquisition()
            async def refresh():
                symbols=await coordinator.queue.scheduled_universe(pinned,datetime.now(timezone.utc)-timedelta(days=int(os.getenv("HISTORICAL_RECENT_ACCESS_DAYS","30"))))
                await coordinator.schedule_refresh(symbols,"1m" if os.getenv("AKSHARE_INTRADAY_REFRESH_ENABLED","false").lower() in {"1","true","yes"} else "1d")
            scheduler=DatasetScheduler([ScheduledJob("historical-refresh",int(os.getenv("AKSHARE_DAILY_INTERVAL_SECONDS","86400")),refresh)])
            try:await scheduler.run();return 0
            finally:await history.close();await coordinator_metadata.close()
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
                batch_size=int(os.getenv("AKSHARE_QUOTE_BATCH_SIZE","20")),resolver=metadata.instrument_resolver)
            if args.command=="quote":
                emitted=await poller.poll_once();print(json.dumps({"producer_id":poller.producer_id,"emitted":emitted}));await ingress.close();return 0
            loop=asyncio.get_running_loop()
            for name in (signal.SIGINT,signal.SIGTERM):loop.add_signal_handler(name,poller.stop)
            try:await poller.run()
            finally:await ingress.close()
            return 0
        async def request(instrument):
            source=("EASTMONEY" if args.dataset=="futures-foreign-daily" and args.source=="eastmoney" else "SINA")
            exchange,canonical,symbol=await metadata.provider_symbol(instrument, date.fromisoformat(args.end or args.start) if args.end or args.start else None,source)
            endpoint_name=("futures_1m_sina" if args.dataset=="futures-1m" else
                           ("futures_foreign_daily_eastmoney" if source=="EASTMONEY" else "futures_foreign_daily_sina") if args.dataset=="futures-foreign-daily" else
                           "futures_daily_sina")
            return HistoricalBarRequest(symbol,date.fromisoformat(args.start) if args.start else None,
                date.fromisoformat(args.end) if args.end else None,endpoint_name,exchange)
        if args.command=="fetch":
            if args.start and args.end and args.start>args.end:raise ValueError("start must be <= end")
            if args.dataset in {"futures-daily", "futures-1m", "futures-foreign-daily"}:
                if not args.instrument:raise ValueError("--instrument is required for the selected dataset")
                bar_request = await request(args.instrument)
            else:
                if args.instrument:raise ValueError("legacy raw-symbol fetch cannot also specify --instrument")
                bar_request = HistoricalBarRequest(args.dataset,
                    date.fromisoformat(args.start) if args.start else None,
                    date.fromisoformat(args.end) if args.end else None,exchange=args.exchange)
            batch,written,revisions=await service.ingest_bars(bar_request);print(json.dumps({"fetch_id":batch.fetch_id,"written":written,"revisions":revisions,"unresolved":batch.unresolved_symbols,"coverage_complete":batch.lineage.get("coverage_complete",True)}));return 0
        async def ingest(instrument):return await service.ingest_bars(await request(instrument))
        state=await backfill(args.instrument,ingest,BackfillState(Path(args.state)))
        print(json.dumps(state));return 0 if not state["failed"] else 2
    finally: await metadata.close()


def main(argv=None) -> int:
    try:return asyncio.run(run(parser().parse_args(argv)))
    except Exception as exc: print(json.dumps({"status":"FAILED","error":f"{type(exc).__name__}: {exc}"}));return 1


if __name__=="__main__": raise SystemExit(main())
