from zoneinfo import ZoneInfo

from api.postgres_repository import PostgresMetadataRepository
from api.questdb_repository import QuestDBQuoteRepository
from historical import HistoricalFetchCoordinator, HistoricalFetchWorker
from historical.config import parse_refresh_policies
from instruments.registry import DOMESTIC_EXCHANGES

from .models import HistoricalBarRequest


class AkshareHistoricalExecutor:
    def __init__(self,service,metadata):self.service=service;self.metadata=metadata
    async def __call__(self,job):
        market_tz=ZoneInfo("Asia/Shanghai")
        start_day=job["range_start"].astimezone(market_tz).date()
        end_day=job["range_end"].astimezone(market_tz).date()
        source=job.get("provider_source","SINA")
        source="EASTMONEY" if "EASTMONEY" in source else "SINA"
        exchange,_,native=await self.metadata.provider_symbol(job["instrument_id"],end_day,source)
        if job["interval"]=="1m":
            if exchange not in DOMESTIC_EXCHANGES:raise ValueError("UNSUPPORTED_RANGE: AKShare foreign 1m endpoint unavailable")
            endpoint="futures_1m_sina"
        elif job["interval"]=="1d":
            endpoint=("futures_daily_sina" if exchange in DOMESTIC_EXCHANGES else
                      "futures_foreign_daily_eastmoney" if source=="EASTMONEY" else "futures_foreign_daily_sina")
        else:raise ValueError("UNSUPPORTED_RANGE: AKShare supports acquisition for 1m/1d")
        batch,written,_=await self.service.ingest_bars(HistoricalBarRequest(native,
          start_day,end_day,endpoint,exchange,
          job["trigger"],str(job["id"])))
        return {"rows_received":batch.rows_received,"rows_written":written,
                "actual_start":batch.rows[0].bar_start if batch.rows else None,
                "actual_end":batch.rows[-1].bar_start if batch.rows else None,
                "provider_limitation":batch.lineage.get("known_limit")}


async def build_acquisition(service,akshare_metadata,dsn,qdb_url,policy_text,fallback=False):
    metadata=PostgresMetadataRepository(dsn);await metadata.start()
    history=QuestDBQuoteRepository(qdb_url)
    policies=parse_refresh_policies(policy_text)
    coordinator=HistoricalFetchCoordinator(metadata.acquisition,history,metadata,policies,
                                             acquisition_fallback=fallback)
    limits={policy.provider:policy.max_concurrency for policy in policies}
    worker=HistoricalFetchWorker(coordinator,AkshareHistoricalExecutor(service,akshare_metadata),limits)
    return coordinator,worker,metadata,history
