from __future__ import annotations

import json


class PostgresAcquisitionRepository:
    def __init__(self,pool_factory):self._pool_factory=pool_factory
    def _db(self):return self._pool_factory()

    async def active_covering(self,provider,source,instrument,interval,start,end):
        return await self._db().fetchval("""SELECT id FROM historical_fetch_requests
          WHERE provider_code=$1 AND provider_source=$2 AND instrument_id=$3 AND interval=$4
          AND status IN ('QUEUED','RUNNING') AND range_start<=$5 AND range_end>=$6
          ORDER BY requested_at LIMIT 1""",provider.lower(),source,instrument,interval,start,end)

    async def refresh_state(self,provider,source,interval):
        row=await self._db().fetchrow("""SELECT last_attempt_at,last_success_at,next_allowed_at,
          consecutive_failures,last_error_code FROM historical_provider_refresh_state
          WHERE provider_code=$1 AND provider_source=$2 AND interval=$3""",provider.lower(),source,interval)
        return dict(row) if row else None

    async def enqueue(self,request_id,provider,source,request,start,end,coverage):
        async with self._db().acquire() as connection:
            async with connection.transaction():
                await connection.execute("SELECT pg_advisory_xact_lock(hashtext($1))",
                    f"{provider.lower()}:{source}:{request.instrument_id}:{request.interval}")
                active=await connection.fetchval("""SELECT id FROM historical_fetch_requests
                  WHERE provider_code=$1 AND provider_source=$2 AND instrument_id=$3 AND interval=$4
                  AND status IN ('QUEUED','RUNNING') AND range_start<=$5 AND range_end>=$6 LIMIT 1""",
                  provider.lower(),source,request.instrument_id,request.interval,start,end)
                if active:return False
                await connection.execute("""INSERT INTO historical_fetch_requests
                  (id,provider_code,provider_source,instrument_id,interval,range_start,range_end,trigger,reason,status,force,coverage_before)
                  VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'QUEUED',$10,$11::jsonb)""",request_id,provider.lower(),source,
                  request.instrument_id,request.interval,start,end,request.trigger.value,request.reason,request.force,
                  json.dumps(coverage,default=str))
                await connection.execute("""INSERT INTO historical_instrument_access(instrument_id,interval)
                  VALUES($1,$2) ON CONFLICT(instrument_id,interval) DO UPDATE SET last_requested_at=now()""",
                  request.instrument_id,request.interval)
                return True

    async def claim(self,limits):
        async with self._db().acquire() as connection:
            async with connection.transaction():
                rows=await connection.fetch("""SELECT * FROM historical_fetch_requests
                  WHERE status='QUEUED' ORDER BY requested_at FOR UPDATE SKIP LOCKED LIMIT 32""")
                for row in rows:
                    provider=row["provider_code"]
                    await connection.execute("SELECT pg_advisory_xact_lock(hashtext($1))",f"historical-worker:{provider}")
                    running=await connection.fetchval("""SELECT count(*) FROM historical_fetch_requests
                      WHERE provider_code=$1 AND status='RUNNING'""",provider)
                    if running>=limits.get(provider.upper(),limits.get(provider,1)):continue
                    claimed=await connection.fetchrow("""UPDATE historical_fetch_requests SET status='RUNNING',
                      started_at=now(),last_attempt_at=now() WHERE id=$1 RETURNING *""",row["id"])
                    return dict(claimed)
        return None

    async def complete(self,request_id,status,rows_received,rows_written,coverage,error_code=None,error_message=None,result_metadata=None):
        await self._db().execute("""UPDATE historical_fetch_requests SET status=$2,completed_at=now(),
          rows_received=$3,rows_written=$4,coverage_after=$5::jsonb,error_code=$6,error_message=$7,
          result_metadata=$8::jsonb WHERE id=$1""",
          request_id,status,rows_received,rows_written,json.dumps(coverage,default=str) if coverage else None,
          error_code,(error_message or '')[:2000] or None,json.dumps(result_metadata,default=str) if result_metadata else None)

    async def record_success(self,provider,source,interval,when,next_allowed):
        await self._db().execute("""INSERT INTO historical_provider_refresh_state
          (provider_code,provider_source,interval,last_attempt_at,last_success_at,next_allowed_at,consecutive_failures,last_error_code)
          VALUES($1,$2,$3,$4,$4,$5,0,NULL) ON CONFLICT(provider_code,provider_source,interval) DO UPDATE SET
          last_attempt_at=$4,last_success_at=$4,next_allowed_at=$5,consecutive_failures=0,last_error_code=NULL,updated_at=now()""",
          provider.lower(),source,interval,when,next_allowed)

    async def record_failure(self,provider,source,interval,when,next_allowed,error_code):
        await self._db().execute("""INSERT INTO historical_provider_refresh_state
          (provider_code,provider_source,interval,last_attempt_at,next_allowed_at,consecutive_failures,last_error_code)
          VALUES($1,$2,$3,$4,$5,1,$6) ON CONFLICT(provider_code,provider_source,interval) DO UPDATE SET
          last_attempt_at=$4,next_allowed_at=$5,consecutive_failures=historical_provider_refresh_state.consecutive_failures+1,
          last_error_code=$6,updated_at=now()""",provider.lower(),source,interval,when,next_allowed,error_code)

    async def status(self,request_id):
        row=await self._db().fetchrow("SELECT * FROM historical_fetch_requests WHERE id=$1",request_id)
        return dict(row) if row else None

    async def scheduled_universe(self,pinned,accessed_after):
        rows=await self._db().fetch("""SELECT a.instrument_id,a.interval FROM historical_instrument_access a
          LEFT JOIN futures_contracts c ON c.exchange_code=split_part(a.instrument_id,'.',1)
            AND c.instrument_id=substring(a.instrument_id from position('.' in a.instrument_id)+1)
          WHERE a.last_requested_at>=$1 AND (c.instrument_id IS NULL OR c.status<>'expired')
          ORDER BY a.last_requested_at DESC""",accessed_after)
        return sorted(set(pinned)|{row["instrument_id"] for row in rows})
