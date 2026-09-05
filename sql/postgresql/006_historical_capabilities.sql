BEGIN;

ALTER TABLE historical_fetch_requests
  ADD COLUMN IF NOT EXISTS provider_source text NOT NULL DEFAULT 'DEFAULT';

DROP INDEX IF EXISTS historical_fetch_requests_overlap_idx;
CREATE INDEX historical_fetch_requests_overlap_idx
  ON historical_fetch_requests(provider_code,provider_source,instrument_id,interval,range_start,range_end)
  WHERE status IN ('QUEUED','RUNNING');

ALTER TABLE historical_provider_refresh_state
  ADD COLUMN IF NOT EXISTS provider_source text NOT NULL DEFAULT 'DEFAULT';
ALTER TABLE historical_provider_refresh_state
  DROP CONSTRAINT IF EXISTS historical_provider_refresh_state_pkey;
ALTER TABLE historical_provider_refresh_state
  ADD PRIMARY KEY(provider_code,provider_source,interval);

INSERT INTO schema_version(component,version) VALUES ('historical_capabilities',1)
ON CONFLICT(component) DO UPDATE SET version=EXCLUDED.version,applied_at=now();

COMMIT;
