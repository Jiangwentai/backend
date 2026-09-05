BEGIN;

CREATE TABLE IF NOT EXISTS historical_fetch_requests (
  id uuid PRIMARY KEY,
  provider_code text NOT NULL REFERENCES providers(code),
  instrument_id text NOT NULL,
  interval text NOT NULL CHECK (interval IN ('1m','5m','1h','1d')),
  range_start timestamptz NOT NULL,
  range_end timestamptz NOT NULL,
  trigger text NOT NULL CHECK (trigger IN ('MANUAL','SCHEDULED','ON_DEMAND')),
  reason text NOT NULL,
  status text NOT NULL CHECK (status IN ('QUEUED','RUNNING','SUCCESS','PARTIAL','FAILED','SKIPPED','COOLDOWN')),
  force boolean NOT NULL DEFAULT false,
  requested_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz,
  completed_at timestamptz,
  last_attempt_at timestamptz,
  rows_received bigint NOT NULL DEFAULT 0,
  rows_written bigint NOT NULL DEFAULT 0,
  coverage_before jsonb,
  coverage_after jsonb,
  result_metadata jsonb,
  error_code text,
  error_message text,
  CHECK (range_start < range_end)
);
CREATE INDEX IF NOT EXISTS historical_fetch_requests_claim_idx
  ON historical_fetch_requests(status,requested_at);
CREATE INDEX IF NOT EXISTS historical_fetch_requests_overlap_idx
  ON historical_fetch_requests(provider_code,instrument_id,interval,range_start,range_end)
  WHERE status IN ('QUEUED','RUNNING');

CREATE TABLE IF NOT EXISTS historical_provider_refresh_state (
  provider_code text NOT NULL REFERENCES providers(code),
  interval text NOT NULL,
  last_attempt_at timestamptz,
  last_success_at timestamptz,
  next_allowed_at timestamptz,
  consecutive_failures integer NOT NULL DEFAULT 0,
  last_error_code text,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(provider_code,interval)
);

CREATE TABLE IF NOT EXISTS historical_instrument_access (
  instrument_id text NOT NULL,
  interval text NOT NULL,
  last_requested_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(instrument_id,interval)
);

INSERT INTO schema_version(component,version) VALUES ('historical_acquisition',1)
ON CONFLICT(component) DO UPDATE SET version=EXCLUDED.version,applied_at=now();

COMMIT;
