BEGIN;

CREATE TABLE IF NOT EXISTS provider_ingestion_runs (
  id uuid PRIMARY KEY,
  provider_code text NOT NULL REFERENCES providers(code),
  dataset text NOT NULL,
  endpoint text NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  status text NOT NULL CHECK (status IN ('RUNNING','SUCCESS','PARTIAL','FAILED')),
  request_parameters jsonb NOT NULL DEFAULT '{}'::jsonb,
  rows_received bigint NOT NULL DEFAULT 0 CHECK (rows_received >= 0),
  rows_normalized bigint NOT NULL DEFAULT 0 CHECK (rows_normalized >= 0),
  rows_rejected bigint NOT NULL DEFAULT 0 CHECK (rows_rejected >= 0),
  rows_written bigint NOT NULL DEFAULT 0 CHECK (rows_written >= 0),
  error_code text,
  error_message text,
  provider_version text NOT NULL,
  schema_version integer NOT NULL
);
CREATE INDEX IF NOT EXISTS provider_ingestion_runs_lookup_idx
  ON provider_ingestion_runs(provider_code,dataset,started_at DESC);

CREATE TABLE IF NOT EXISTS provider_unresolved_instruments (
  provider_code text NOT NULL REFERENCES providers(code),
  fetch_id uuid NOT NULL REFERENCES provider_ingestion_runs(id),
  endpoint text NOT NULL,
  raw_provider_symbol text NOT NULL,
  normalized_provider_symbol text NOT NULL,
  first_seen_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz,
  resolution_note text,
  PRIMARY KEY(provider_code,fetch_id,raw_provider_symbol)
);

CREATE TABLE IF NOT EXISTS provider_reference_records (
  provider_code text NOT NULL REFERENCES providers(code),
  dataset text NOT NULL,
  provider_key text NOT NULL,
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload)='object'),
  source text NOT NULL,
  upstream_source text,
  fetch_id uuid NOT NULL REFERENCES provider_ingestion_runs(id),
  fetched_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(provider_code,dataset,provider_key)
);

CREATE TABLE IF NOT EXISTS historical_bar_versions (
  provider_code text NOT NULL REFERENCES providers(code),
  instrument_id text NOT NULL,
  interval text NOT NULL,
  bar_start timestamptz NOT NULL,
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload)='object'),
  fetch_id uuid NOT NULL REFERENCES provider_ingestion_runs(id),
  fetched_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(provider_code,instrument_id,interval,bar_start)
);

CREATE TABLE IF NOT EXISTS historical_bar_revisions (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  provider_code text NOT NULL REFERENCES providers(code),
  instrument_id text NOT NULL,
  interval text NOT NULL,
  bar_start timestamptz NOT NULL,
  previous_payload jsonb NOT NULL,
  new_payload jsonb NOT NULL,
  previous_fetch_id uuid NOT NULL,
  new_fetch_id uuid NOT NULL,
  detected_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS historical_bar_revisions_lookup_idx
  ON historical_bar_revisions(provider_code,instrument_id,interval,bar_start);

INSERT INTO schema_version(component,version) VALUES ('akshare_provider',1)
ON CONFLICT(component) DO UPDATE SET version=EXCLUDED.version,applied_at=now();

COMMIT;
