BEGIN;

ALTER TABLE historical_bar_versions ADD COLUMN IF NOT EXISTS provider_source text NOT NULL DEFAULT 'unknown';
ALTER TABLE historical_bar_versions DROP CONSTRAINT IF EXISTS historical_bar_versions_pkey;
ALTER TABLE historical_bar_versions ADD PRIMARY KEY(provider_code,provider_source,instrument_id,interval,bar_start);

ALTER TABLE historical_bar_revisions ADD COLUMN IF NOT EXISTS provider_source text NOT NULL DEFAULT 'unknown';
DROP INDEX IF EXISTS historical_bar_revisions_lookup_idx;
CREATE INDEX historical_bar_revisions_lookup_idx
  ON historical_bar_revisions(provider_code,provider_source,instrument_id,interval,bar_start);

INSERT INTO schema_version(component,version) VALUES ('historical_provider_source',1)
ON CONFLICT(component) DO UPDATE SET version=EXCLUDED.version,applied_at=now();

COMMIT;
