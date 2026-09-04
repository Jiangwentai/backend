-- Phase 0 only: prove PostgreSQL availability. Reference metadata is Phase 5.
CREATE TABLE IF NOT EXISTS schema_version (
  component text PRIMARY KEY,
  version integer NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO schema_version(component, version) VALUES ('bootstrap', 1)
ON CONFLICT (component) DO UPDATE SET version = EXCLUDED.version, applied_at = now();

