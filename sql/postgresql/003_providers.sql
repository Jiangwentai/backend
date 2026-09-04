BEGIN;

CREATE TABLE IF NOT EXISTS providers (
  code text PRIMARY KEY CHECK (code ~ '^[a-z][a-z0-9_]{1,31}$'),
  name text NOT NULL,
  enabled boolean NOT NULL DEFAULT false,
  provider_type text NOT NULL CHECK (provider_type IN ('realtime','historical','reference','hybrid')),
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO providers(code,name,enabled,provider_type) VALUES
  ('ctp','CTP',true,'realtime'),
  ('synthetic','Synthetic',true,'realtime'),
  ('ibkr','Interactive Brokers',false,'hybrid'),
  ('akshare','AKShare',false,'hybrid')
ON CONFLICT (code) DO NOTHING;

CREATE TABLE IF NOT EXISTS provider_instruments (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  provider_code text NOT NULL REFERENCES providers(code),
  exchange_code text NOT NULL,
  instrument_id text NOT NULL,
  provider_symbol text NOT NULL,
  provider_instrument_id text,
  valid_from date,
  valid_to date,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (exchange_code,instrument_id) REFERENCES futures_contracts(exchange_code,instrument_id),
  CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from),
  UNIQUE (provider_code,provider_symbol,valid_from),
  UNIQUE (provider_code,exchange_code,instrument_id,valid_from)
);
CREATE INDEX IF NOT EXISTS provider_instruments_canonical_idx ON provider_instruments(exchange_code,instrument_id,provider_code);

INSERT INTO schema_version(component,version) VALUES ('providers',1)
ON CONFLICT (component) DO UPDATE SET version=EXCLUDED.version,applied_at=now();

COMMIT;
