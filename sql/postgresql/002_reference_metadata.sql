BEGIN;

CREATE TABLE IF NOT EXISTS exchanges (
  code text PRIMARY KEY CHECK (code ~ '^[A-Z][A-Z0-9_]{1,15}$'),
  name text NOT NULL,
  timezone text NOT NULL DEFAULT 'Asia/Shanghai',
  country_code char(2) NOT NULL DEFAULT 'CN',
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS products (
  exchange_code text NOT NULL REFERENCES exchanges(code),
  code text NOT NULL,
  name text NOT NULL,
  asset_class text NOT NULL DEFAULT 'futures',
  currency char(3) NOT NULL DEFAULT 'CNY',
  contract_multiplier numeric(24,8) CHECK (contract_multiplier > 0),
  price_tick numeric(24,8) CHECK (price_tick > 0),
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (exchange_code, code)
);

CREATE TABLE IF NOT EXISTS futures_contracts (
  exchange_code text NOT NULL,
  instrument_id text NOT NULL,
  product_code text NOT NULL,
  delivery_month char(6) CHECK (delivery_month ~ '^[0-9]{6}$'),
  listed_date date,
  last_trading_date date,
  delivery_date date,
  status text NOT NULL DEFAULT 'active' CHECK (status IN ('prelisted','active','expired','suspended')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (exchange_code, instrument_id),
  FOREIGN KEY (exchange_code, product_code) REFERENCES products(exchange_code, code),
  CHECK (last_trading_date IS NULL OR listed_date IS NULL OR last_trading_date >= listed_date),
  CHECK (delivery_date IS NULL OR last_trading_date IS NULL OR delivery_date >= last_trading_date)
);
CREATE INDEX IF NOT EXISTS futures_contracts_product_idx ON futures_contracts(exchange_code, product_code, status);

CREATE TABLE IF NOT EXISTS trading_calendar (
  exchange_code text NOT NULL REFERENCES exchanges(code),
  trading_day date NOT NULL,
  is_trading_day boolean NOT NULL,
  night_session_open date,
  note text,
  PRIMARY KEY (exchange_code, trading_day)
);

CREATE TABLE IF NOT EXISTS trading_sessions (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  exchange_code text NOT NULL REFERENCES exchanges(code),
  product_code text,
  name text NOT NULL,
  session_order smallint NOT NULL CHECK (session_order >= 0),
  start_time time NOT NULL,
  end_time time NOT NULL,
  crosses_midnight boolean NOT NULL DEFAULT false,
  effective_from date NOT NULL,
  effective_to date,
  FOREIGN KEY (exchange_code, product_code) REFERENCES products(exchange_code, code),
  CHECK (effective_to IS NULL OR effective_to >= effective_from),
  UNIQUE (exchange_code, product_code, effective_from, session_order)
);
CREATE INDEX IF NOT EXISTS trading_sessions_lookup_idx ON trading_sessions(exchange_code, product_code, effective_from, effective_to);

CREATE TABLE IF NOT EXISTS roll_rules (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  exchange_code text NOT NULL,
  product_code text NOT NULL,
  name text NOT NULL,
  rule jsonb NOT NULL CHECK (jsonb_typeof(rule) = 'object'),
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (exchange_code, product_code) REFERENCES products(exchange_code, code),
  UNIQUE (exchange_code, product_code, name)
);

CREATE TABLE IF NOT EXISTS continuous_contract_mapping (
  continuous_symbol text NOT NULL,
  trading_day date NOT NULL,
  exchange_code text NOT NULL,
  product_code text NOT NULL,
  instrument_id text NOT NULL,
  roll_rule_id bigint REFERENCES roll_rules(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (continuous_symbol, trading_day),
  FOREIGN KEY (exchange_code, product_code) REFERENCES products(exchange_code, code),
  FOREIGN KEY (exchange_code, instrument_id) REFERENCES futures_contracts(exchange_code, instrument_id)
);
CREATE INDEX IF NOT EXISTS continuous_mapping_contract_idx ON continuous_contract_mapping(exchange_code, instrument_id, trading_day);

INSERT INTO schema_version(component, version) VALUES ('reference_metadata', 1)
ON CONFLICT (component) DO UPDATE SET version = EXCLUDED.version, applied_at = now();

COMMIT;
