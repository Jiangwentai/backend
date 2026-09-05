-- Additive provenance only; DEDUP keys are unchanged.
ALTER TABLE historical_bars ADD COLUMN IF NOT EXISTS raw_provider_symbol VARCHAR;
