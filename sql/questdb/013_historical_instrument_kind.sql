-- Additive provenance only; DEDUP keys are unchanged.
ALTER TABLE historical_bars ADD COLUMN IF NOT EXISTS instrument_kind SYMBOL;
