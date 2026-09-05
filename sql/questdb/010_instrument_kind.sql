-- Additive provenance only; DEDUP keys are unchanged.
ALTER TABLE ctp_market_data ADD COLUMN IF NOT EXISTS instrument_kind SYMBOL;
