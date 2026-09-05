-- Additive provider-observation quality; historical DEDUP keys are unchanged.
ALTER TABLE historical_bars ADD COLUMN IF NOT EXISTS quality SYMBOL;
