ALTER TABLE historical_bars DEDUP ENABLE UPSERT KEYS(bar_start,provider,upstream_source,instrument_id,interval);
