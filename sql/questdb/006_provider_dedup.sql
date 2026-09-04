ALTER TABLE ctp_market_data DEDUP ENABLE UPSERT KEYS(event_ts, provider, producer_id, seq);
