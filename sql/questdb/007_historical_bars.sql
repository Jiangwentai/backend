CREATE TABLE IF NOT EXISTS historical_bars (
    bar_start TIMESTAMP,
    provider SYMBOL,
    instrument_id SYMBOL,
    interval SYMBOL,
    upstream_source SYMBOL,
    exchange SYMBOL,
    provider_symbol SYMBOL,
    trading_day SYMBOL,
    source SYMBOL,
    fetch_id SYMBOL,
    fetched_at TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume LONG,
    open_interest LONG,
    turnover DOUBLE,
    settlement DOUBLE
) TIMESTAMP(bar_start) PARTITION BY YEAR WAL
DEDUP UPSERT KEYS(bar_start,provider,instrument_id,interval);
