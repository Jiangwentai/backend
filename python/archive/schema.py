import pyarrow as pa

FIELDS=[("event_ts",pa.timestamp("us",tz="UTC")),("recv_ts",pa.timestamp("ns",tz="UTC")),("producer_id",pa.string()),("seq",pa.uint64()),("exchange",pa.string()),("instrument",pa.string()),("trading_day",pa.string()),("action_day",pa.string()),("last_price",pa.float64()),("volume",pa.int64()),("turnover",pa.float64()),("open_interest",pa.float64()),("upper_limit_price",pa.float64()),("lower_limit_price",pa.float64())]
for level in range(1,6):FIELDS.extend([(f"bid_price{level}",pa.float64()),(f"bid_volume{level}",pa.int32()),(f"ask_price{level}",pa.float64()),(f"ask_volume{level}",pa.int32())])
MARKET_DATA_SCHEMA=pa.schema(FIELDS,metadata={b"schema_version":b"1",b"source":b"ctp_market_data"})
COLUMN_NAMES=MARKET_DATA_SCHEMA.names
