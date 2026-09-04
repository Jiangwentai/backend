from pathlib import Path
import pyarrow.parquet as pq
import pytest
from archive.writer import archive_partition,partition_path,verify_archive

def row(seq=1):
    value={"event_ts":1788435000000000+seq,"recv_ts":1788435000000000000+seq,"producer_id":"producer","seq":seq,"exchange":"SHFE","instrument":"zn2610","trading_day":"20260904","action_day":"20260903","last_price":22580.0,"volume":100,"turnover":200.0,"open_interest":300.0,"upper_limit_price":24000.0,"lower_limit_price":21000.0}
    for level in range(1,6):value.update({f"bid_price{level}":22580.0-level,f"bid_volume{level}":level,f"ask_price{level}":22580.0+level,f"ask_volume{level}":level})
    return value
class Source:
    def __init__(self,rows):self.rows=rows;self.fetches=0;self.deleted=[]
    def fetch_partition(self,*_):self.fetches+=1;return self.rows

def test_archive_is_zstd_partitioned_verified_and_idempotent(tmp_path:Path):
    source=Source([row(1),row(2)]);result=archive_partition(source,tmp_path,"SHFE","zn2610","20260904")
    assert result.path==tmp_path/"ctp/exchange=SHFE/instrument=zn2610/trading_day=2026-09-04"
    assert result.row_count==2 and result.verified
    assert verify_archive(result.path)["verification"]["instruments"]==["zn2610"]
    assert pq.ParquetFile(result.path/"part-0000.parquet").metadata.row_group(0).column(0).compression=="ZSTD"
    again=archive_partition(source,tmp_path,"SHFE","zn2610","20260904");assert again.row_count==2 and source.fetches==1

def test_empty_or_unsafe_partition_is_rejected(tmp_path:Path):
    with pytest.raises(ValueError):archive_partition(Source([]),tmp_path,"SHFE","zn2610","20260904")
    with pytest.raises(ValueError):partition_path(tmp_path,"../bad","zn2610","20260904")

def test_manifest_tampering_is_detected(tmp_path:Path):
    result=archive_partition(Source([row()]),tmp_path,"SHFE","zn2610","20260904");manifest=result.path/"_SUCCESS.json";manifest.write_text(manifest.read_text().replace('"row_count": 1','"row_count": 2'))
    with pytest.raises(RuntimeError):verify_archive(result.path)
