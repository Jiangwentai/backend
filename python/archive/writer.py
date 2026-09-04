from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime,timezone
import json,os,re
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from .schema import COLUMN_NAMES,MARKET_DATA_SCHEMA

SAFE=re.compile(r"^[A-Za-z0-9_-]{1,32}$")
@dataclass(frozen=True)
class ArchiveResult: path:Path;row_count:int;verified:bool

def partition_path(root:Path,exchange:str,instrument:str,trading_day:str)->Path:
    if not SAFE.fullmatch(exchange) or not SAFE.fullmatch(instrument) or not re.fullmatch(r"[0-9]{8}",trading_day):raise ValueError("unsafe archive partition")
    day=f"{trading_day[:4]}-{trading_day[4:6]}-{trading_day[6:]}"
    return root/"ctp"/f"exchange={exchange}"/f"instrument={instrument}"/f"trading_day={day}"

def _summary(table:pa.Table)->dict:
    events=table.column("event_ts");instruments=sorted(set(table.column("instrument").to_pylist()))
    return {"schema_version":1,"row_count":table.num_rows,"min_event_ts":None if not table.num_rows else events[0].as_py().isoformat(),"max_event_ts":None if not table.num_rows else events[-1].as_py().isoformat(),"instruments":instruments}

def verify_archive(path:Path)->dict:
    manifest=json.loads((path/"_SUCCESS.json").read_text());parquet_file=pq.ParquetFile(path/"part-0000.parquet");table=parquet_file.read(columns=["event_ts","instrument"]);actual=_summary(table)
    if actual!=manifest["verification"]:raise RuntimeError("Parquet verification mismatch")
    if parquet_file.metadata.row_group(0).column(0).compression!="ZSTD":raise RuntimeError("archive is not ZSTD compressed")
    return manifest

def archive_partition(source,root:Path,exchange:str,instrument:str,trading_day:str)->ArchiveResult:
    destination=partition_path(root,exchange,instrument,trading_day);success=destination/"_SUCCESS.json"
    if success.exists():
        manifest=verify_archive(destination);return ArchiveResult(destination,manifest["verification"]["row_count"],True)
    batches=source.iter_partition(exchange,instrument,trading_day) if hasattr(source,"iter_partition") else [source.fetch_partition(exchange,instrument,trading_day)]
    destination.mkdir(parents=True,exist_ok=True)
    parquet_tmp=destination/"part-0000.parquet.tmp";parquet=destination/"part-0000.parquet";manifest_tmp=destination/"_SUCCESS.json.tmp"
    try:
        writer=pq.ParquetWriter(parquet_tmp,MARKET_DATA_SCHEMA,compression="zstd",use_dictionary=["producer_id","exchange","instrument","trading_day","action_day"]);row_count=0;min_event=None;max_event=None;instruments=set()
        try:
            for rows in batches:
                if not rows:continue
                table=pa.Table.from_pylist([{name:row.get(name) for name in COLUMN_NAMES} for row in rows],schema=MARKET_DATA_SCHEMA);writer.write_table(table);row_count+=table.num_rows;events=table.column("event_ts");min_event=min_event or events[0].as_py();max_event=events[-1].as_py();instruments.update(table.column("instrument").to_pylist())
        finally:writer.close()
        if not row_count:raise ValueError("source partition is empty")
        os.replace(parquet_tmp,parquet);verification={"schema_version":1,"row_count":row_count,"min_event_ts":min_event.isoformat(),"max_event_ts":max_event.isoformat(),"instruments":sorted(instruments)};manifest={"partition":{"exchange":exchange,"instrument":instrument,"trading_day":trading_day},"verification":verification,"created_at":datetime.now(timezone.utc).isoformat(),"format":"parquet","compression":"zstd"}
        manifest_tmp.write_text(json.dumps(manifest,sort_keys=True,indent=2)+"\n");os.replace(manifest_tmp,success);verify_archive(destination)
        return ArchiveResult(destination,row_count,True)
    except Exception:
        parquet_tmp.unlink(missing_ok=True);manifest_tmp.unlink(missing_ok=True)
        if not success.exists():parquet.unlink(missing_ok=True)
        raise
