from __future__ import annotations

from pathlib import Path
import re

import duckdb
import pyarrow as pa


SAFE=re.compile(r"^[a-z0-9_-]{1,64}$")


def query_raw(root: str | Path, *, dataset: str, start_date: str | None = None,
              end_date: str | None = None) -> pa.Table:
    if not SAFE.fullmatch(dataset): raise ValueError("invalid dataset")
    files=[str(path) for path in (Path(root)/"provider=akshare"/f"dataset={dataset}").glob("fetch_date=*/fetch_id=*/raw.parquet")]
    if not files: raise FileNotFoundError(f"no AKShare raw archives for {dataset}")
    clauses=[];parameters=[]
    if start_date: clauses.append("CAST(_fetched_at AS DATE)>=?");parameters.append(start_date)
    if end_date: clauses.append("CAST(_fetched_at AS DATE)<=?");parameters.append(end_date)
    where=" AND ".join(clauses) or "TRUE"
    with duckdb.connect() as connection:
        connection.from_parquet(files,union_by_name=True,hive_partitioning=False).create_view("raw_akshare")
        return connection.execute(f"SELECT * FROM raw_akshare WHERE {where}",parameters).to_arrow_table()
