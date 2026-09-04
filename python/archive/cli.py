from __future__ import annotations
import argparse,os
from pathlib import Path
from .questdb_source import QuestDBArchiveSource
from .writer import archive_partition

def main()->int:
    parser=argparse.ArgumentParser(description="Archive one immutable QuestDB market-data partition")
    parser.add_argument("--questdb-url",default=os.getenv("QDB_HTTP_URL","http://127.0.0.1:9000"));parser.add_argument("--archive-root",default=os.getenv("ARCHIVE_ROOT","./data/market"));parser.add_argument("--exchange",required=True);parser.add_argument("--instrument",required=True);parser.add_argument("--trading-day",required=True)
    args=parser.parse_args();source=QuestDBArchiveSource(args.questdb_url)
    try:result=archive_partition(source,Path(args.archive_root),args.exchange,args.instrument,args.trading_day);print(f"archived rows={result.row_count} path={result.path} verified={result.verified}");return 0
    finally:source.close()
if __name__=="__main__":raise SystemExit(main())
