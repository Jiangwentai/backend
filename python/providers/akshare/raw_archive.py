from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


SAFE = re.compile(r"^[a-z0-9_-]{1,64}$")


class RawArchiveRepository:
    def __init__(self, root: str | Path): self.root = Path(root)

    def write(self, *, dataset: str, fetch_id: str, rows: list[dict[str, Any]], lineage: dict[str, Any]) -> Path:
        if not SAFE.fullmatch(dataset) or not re.fullmatch(r"[0-9a-f-]{36}", fetch_id): raise ValueError("unsafe raw archive identity")
        fetched = datetime.fromisoformat(lineage["fetched_at"])
        destination = self.root / "provider=akshare" / f"dataset={dataset}" / f"fetch_date={fetched.date()}" / f"fetch_id={fetch_id}"
        destination.mkdir(parents=True, exist_ok=False)
        enriched = [{**row, "_provider": "AKSHARE", "_fetch_id": fetch_id,
                     "_fetched_at": fetched, "_endpoint": lineage["function_name"],
                     "_upstream_source": lineage.get("upstream_source"),
                     "_akshare_version": lineage["provider_version"],
                     "_request_parameters": json.dumps(lineage["request_parameters"], sort_keys=True)} for row in rows]
        table = pa.Table.from_pylist(enriched)
        parquet = destination / "raw.parquet"; pq.write_table(table, parquet, compression="zstd")
        digest = hashlib.sha256(parquet.read_bytes()).hexdigest()
        manifest = {**lineage, "row_count": len(rows), "sha256": digest, "format": "parquet", "compression": "zstd"}
        (destination / "manifest.json").write_text(json.dumps(manifest, sort_keys=True, indent=2, default=str) + "\n")
        return destination
