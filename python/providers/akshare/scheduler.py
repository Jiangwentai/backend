from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Awaitable, Callable


@dataclass(frozen=True)
class ScheduledJob:
    name: str
    interval_seconds: int
    operation: Callable[[], Awaitable[object]]


class DatasetScheduler:
    def __init__(self, jobs: list[ScheduledJob]):
        if any(job.interval_seconds < 1 for job in jobs): raise ValueError("job interval must be positive")
        self.jobs = jobs; self._stop = asyncio.Event()
    async def run(self) -> None:
        async def loop(job):
            while not self._stop.is_set():
                await job.operation()
                try: await asyncio.wait_for(self._stop.wait(), job.interval_seconds)
                except TimeoutError: pass
        await asyncio.gather(*(loop(job) for job in self.jobs))
    def stop(self) -> None: self._stop.set()


class BackfillState:
    def __init__(self, path: str | Path): self.path = Path(path)
    def load(self) -> dict:
        return json.loads(self.path.read_text()) if self.path.exists() else {"completed": [], "failed": {}}
    def save(self, state: dict) -> None:
        self.path.parent.mkdir(parents=True,exist_ok=True); temporary=self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps({**state,"updated_at":datetime.now(timezone.utc).isoformat()},sort_keys=True,indent=2)+"\n")
        temporary.replace(self.path)


async def backfill(instruments: list[str], operation: Callable[[str], Awaitable[object]], state_store: BackfillState) -> dict:
    state=state_store.load(); completed=set(state["completed"])
    for instrument in instruments:
        if instrument in completed: continue
        try:
            await operation(instrument); completed.add(instrument); state["failed"].pop(instrument,None)
        except Exception as exc: state["failed"][instrument]=f"{type(exc).__name__}: {exc}"
        state["completed"]=sorted(completed); state_store.save(state)
    return state
