from __future__ import annotations
from live.cache import LatestQuoteCache

async def reconcile_recovery(cache:LatestQuoteCache,recovered:list[dict])->int:
    """Merge recovery through the same ordering rules used by the live path."""
    applied=0
    for tick in recovered:
        if await cache.update(tick):applied+=1
    return applied
