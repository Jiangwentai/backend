from .acquisition import HistoricalRefreshPolicy


def parse_refresh_policies(value:str)->tuple[HistoricalRefreshPolicy,...]:
    result=[]
    for item in value.split(","):
        fields=item.strip().split(":")
        if len(fields) not in {10,12}:raise ValueError("historical refresh policy requires 10 or 12 fields")
        provider,interval,priority,cooldown,stale,recent,immutable,concurrency,arbitrary,bounded=fields[:10]
        source=fields[10] if len(fields)==12 else "SINA" if provider.upper()=="AKSHARE" else "DEFAULT"
        markets=tuple(fields[11].upper().split("|")) if len(fields)==12 else (
          ("DOMESTIC",) if provider.upper()=="AKSHARE" and interval=="1m" else ("DOMESTIC","FOREIGN"))
        result.append(HistoricalRefreshPolicy(provider.upper(),interval,
          min_refresh_interval_seconds=int(cooldown),stale_after_seconds=int(stale),recent_refresh_days=int(recent),
          immutable_after_days=int(immutable),max_concurrency=int(concurrency),supports_arbitrary_range=arbitrary.lower()=="true",
          bounded_recent_days=int(bounded) or None,acquisition_priority=int(priority),upstream_source=source.upper(),
          supported_markets=markets))
    return tuple(result)
