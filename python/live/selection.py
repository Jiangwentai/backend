from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal


@dataclass(frozen=True)
class SelectionPolicy:
    mode: Literal["explicit", "preferred", "ranked"] = "explicit"
    preferred_providers: tuple[str, ...] = ("ctp", "ibkr", "synthetic", "akshare")
    fallback_enabled: bool = False
    allow_stale: bool = False
    freshness_seconds: tuple[tuple[str, float], ...] = (("ctp", 5.0), ("ibkr", 5.0), ("synthetic", 5.0), ("akshare", 15.0))
    discrepancy_bps: float = 20.0

    def __post_init__(self) -> None:
        if self.mode not in {"explicit", "preferred", "ranked"}:raise ValueError("invalid provider selection mode")
        if not self.preferred_providers:raise ValueError("provider preference must not be empty")
        if any(seconds <= 0 for _,seconds in self.freshness_seconds):raise ValueError("provider freshness must be positive")
        if self.discrepancy_bps < 0:raise ValueError("discrepancy threshold must be non-negative")


@dataclass(frozen=True)
class SelectionResult:
    quote: dict | None
    reason: str
    fallback: bool = False
    preferred_provider: str | None = None


class ProviderSelector:
    QUALITY_RANK={"REALTIME":4,"EXCHANGE_DIRECT":4,"AUTHORITATIVE":4,"DELAYED":3,"BEST_EFFORT":2,"UNKNOWN":1}

    def __init__(self,policy:SelectionPolicy):self.policy=policy;self._freshness=dict(policy.freshness_seconds)

    def _age(self,quote:dict,now:datetime)->float:
        return max(0.0,now.timestamp()-quote["recv_ts"]/1_000_000_000)

    def assess(self,quotes:list[dict],now:datetime|None=None)->list[dict]:
        now=now or datetime.now(timezone.utc);result=[]
        for raw in quotes:
            quote=dict(raw);provider=str(quote.get("provider","unknown")).lower()
            quote["age_seconds"]=self._age(quote,now)
            quote["stale"]=quote["age_seconds"]>self._freshness.get(provider,5.0)
            result.append(quote)
        return result

    def select(self,quotes:list[dict],now:datetime|None=None)->SelectionResult:
        values=self.assess(quotes,now)
        if not values:return SelectionResult(None,"NO_OBSERVATION")
        if len(values)==1 and self.policy.mode=="explicit":return SelectionResult(values[0],"ONLY_PROVIDER")
        if self.policy.mode=="explicit":return SelectionResult(None,"EXPLICIT_PROVIDER_REQUIRED")
        eligible=[value for value in values if not value["stale"]]
        if not eligible and self.policy.allow_stale:
            eligible=sorted(values,key=lambda value:value["recv_ts"],reverse=True)
        if not eligible:return SelectionResult(None,"NO_FRESH_PROVIDER")
        preference={provider.lower():index for index,provider in enumerate(self.policy.preferred_providers)}
        primary=self.policy.preferred_providers[0].lower()
        if self.policy.mode=="preferred":
            eligible.sort(key=lambda value:(preference.get(str(value["provider"]).lower(),len(preference)),-value["recv_ts"]))
        else:
            eligible.sort(key=lambda value:(-self.QUALITY_RANK.get(str(value.get("quality","UNKNOWN")).upper(),0),
                                             preference.get(str(value["provider"]).lower(),len(preference)),-value["recv_ts"]))
        selected=eligible[0];provider=str(selected["provider"]).lower();fallback=provider!=primary
        if fallback and not self.policy.fallback_enabled:return SelectionResult(None,"FALLBACK_DISABLED",False,primary)
        reason="QUALITY_RANK" if self.policy.mode=="ranked" else "PROVIDER_PREFERENCE"
        if fallback:reason="FAILOVER"
        return SelectionResult(selected,reason,fallback,primary)

    def diagnose(self,quotes:list[dict],now:datetime|None=None)->dict:
        values=self.assess(quotes,now);fresh=[value for value in values if not value["stale"] and value.get("last_price") not in (None,0)]
        discrepancy=0.0
        if len(fresh)>=2:
            prices=[float(value["last_price"]) for value in fresh];reference=min(prices)
            discrepancy=(max(prices)-reference)/reference*10_000 if reference else 0.0
        selection=self.select(quotes,now)
        return {"mode":self.policy.mode,"selected_provider":selection.quote.get("provider") if selection.quote else None,
                "reason":selection.reason,"fallback":selection.fallback,"discrepancy_bps":discrepancy,
                "discrepancy":discrepancy>self.policy.discrepancy_bps,
                "observations":[{"provider":value.get("provider"),"quality":value.get("quality"),
                  "last_price":value.get("last_price"),"age_seconds":value["age_seconds"],"stale":value["stale"]} for value in values]}
