from __future__ import annotations

from datetime import datetime,timezone

from live.selection import ProviderSelector,SelectionPolicy


def quote(provider:str,quality:str,price:float,age:float=0)->dict:
    now=datetime.now(timezone.utc).timestamp()
    return {"provider":provider,"quality":quality,"last_price":price,"recv_ts":int((now-age)*1_000_000_000)}


def test_explicit_mode_never_silently_selects_between_providers():
    selector=ProviderSelector(SelectionPolicy())
    result=selector.select([quote("ctp","REALTIME",100),quote("AKSHARE","BEST_EFFORT",101)])
    assert result.quote is None and result.reason=="EXPLICIT_PROVIDER_REQUIRED"


def test_preferred_policy_fails_over_only_when_explicitly_enabled():
    values=[quote("ctp","REALTIME",100,age=10),quote("AKSHARE","BEST_EFFORT",101)]
    disabled=ProviderSelector(SelectionPolicy(mode="preferred",fallback_enabled=False))
    assert disabled.select(values).reason=="FALLBACK_DISABLED"
    enabled=ProviderSelector(SelectionPolicy(mode="preferred",fallback_enabled=True))
    result=enabled.select(values)
    assert result.quote["provider"]=="AKSHARE" and result.fallback and result.reason=="FAILOVER"


def test_ranked_selection_prefers_quality_and_reports_discrepancy():
    selector=ProviderSelector(SelectionPolicy(mode="ranked",fallback_enabled=True,discrepancy_bps=20))
    values=[quote("ctp","REALTIME",100),quote("AKSHARE","BEST_EFFORT",101)]
    result=selector.select(values);diagnostic=selector.diagnose(values)
    assert result.quote["provider"]=="ctp"
    assert diagnostic["discrepancy"] and diagnostic["discrepancy_bps"]==100


def test_no_fresh_provider_is_visible_and_stale_can_be_opted_in():
    values=[quote("ctp","REALTIME",100,age=30)]
    strict=ProviderSelector(SelectionPolicy(mode="preferred"))
    assert strict.select(values).reason=="NO_FRESH_PROVIDER"
    multiple=[*values,quote("AKSHARE","BEST_EFFORT",101,age=30)]
    assert strict.select(multiple).reason=="NO_FRESH_PROVIDER"
    permissive=ProviderSelector(SelectionPolicy(mode="preferred",allow_stale=True,fallback_enabled=True))
    assert permissive.select(multiple).quote is not None
