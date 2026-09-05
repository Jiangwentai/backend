from collections import Counter, defaultdict
from dataclasses import asdict

from .coverage import CoverageEngine, _utc
from .models import HistoricalIncompleteError, HistoricalProviderPolicy, SelectionMode

QUALITY_ORDER = {name: score for score, name in enumerate((
    "UNKNOWN", "DERIVED", "BEST_EFFORT", "PUBLIC", "BROKER", "EXCHANGE_DIRECT", "AUTHORITATIVE"))}


class HistoricalSelector:
    def __init__(self, policies: tuple[HistoricalProviderPolicy, ...], minimum_coverage: float = .95):
        if not 0 <= minimum_coverage <= 1:
            raise ValueError("minimum historical coverage must be in [0,1]")
        self.policies = {policy.provider.upper(): policy for policy in policies}
        self.minimum_coverage = minimum_coverage
        self.coverage = CoverageEngine()

    def _eligibility(self, provider, instrument_id, interval):
        policy = self.policies.get(provider)
        if policy is None:return False,"PROVIDER_NOT_CONFIGURED"
        if not policy.enabled:return False,"PROVIDER_DISABLED"
        return policy.eligibility(instrument_id,interval)

    def select(self, mode: SelectionMode, instrument_id: str, interval: str, start, end,
               expected: tuple, bars: list[dict], *, provider: str | None = None,
               require_complete: bool = False) -> dict:
        grouped = defaultdict(list)
        for bar in bars:
            grouped[str(bar["provider"]).upper()].append(bar)
        coverage = {name: self.coverage.calculate(name, instrument_id, interval, start, end, expected, rows)
                    for name, rows in grouped.items()}
        for name in self.policies:
            coverage.setdefault(name, self.coverage.calculate(name, instrument_id, interval, start, end, expected, []))
        if mode == SelectionMode.EXPLICIT:
            selected = (provider or "").upper()
            result = sorted(grouped.get(selected, []), key=lambda bar: _utc(bar["bar_start"]))
            selected_coverage = coverage.get(selected) or self.coverage.calculate(selected, instrument_id, interval, start, end, expected, [])
            return self._result(mode, result, selected_coverage, {selected: len(result)} if result else {}, coverage,
                                selected_provider=selected, require_complete=require_complete)
        if mode == SelectionMode.SINGLE:
            candidates = [(name, value) for name, value in coverage.items()
                          if self._eligibility(name,instrument_id,interval)[0] and value.coverage_ratio >= self.minimum_coverage]
            if not candidates:
                candidates = [(name, value) for name, value in coverage.items() if self._eligibility(name,instrument_id,interval)[0]]
            candidates.sort(key=lambda item: (-self.policies[item[0]].priority,
                -QUALITY_ORDER[self.policies[item[0]].quality.value], -item[1].coverage_ratio, item[0]))
            selected = candidates[0][0] if candidates else None
            selected_coverage = coverage[selected] if selected else self.coverage.calculate("", instrument_id, interval, start, end, expected, [])
            result = sorted(grouped.get(selected, []), key=lambda bar: _utc(bar["bar_start"])) if selected else []
            return self._result(mode, result, selected_coverage, {selected: len(result)} if selected and result else {}, coverage,
                                selected_provider=selected, require_complete=require_complete)
        ranked = sorted((policy for policy in self.policies.values() if policy.enabled and policy.eligibility(instrument_id,interval)[0]),
                        key=lambda policy: (-policy.priority, -QUALITY_ORDER[policy.quality.value], policy.provider))
        by_time = defaultdict(dict)
        for name, rows in grouped.items():
            for bar in rows:
                by_time[_utc(bar["bar_start"])][name] = bar
        result, contributions = [], Counter()
        for timestamp in expected:
            for policy in ranked:
                bar = by_time.get(timestamp, {}).get(policy.provider.upper())
                if bar is not None:
                    value = dict(bar); value["selection_reason"] = "PRIMARY" if policy == ranked[0] else "FALLBACK"
                    result.append(value); contributions[policy.provider.upper()] += 1; break
        combined = self.coverage.calculate("COMPOSITE", instrument_id, interval, start, end, expected, result)
        return self._result(mode, result, combined, dict(contributions), coverage,
                            selected_provider=None, require_complete=require_complete)

    def _result(self, mode, bars, selected, contributions, coverage, *, selected_provider, require_complete):
        value = {"selection_mode": mode.value, "selected_provider": selected_provider,
                 "coverage_ratio": selected.coverage_ratio, "expected_bars": selected.expected_bars,
                 "observed_bars": selected.observed_bars, "missing_bars": selected.missing_bars,
                 "complete": selected.status.value == "COMPLETE", "providers_used": contributions,
                 "bars": bars, "providers": [{**asdict(item),
                    "priority":self.policies[name].priority if name in self.policies else None,
                    "quality":self.policies[name].quality.value if name in self.policies else "UNKNOWN",
                    "enabled":self.policies[name].enabled if name in self.policies else False,
                    "eligible":self._eligibility(name,item.instrument_id,item.interval)[0],
                    "ineligible_reason":self._eligibility(name,item.instrument_id,item.interval)[1]}
                    for name,item in sorted(coverage.items())]}
        if require_complete and not value["complete"]:
            raise HistoricalIncompleteError({key: value[key] for key in (
                "selection_mode", "selected_provider", "coverage_ratio", "expected_bars", "observed_bars", "missing_bars")})
        return value
