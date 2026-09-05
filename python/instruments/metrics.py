from collections import Counter
from dataclasses import asdict
import logging

from .kinds import InstrumentKind

logger = logging.getLogger(__name__)


class ResolutionMetrics:
    """Process-local counters. Kind is an enum; no unbounded provider/symbol labels."""
    NAMES = ("instrument_resolution_total", "instrument_resolution_explicit_total",
             "instrument_resolution_normalized_mapping_total", "instrument_resolution_parser_total",
             "instrument_resolution_unresolved_total", "instrument_resolution_conflicts_total",
             "instrument_metadata_missing_total")

    def __init__(self):
        self.counts = Counter({name: 0 for name in self.NAMES})
        self.kinds = Counter({kind.value: 0 for kind in InstrumentKind})

    def record(self, result, exchange_hint=None):
        self.counts["instrument_resolution_total"] += 1
        self.kinds[result.kind.value] += 1
        if not result.resolved:
            self.counts["instrument_resolution_unresolved_total"] += 1
        elif result.method == "EXPLICIT_MAPPING":
            self.counts["instrument_resolution_explicit_total"] += 1
        elif result.method == "NORMALIZED_EXPLICIT_MAPPING":
            self.counts["instrument_resolution_normalized_mapping_total"] += 1
        else:
            self.counts["instrument_resolution_parser_total"] += 1
        if result.conflict:
            self.counts["instrument_resolution_conflicts_total"] += 1
        if result.resolved and result.metadata_registered is False:
            self.counts["instrument_metadata_missing_total"] += 1
        if result.conflict or not result.resolved:
            logger.warning("instrument_resolution", extra={"resolution": asdict(result), "exchange_hint": exchange_hint,
                           "error_code": result.reason, "provider_mapping_parser_conflict": result.conflict})

    def render(self):
        lines = [f"{name} {self.counts[name]}" for name in self.NAMES]
        lines += [f'instrument_kind_total{{kind="{kind}"}} {count}' for kind, count in sorted(self.kinds.items())]
        return "\n".join(lines) + "\n"
