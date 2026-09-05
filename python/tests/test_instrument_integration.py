from datetime import date, datetime, timezone
from dataclasses import replace
import json

import pytest

from instruments import ProviderInstrumentResolver, ExplicitMapping, InstrumentKind as Kind
from instruments.metadata import MemoryInstrumentMetadata
from providers.akshare.client import AkshareClient
from providers.akshare.models import QuoteSubscription, HistoricalBarRequest, ReferenceDataRequest
from providers.akshare.normalizers.quote import normalize_quotes
from providers.akshare.registry import endpoint
from providers.akshare.quote_poller import AkshareQuotePoller
from providers.akshare.errors import SchemaError, MappingError
from providers.akshare.cli import main
from live.ingress import LiveEventIngress
from test_akshare_provider import Module, Frame, make_service

NOW = datetime(2026, 9, 5, tzinfo=timezone.utc)
SUBSCRIPTIONS = [QuoteSubscription("SHFE.cu2610", "CU2610", "SHFE"), QuoteSubscription("SHFE.zn2610", "ZN2610", "SHFE")]


def normalize(rows, subscriptions=SUBSCRIPTIONS):
    return normalize_quotes(rows, definition=endpoint("futures_realtime_quote"), subscriptions=subscriptions, recv_at=NOW)


def test_quote_reorder_and_display_names_preserve_identity():
    values = normalize([{"symbol": "沪锌2610", "current_price": 20000}, {"symbol": "cu-2610", "current_price": 80000}])
    assert {row.instrument_id: row.last_price for row in values} == {"SHFE.cu2610": 80000, "SHFE.zn2610": 20000}
    assert values[0].volume is None and values[0].provider_symbol == "CU2610"
    assert values[1].raw_provider_symbol == "沪锌2610"


@pytest.mark.parametrize("rows,reason", [
    ([{"symbol": "CU2610", "current_price": 1}, {"symbol": "cu-2610", "current_price": 2}], "DUPLICATE_QUOTE_SYMBOL"),
    ([{"symbol": "CU2610", "current_price": 1}], "MISSING_QUOTE_SYMBOLS"),
    ([{"symbol": "CU2610", "current_price": 1}, {"symbol": "RB2610", "current_price": 2}], "UNEXPECTED_QUOTE_SYMBOL"),
    ([{"symbol": "DCE.CU2610", "current_price": 1}], "EXCHANGE_HINT_CONFLICT"),
    ([{"symbol": "", "current_price": 1}], "MISSING_QUOTE_SYMBOL"),
    ([{"current_price": 1}], "missing required columns"),
])
def test_quote_identity_errors(rows, reason):
    with pytest.raises(SchemaError, match=reason): normalize(rows)


def test_ambiguous_requested_symbols_rejected():
    with pytest.raises(SchemaError, match="AMBIGUOUS_REQUEST_SYMBOL"):
        normalize([{"symbol": "CU2610", "current_price": 1}], [SUBSCRIPTIONS[0], replace(SUBSCRIPTIONS[0], provider_symbol="cu-2610")])


@pytest.mark.asyncio
async def test_poller_reordering_null_api_and_feed_duplicates(tmp_path):
    class Quotes(Module):
        def futures_zh_spot(self, **kwargs):
            return Frame([{"symbol": "ZN2610", "current_price": 20}, {"symbol": "CU2610", "current_price": 80}])
    class Transport:
        def __init__(self): self.events = []
        async def publish(self, event): self.events.append(event)
    transport = Transport()
    poller = AkshareQuotePoller(AkshareClient(module=Quotes(), min_interval_ms=0), LiveEventIngress([transport]), SUBSCRIPTIONS)
    assert await poller.poll_once() == 2
    assert await poller.poll_once() == 2
    assert [event["seq"] for event in transport.events] == [1, 2, 3, 4]
    assert len({event["producer_id"] for event in transport.events}) == 1
    assert all(event["event_type"] == "quote_snapshot" and event["quality"] == "BEST_EFFORT" for event in transport.events)
    assert transport.events[0]["instrument_id"] == "SHFE.cu2610" and transport.events[0]["last_price"] == 80
    assert transport.events[0]["provider_symbol"] == "CU2610" and transport.events[0]["instrument_kind"] == "PHYSICAL_FUTURE"
    from api.models import quote_from_tick
    value = quote_from_tick(transport.events[0]).model_dump()
    assert value["volume"] is value["turnover"] is value["open_interest"] is None
    assert value["upstream_source"] == "SINA" and value["provider_symbol"] == "CU2610"


@pytest.mark.asyncio
async def test_quote_missing_symbol_fails_whole_batch_without_emitting():
    class Quotes(Module):
        def futures_zh_spot(self, **kwargs): return Frame([{"symbol": "CU2610", "current_price": 80}])
    class Transport:
        async def publish(self, event): pytest.fail("invalid batch must not be published")
    client = AkshareClient(module=Quotes(), min_interval_ms=0)
    poller = AkshareQuotePoller(client, LiveEventIngress([Transport()]), SUBSCRIPTIONS)
    with pytest.raises(SchemaError): await poller.poll_once()
    assert client.metrics.quote_schema_errors_total == 1 and poller.seq == 0


@pytest.mark.asyncio
async def test_quote_explicit_override_is_respected_before_network():
    resolver = ProviderInstrumentResolver(MemoryInstrumentMetadata([ExplicitMapping("akshare", "CU2610", "SHFE.cu2611")]))
    poller = AkshareQuotePoller(AkshareClient(module=Module(), min_interval_ms=0), LiveEventIngress([]), [SUBSCRIPTIONS[0]], resolver=resolver)
    with pytest.raises(MappingError, match="SUBSCRIPTION_IDENTITY_CONFLICT"): await poller.poll_once()
    assert poller.client.metrics.quote_mapping_errors_total == 1 and poller.client.metrics.requests_total == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("dataset", ["futures_daily_sina", "futures_1m_sina"])
async def test_history_deterministic_native_formatting_and_identity(tmp_path, dataset):
    class Recording(Module):
        def futures_zh_daily_sina(self, symbol):
            assert symbol == "RB2610"
            return super().futures_zh_daily_sina(symbol)
        def futures_zh_minute_sina(self, symbol, period):
            assert symbol == "RB2610"
            return super().futures_zh_minute_sina(symbol, period)
    metadata = MemoryInstrumentMetadata()
    service, _, bars = make_service(tmp_path, Recording(), ProviderInstrumentResolver(metadata))
    batch, written, _ = await service.ingest_bars(HistoricalBarRequest("SHFE.Rb-2610", endpoint=dataset))
    assert written == 1 and batch.rows[0].instrument_id == "SHFE.rb2610"
    assert batch.rows[0].raw_provider_symbol == "SHFE.Rb-2610"
    assert batch.rows[0].provider_symbol == "RB2610"
    assert batch.lineage["instrument_resolution"]["metadata_registered"] is False
    assert not metadata.mappings and len(bars.values) == 1


@pytest.mark.asyncio
async def test_lme_foreign_history_routes_canonical_identity_to_sina_alias(tmp_path):
    class Recording(Module):
        def futures_foreign_hist(self, symbol):
            assert symbol == "ZSD"
            return Frame([{"date": "2024-01-02", "open": 1, "high": 3, "low": 1,
                           "close": 2, "volume": 4, "position": 5, "s": 0}])

    service, _, bars = make_service(tmp_path, Recording())
    batch, written, _ = await service.ingest_bars(HistoricalBarRequest(
        "ZSD", endpoint="futures_foreign_daily_sina"))
    assert written == 1 and len(bars.values) == 1
    assert batch.rows[0].instrument_id == "LME.zn.3m"
    assert batch.rows[0].provider_symbol == "ZSD"
    assert batch.rows[0].raw_provider_symbol == "ZSD"
    assert batch.rows[0].instrument_kind == "ROLLING_TENOR"
    assert batch.lineage["upstream_source"] == "SINA"


@pytest.mark.asyncio
async def test_foreign_and_domestic_history_endpoints_reject_cross_routing(tmp_path):
    service, _, _ = make_service(tmp_path)
    with pytest.raises(MappingError, match="foreign Sina endpoint only"):
        await service.provider.fetch_bars(HistoricalBarRequest(
            "RB2610", exchange="SHFE", endpoint="futures_foreign_daily_sina"))
    with pytest.raises(MappingError, match="domestic Sina bar endpoints only"):
        await service.provider.fetch_bars(HistoricalBarRequest("ZSD", endpoint="futures_daily_sina"))


@pytest.mark.asyncio
async def test_history_override_changes_canonical_not_raw_symbol(tmp_path):
    resolver = ProviderInstrumentResolver(MemoryInstrumentMetadata([ExplicitMapping("akshare", "CU2610", "SHFE.cu2611")]))
    service, _, _ = make_service(tmp_path, resolver=resolver)
    batch, _, _ = await service.ingest_bars(HistoricalBarRequest("CU2610", exchange="SHFE"))
    assert batch.rows[0].instrument_id == "SHFE.cu2611" and batch.rows[0].raw_provider_symbol == "CU2610"
    assert service.provider.client.metrics.instrument_resolution.counts["instrument_resolution_conflicts_total"] >= 1


@pytest.mark.asyncio
async def test_foreign_reference_sync_preserves_unknown_and_rolling_semantics(tmp_path):
    class References(Module):
        def futures_hq_subscribe_exchange_symbol(self):
            return Frame([{"symbol": "LME锌3个月", "code": "ZSD"}, {"symbol": "COMEX黄金", "code": "GC"}, {"symbol": "unclassified", "code": "NEW"}])
    service, metadata, _ = make_service(tmp_path, References())
    batch, count = await service.ingest_reference(ReferenceDataRequest("futures_foreign_products", {}))
    assert count == 3 and metadata.references["ZSD"].values["definition"]["tenor"] == "P3M"
    assert metadata.references["GC"].values["instrument_kind"] == "CONTINUOUS_FUTURE"
    assert metadata.references["NEW"].values["definition"] is None
    assert batch.raw_archive


@pytest.mark.asyncio
async def test_foreign_reference_drift_is_not_silently_reclassified(tmp_path):
    class References(Module):
        def futures_hq_subscribe_exchange_symbol(self): return Frame([{"symbol": "something else", "code": "ZSD"}])
    service, _, _ = make_service(tmp_path, References())
    with pytest.raises(SchemaError, match="FOREIGN_REFERENCE_SEMANTICS_CHANGED"):
        await service.ingest_reference(ReferenceDataRequest("futures_foreign_products", {}))


@pytest.mark.parametrize("arguments,canonical", [(["resolve-instrument", "RB2610", "--exchange", "SHFE"], "SHFE.rb2610"),
                                                (["resolve-instrument", "GC25Z"], "COMEX.gc2512"),
                                                (["resolve-instrument", "ZSD"], "LME.zn.3m"),
                                                (["provider-symbol", "SHFE.rb2610"], "SHFE.rb2610")])
def test_offline_cli_needs_neither_sdk_nor_databases(arguments, canonical, monkeypatch, capsys):
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.delenv("QDB_HTTP_URL", raising=False)
    assert main(arguments + ["--json"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["canonical_instrument"] == canonical and result["mapping_source"] == "offline_rules_only"


def test_cli_unresolved_has_nonzero_status(monkeypatch, capsys):
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    assert main(["resolve-instrument", "UNKNOWN123", "--json"]) == 2
    assert json.loads(capsys.readouterr().out)["resolved"] is False


def test_api_accepts_typed_canonical_suffixes_without_loosening_arbitrary_paths():
    from api.models import validate_symbol
    for value in ("LME.zn.3m", "COMEX.gc.continuous", "SHFE.rb2610"):
        assert validate_symbol(value) == value
    for value in ("LME.zn.other", "LME.zn../", "LME.zn.3m'", "LME.zn.3m.more"):
        with pytest.raises(ValueError): validate_symbol(value)


@pytest.mark.asyncio
async def test_historical_api_reads_canonical_and_legacy_ids_without_duplicate_results():
    import httpx
    from api.questdb_repository import QuestDBQuoteRepository
    columns = ["exchange", "instrument", "trading_day", "interval", "bar_start", "open", "high", "low", "close", "volume", "open_interest", "settlement", "provider", "provider_symbol", "source", "upstream_source"]
    legacy = ["SHFE", "rb2610", "2026-09-01", "1d", "2026-08-31T16:00:00Z", 1, 3, 1, 2, None, None, None, "AKSHARE", "RB2610", "test", "SINA"]
    canonical = list(legacy)
    canonical[1], canonical[8] = "SHFE.rb2610", 3
    def handler(request):
        assert "instrument_id IN ('SHFE.rb2610','rb2610')" in request.url.params["query"]
        return httpx.Response(200, json={"columns": [{"name": name} for name in columns], "dataset": [canonical, legacy]})
    client = httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(handler))
    repository = QuestDBQuoteRepository("http://test", client=client)
    try:
        rows = await repository.load_historical_bars("SHFE", "rb2610", "1d")
        assert len(rows) == 1 and rows[0]["close"] == 3 and rows[0]["instrument_id"] == "SHFE.rb2610"
        assert rows[0]["volume"] is None
    finally:
        await repository.close()


def test_continuous_archive_retains_provenance_and_duckdb_reads_it(tmp_path):
    import pyarrow.parquet as pq
    from archive.writer import archive_partition, verify_archive, partition_path
    from research.query import load_ticks
    row = {"event_ts": 1, "recv_ts": 1000, "provider": "AKSHARE", "event_type": "quote_snapshot",
           "instrument_id": "SHFE.rb.continuous", "instrument": "rb.continuous", "exchange": "SHFE",
           "quality": "BEST_EFFORT", "producer_id": "p", "seq": 1, "trading_day": "20260905", "action_day": "20260905",
           "provider_symbol": "RB0", "raw_provider_symbol": "螺纹钢0", "instrument_kind": "CONTINUOUS_FUTURE",
           "source": "futures_zh_spot", "upstream_source": "SINA"}
    class Source:
        def iter_partition(self, *args): yield [row]
    result = archive_partition(Source(), tmp_path, "SHFE", "rb.continuous", "20260905")
    assert verify_archive(result.path)["verification"]["schema_version"] == 3
    loaded = load_ticks(tmp_path, "SHFE", "rb.continuous").to_pylist()
    assert loaded[0]["raw_provider_symbol"] == "螺纹钢0" and loaded[0]["provider_symbol"] == "RB0"
    with pytest.raises(ValueError): partition_path(tmp_path, "SHFE", "rb../", "20260905")
