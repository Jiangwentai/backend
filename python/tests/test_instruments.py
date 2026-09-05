from datetime import date

import pytest

from instruments import ProviderInstrumentResolver, ExplicitMapping, InstrumentKind as Kind, ProviderProductDefinition
from instruments.metadata import MemoryInstrumentMetadata
from instruments.month_codes import expand_yymm, month_code_to_month, month_to_month_code
from instruments.normalization import provider_symbol_key


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", ["RB2610", "rb2610", "Rb2610", " RB2610 ", "RB-2610", "RB_2610", "RB.2610", "2610RB", "SHFE.RB2610", "SHFE_RB2610", "SHFE-RB2610", "RB2610.SHFE"])
async def test_domestic_identity_without_metadata(raw):
    metadata = MemoryInstrumentMetadata()
    result = await ProviderInstrumentResolver(metadata).resolve_raw("akshare", raw, exchange_hint="SHFE")
    assert result.resolved and result.canonical_instrument == "SHFE.rb2610"
    assert result.kind == Kind.PHYSICAL_FUTURE and result.delivery_month == "202610"
    assert result.raw_symbol == raw and result.normalized_symbol == "RB2610"
    assert result.metadata_registered is False and not metadata.mappings and not metadata.registered


@pytest.mark.asyncio
@pytest.mark.parametrize("raw,exchange,canonical", [("I2701", "DCE", "DCE.i2701"), ("IF2612", "CFFEX", "CFFEX.if2612"),
                                                    ("LC2701", "GFEX", "GFEX.lc2701"), ("SC2610", "INE", "INE.sc2610")])
async def test_domestic_venues(raw, exchange, canonical):
    assert (await ProviderInstrumentResolver().resolve_raw("akshare", raw, exchange_hint=exchange)).canonical_instrument == canonical


@pytest.mark.asyncio
@pytest.mark.parametrize("symbol,reason", [("SHFE.RB2610", "EXCHANGE_HINT_CONFLICT"), ("CU2610", "PRODUCT_EXCHANGE_MISMATCH"),
                                          ("I2613", "INVALID_MONTH"), ("ZZ2610", "UNKNOWN_PRODUCT")])
async def test_reject_conflicts_and_bad_symbols(symbol, reason):
    result = await ProviderInstrumentResolver().resolve_raw("akshare", symbol, exchange_hint="DCE")
    assert not result.resolved and result.reason == reason


@pytest.mark.asyncio
@pytest.mark.parametrize("root", ["SR", "MA", "CF"])
async def test_czce_needs_explicit_decade_context(root):
    resolver = ProviderInstrumentResolver()
    assert not (await resolver.resolve_raw("akshare", root + "701", exchange_hint="CZCE")).resolved
    result = await resolver.resolve_raw("akshare", root + "701", exchange_hint="CZCE", as_of=date(2026, 9, 5))
    assert result.delivery_month == "202701" and result.canonical_instrument == f"CZCE.{root.lower()}2701"
    assert not (await resolver.resolve_raw("akshare", root + "701", exchange_hint="CZCE", as_of=date(2031, 1, 1))).resolved


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", ["RB0", "RB00", "RB888", "RB999", "主力", "连续"])
async def test_nonphysical_domestic_not_assigned_delivery(raw):
    result = await ProviderInstrumentResolver().resolve_raw("akshare", raw, exchange_hint="SHFE")
    assert result.kind != Kind.PHYSICAL_FUTURE and result.delivery_month is None


def test_all_month_codes_and_year_policy():
    for month in range(1, 13):
        assert month_code_to_month(month_to_month_code(month)) == month
    with pytest.raises(ValueError): month_code_to_month("I")
    with pytest.raises(ValueError): month_to_month_code(0)
    assert expand_yymm("2610") == "202610"
    assert expand_yymm("9912", as_of=date(2000, 1, 1)) == "199912"
    with pytest.raises(ValueError, match="AMBIGUOUS_CENTURY"): expand_yymm("5012", as_of=date(2000, 1, 1))


@pytest.mark.asyncio
@pytest.mark.parametrize("raw,canonical,delivery", [("GC25Z", "COMEX.gc2512", "202512"), ("CL26F", "NYMEX.cl2601", "202601"),
                                                    ("NG26H", "NYMEX.ng2603", "202603"), ("SB26K", "ICEUS.sb2605", "202605")])
async def test_foreign_physical_without_registration(raw, canonical, delivery):
    result = await ProviderInstrumentResolver(MemoryInstrumentMetadata()).resolve_raw("akshare", raw)
    assert result.canonical_instrument == canonical and result.delivery_month == delivery
    assert result.kind == Kind.PHYSICAL_FUTURE and result.metadata_registered is False


@pytest.mark.asyncio
async def test_aliases_and_unknown_foreign_semantics():
    resolver = ProviderInstrumentResolver()
    rolling = await resolver.resolve_raw("akshare", "ZSD")
    assert rolling.canonical_instrument == "LME.zn.3m" and rolling.kind == Kind.ROLLING_TENOR and rolling.tenor == "P3M"
    continuous = await resolver.resolve_raw("akshare", "GC")
    assert continuous.canonical_instrument == "COMEX.gc.continuous" and continuous.kind == Kind.CONTINUOUS_FUTURE
    assert rolling.delivery_month is continuous.delivery_month is None
    assert not (await resolver.resolve_raw("akshare", "SOMETHING_UNKNOWN")).resolved
    assert not (await resolver.resolve_raw("ibkr", "GC25Z")).resolved  # no borrowed AKShare dialect


@pytest.mark.asyncio
async def test_foreign_aliases_and_reverse_resolution_are_source_specific():
    resolver=ProviderInstrumentResolver()
    sina=await resolver.format_provider_symbol("akshare","LME.zn.3m",provider_source="SINA")
    eastmoney=await resolver.format_provider_symbol("akshare","LME.zn.3m",provider_source="EASTMONEY")
    assert sina.provider_symbol=="ZSD" and eastmoney.provider_symbol=="LZNT"
    assert (await resolver.resolve_raw("akshare","ZSD",provider_source="SINA")).canonical_instrument=="LME.zn.3m"
    assert (await resolver.resolve_raw("akshare","LZNT",provider_source="EASTMONEY")).canonical_instrument=="LME.zn.3m"
    assert not (await resolver.resolve_raw("akshare","LZNT",provider_source="SINA")).resolved
    assert not (await resolver.resolve_raw("akshare","UNKNOWN",provider_source="EASTMONEY")).resolved


@pytest.mark.asyncio
async def test_explicit_precedence_normalization_conflict_and_ambiguity(caplog):
    metadata = MemoryInstrumentMetadata([
        ExplicitMapping("akshare", "CU2610", "SHFE.cu2611", delivery_month="202611"),
        ExplicitMapping("akshare", "SPECIAL_RB", "SHFE.rb2610"),
    ])
    resolver = ProviderInstrumentResolver(metadata)
    exact = await resolver.resolve_raw("akshare", "CU2610")
    assert exact.canonical_instrument == "SHFE.cu2611" and exact.delivery_month == "202611" and exact.conflict
    assert exact.method == "EXPLICIT_MAPPING" and resolver.metrics.counts["instrument_resolution_conflicts_total"] == 1
    assert any(record.provider_mapping_parser_conflict for record in caplog.records)
    drift = await resolver.resolve_raw("akshare", "cu-2610")
    assert drift.method == "NORMALIZED_EXPLICIT_MAPPING" and drift.canonical_instrument == exact.canonical_instrument
    assert (await resolver.resolve_raw("akshare", "SPECIAL_RB")).method == "EXPLICIT_MAPPING"
    metadata.mappings.append(ExplicitMapping("akshare", "cu2610", "SHFE.cu2612"))
    assert (await resolver.resolve_raw("akshare", "Cu2610")).reason == "AMBIGUOUS_EXPLICIT_MAPPING"
    assert (await resolver.resolve_raw("akshare", "CU2610")).canonical_instrument == "SHFE.cu2611"  # exact wins
    assert "CU2610" not in resolver.metrics.render()


@pytest.mark.asyncio
async def test_dated_aliases_and_metadata_enrichment():
    metadata = MemoryInstrumentMetadata([
        ExplicitMapping("akshare", "OLD", "SHFE.rb2610", valid_to=date(2026, 1, 1)),
        ExplicitMapping("akshare", "OLD", "SHFE.rb2611", valid_from=date(2026, 1, 2)),
    ], registered=["SHFE.rb2610"])
    resolver = ProviderInstrumentResolver(metadata)
    old = await resolver.resolve_raw("akshare", "OLD", as_of=date(2026, 1, 1))
    assert old.metadata_registered and old.canonical_instrument == "SHFE.rb2610"
    assert (await resolver.resolve_raw("akshare", "OLD", as_of=date(2026, 1, 2))).canonical_instrument == "SHFE.rb2611"


@pytest.mark.asyncio
@pytest.mark.parametrize("canonical,symbol", [("SHFE.rb2610", "RB2610"), ("SHFE.cu2610", "CU2610"), ("CZCE.sr2701", "SR2701"),
                                             ("COMEX.gc2512", "GC25Z"), ("LME.zn.3m", "ZSD"), ("COMEX.gc.continuous", "GC"),
                                             ("SHFE.rb.continuous", "RB0")])
async def test_reverse_round_trip(canonical, symbol):
    resolver = ProviderInstrumentResolver()
    result = await resolver.format_provider_symbol("akshare", canonical)
    assert result.resolved and result.provider_symbol == symbol
    assert (await resolver.resolve_raw("akshare", symbol, exchange_hint=canonical.partition(".")[0])).canonical_instrument == canonical


@pytest.mark.asyncio
async def test_reverse_respects_forward_override_and_alias():
    resolver = ProviderInstrumentResolver(MemoryInstrumentMetadata([
        ExplicitMapping("akshare", "RB2610", "SHFE.rb2611"),
        ExplicitMapping("akshare", "ODD", "SHFE.cu2610"),
    ]))
    assert (await resolver.format_provider_symbol("akshare", "SHFE.rb2610")).reason == "ROUND_TRIP_CONFLICT"
    assert (await resolver.format_provider_symbol("akshare", "SHFE.cu2610")).provider_symbol == "ODD"
    audit = await resolver.audit_mappings("akshare")
    assert audit[0]["status"] == "CONFLICT" and audit[1]["status"] == "UNPARSEABLE"


def test_normalization_is_conservative_and_provider_scoped():
    assert provider_symbol_key("akshare", "RB-2610") == "RB2610"
    assert provider_symbol_key("akshare", "R B2610") != "RB2610"
    assert provider_symbol_key("akshare", "RB/2610") != "RB2610"
    assert provider_symbol_key("unknown", "rb2610") == "rb2610"


@pytest.mark.asyncio
async def test_explicit_foreign_target_enrichment_uses_target_month():
    resolver = ProviderInstrumentResolver(MemoryInstrumentMetadata([ExplicitMapping("akshare", "GC25Z", "COMEX.gc2601")]))
    result = await resolver.resolve_raw("akshare", "GC25Z")
    assert result.conflict and result.product == "gc" and result.delivery_month == "202601"
