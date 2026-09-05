from datetime import date, datetime, timezone
import json
import os
import uuid

import pytest

from instruments import ExplicitMapping, InstrumentKind as Kind
from providers.akshare.repositories import AkshareMetadataRepository

pytestmark = [pytest.mark.asyncio, pytest.mark.skipif(not os.getenv("POSTGRES_TEST_DSN"), reason="POSTGRES_TEST_DSN not configured")]


async def test_identity_without_rows_json_aliases_and_legacy_physical_mapping():
    import asyncpg
    from providers.akshare.cli import run, parser
    repository = AkshareMetadataRepository(os.environ["POSTGRES_TEST_DSN"])
    await repository.start()
    connection = await asyncpg.connect(os.environ["POSTGRES_TEST_DSN"])
    original = await connection.fetchval("SELECT metadata FROM providers WHERE code='akshare'")
    token = uuid.uuid4().hex[:10].upper()
    alias = "IDENTITY_" + token
    try:
        resolver = repository.instrument_resolver
        result = await resolver.resolve_raw("akshare", "RB2610", exchange_hint="SHFE")
        assert result.resolved and result.canonical_instrument == "SHFE.rb2610" and result.metadata_registered is False
        assert await repository.provider_symbol("SHFE.rb2610") == ("SHFE", "rb2610", "RB2610")
        assert await repository.resolve("RB2610", "SHFE") == ("SHFE", "rb2610")
        assert not await connection.fetchval("SELECT EXISTS(SELECT 1 FROM futures_contracts WHERE exchange_code='SHFE' AND instrument_id='rb2610')")
        mapping = ExplicitMapping("akshare", alias, "LME.zn.3m", Kind.ROLLING_TENOR, "zn", tenor="P3M")
        assert await repository.instrument_metadata.add_explicit_mapping(mapping) == "ADDED"
        assert await repository.instrument_metadata.add_explicit_mapping(mapping) == "UNCHANGED"
        result = await resolver.resolve_raw("akshare", alias)
        assert result.canonical_instrument == "LME.zn.3m" and result.metadata_registered and result.delivery_month is None
        assert await repository.provider_symbol("LME.zn.3m") == ("LME", "zn.3m", alias)
        with pytest.raises(ValueError, match="NORMALIZED_MAPPING_CONFLICT"):
            await repository.instrument_metadata.add_explicit_mapping(ExplicitMapping("akshare", alias.lower(), "SHFE.cu2610"))
        # A missing physical contract can receive an explicit alias in generic
        # metadata without inventing foreign keys or physical reference fields.
        assert await repository.instrument_metadata.add_explicit_mapping(ExplicitMapping("akshare", alias + "_PH", "SHFE.rb2610")) == "ADDED"
        assert (await resolver.resolve_raw("akshare", alias + "_PH")).metadata_registered is False
        assert await connection.fetchval("SELECT count(*) FROM futures_contracts WHERE exchange_code='LME'") == 0
        # Administrative CLI and read-only commands use only PostgreSQL, not QDB/AKShare.
        from unittest.mock import patch
        with patch.dict(os.environ, {"POSTGRES_DSN": os.environ["POSTGRES_TEST_DSN"]}):
            assert await run(parser().parse_args(["add-instrument", "SHFE.cu2610", "--provider-symbol", alias + "_CLI", "--json"])) == 0
            before = await connection.fetchval("SELECT metadata::text FROM providers WHERE code='akshare'")
            assert await run(parser().parse_args(["resolve-instrument", alias + "_CLI", "--json"])) == 0
            assert await run(parser().parse_args(["provider-symbol", "SHFE.cu2610", "--json"])) == 0
            assert await run(parser().parse_args(["list-instruments", "--json"])) == 0
            assert await run(parser().parse_args(["audit-instruments", "--json"])) == 0
            assert await connection.fetchval("SELECT metadata::text FROM providers WHERE code='akshare'") == before
    finally:
        await connection.execute("UPDATE providers SET metadata=$1::jsonb WHERE code='akshare'", original)
        await repository.close()
        await connection.close()


async def test_physical_mapping_normalized_collisions_and_dated_overrides():
    import asyncpg
    repository = AkshareMetadataRepository(os.environ["POSTGRES_TEST_DSN"])
    await repository.start()
    connection = await asyncpg.connect(os.environ["POSTGRES_TEST_DSN"])
    exchange = "IT" + uuid.uuid4().hex[:8].upper()
    symbol = "ALIAS" + uuid.uuid4().hex[:8].upper()
    try:
        await connection.execute("INSERT INTO exchanges(code,name) VALUES($1,'Identity test')", exchange)
        await connection.execute("INSERT INTO products(exchange_code,code,name) VALUES($1,'x','Identity test')", exchange)
        await connection.execute("INSERT INTO futures_contracts(exchange_code,instrument_id,product_code,delivery_month) VALUES($1,'x2610','x','202610'),($1,'x2611','x','202611')", exchange)
        await repository.instrument_metadata.add_explicit_mapping(ExplicitMapping("akshare", symbol, f"{exchange}.x2610", product="x", delivery_month="202610"))
        assert await connection.fetchval("SELECT count(*) FROM provider_instruments WHERE exchange_code=$1", exchange) == 1
        assert await repository.resolve(symbol.lower()) == (exchange, "x2610")
        await connection.execute("INSERT INTO provider_instruments(provider_code,exchange_code,instrument_id,provider_symbol) VALUES('akshare',$1,'x2611',$2)", exchange, symbol.lower())
        result = await repository.instrument_resolver.resolve_raw("akshare", symbol.title())
        assert not result.resolved and result.reason == "AMBIGUOUS_EXPLICIT_MAPPING"
        assert (await repository.instrument_resolver.resolve_raw("akshare", symbol)).canonical_instrument == f"{exchange}.x2610"
        assert any(row["status"] == "CONFLICT" for row in await repository.instrument_resolver.audit_mappings("akshare"))
    finally:
        await repository.close()
        await connection.execute("DELETE FROM provider_instruments WHERE exchange_code=$1", exchange)
        await connection.execute("DELETE FROM futures_contracts WHERE exchange_code=$1", exchange)
        await connection.execute("DELETE FROM products WHERE exchange_code=$1", exchange)
        await connection.execute("DELETE FROM exchanges WHERE code=$1", exchange)
        await connection.close()
