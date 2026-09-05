"""Reuse existing physical mappings and JSON metadata for nonphysical aliases.

No migration, no physical-contract auto-registration, and no provider SDK.
"""
from dataclasses import asdict
from datetime import date
import json

from .foreign_futures import alias_canonical
from .kinds import InstrumentKind as Kind
from .metadata import MappingLookups
from .models import ExplicitMapping, ProviderProductDefinition
from .normalization import provider_symbol_key


def _json(value):
    return json.loads(value) if isinstance(value, str) else value or {}


def _mapping(provider, value):
    fields = dict(value)
    fields["provider"] = provider
    fields["kind"] = Kind(fields.get("kind", "UNKNOWN"))
    for name in ("valid_from", "valid_to"):
        if isinstance(fields.get(name), str):
            fields[name] = date.fromisoformat(fields[name])
    return ExplicitMapping(**fields)


class PostgresInstrumentMetadata(MappingLookups):
    def __init__(self, pool_factory):
        self.pool_factory = pool_factory

    def _db(self):
        return self.pool_factory()

    async def list_explicit_mappings(self, provider, as_of):
        rows = await self._db().fetch("""SELECT p.provider_symbol,p.exchange_code,p.instrument_id,
          p.valid_from,p.valid_to,f.product_code,f.delivery_month FROM provider_instruments p
          JOIN futures_contracts f ON (f.exchange_code,f.instrument_id)=(p.exchange_code,p.instrument_id)
          WHERE p.provider_code=$1 AND (p.valid_from IS NULL OR p.valid_from<=$2)
            AND (p.valid_to IS NULL OR p.valid_to>=$2) ORDER BY p.id""", provider, as_of)
        result = [ExplicitMapping(provider, row["provider_symbol"], f'{row["exchange_code"]}.{row["instrument_id"]}',
                                  Kind.PHYSICAL_FUTURE, row["product_code"], row["delivery_month"],
                                  valid_from=row["valid_from"], valid_to=row["valid_to"]) for row in rows]
        metadata = _json(await self._db().fetchval("SELECT metadata FROM providers WHERE code=$1", provider))
        for value in metadata.get("instrument_aliases", []):
            mapping = _mapping(provider, value)
            if mapping.valid_at(as_of):
                result.append(mapping)
        return result

    async def lookup_metadata_registration(self, canonical, kind):
        exchange, _, instrument = canonical.partition(".")
        if kind == Kind.PHYSICAL_FUTURE:
            return bool(await self._db().fetchval("""SELECT EXISTS(SELECT 1 FROM futures_contracts
              WHERE exchange_code=$1 AND instrument_id=$2)""", exchange, instrument))
        # No generic instrument table is necessary. Explicit typed alias records
        # and typed provider product definitions are the nonphysical metadata.
        providers = await self._db().fetch("SELECT code,metadata FROM providers")
        for row in providers:
            for alias in _json(row["metadata"]).get("instrument_aliases", []):
                if alias.get("canonical_instrument") == canonical and alias.get("kind") == kind:
                    return True
            if any(alias_canonical(definition) == canonical and definition.kind == kind
                   for definition in await self.product_definitions(row["code"])):
                return True
        return False

    async def product_definitions(self, provider):
        metadata = _json(await self._db().fetchval("SELECT metadata FROM providers WHERE code=$1", provider))
        definitions = metadata.get("product_definitions", [])
        result = []
        for value in definitions:
            fields = dict(value)
            fields["provider"] = provider
            fields["kind"] = Kind(fields["kind"])
            result.append(ProviderProductDefinition(**fields))
        operator_keys = {row.provider_symbol for row in result}
        rows = await self._db().fetch("""SELECT payload FROM provider_reference_records
          WHERE provider_code=$1 AND dataset='futures_foreign_products'""", provider)
        for row in rows:
            value = _json(row["payload"]).get("definition")
            if value and value["provider_symbol"] not in operator_keys:
                fields = dict(value)
                fields["provider"] = provider
                fields["kind"] = Kind(fields["kind"])
                result.append(ProviderProductDefinition(**fields))
        return result

    async def add_explicit_mapping(self, mapping: ExplicitMapping):
        """Administrative insertion; no hidden writes in resolve/format/audit.

        Keep existing physical-table mappings when registration exists, otherwise
        use providers.metadata.instrument_aliases without fabricating metadata.
        """
        async with self._db().acquire() as connection:
            async with connection.transaction():
                value = await connection.fetchval("SELECT metadata FROM providers WHERE code=$1 FOR UPDATE", mapping.provider)
                if value is None:
                    raise ValueError("UNKNOWN_PROVIDER")
                metadata = _json(value)
                rows = await connection.fetch("""SELECT provider_symbol,exchange_code,instrument_id
                  FROM provider_instruments WHERE provider_code=$1
                  AND (valid_from IS NULL OR valid_from<=CURRENT_DATE)
                  AND (valid_to IS NULL OR valid_to>=CURRENT_DATE)""", mapping.provider)
                existing = [(row["provider_symbol"], f'{row["exchange_code"]}.{row["instrument_id"]}') for row in rows]
                existing += [(row["provider_symbol"], row["canonical_instrument"])
                             for row in metadata.get("instrument_aliases", [])
                             if _mapping(mapping.provider, row).valid_at(date.today()) and
                             _mapping(mapping.provider,row).provider_source==mapping.provider_source]
                exchange, _, instrument = mapping.canonical_instrument.partition(".")
                for raw, canonical in existing:
                    same_key = provider_symbol_key(mapping.provider, raw) == provider_symbol_key(mapping.provider, mapping.provider_symbol)
                    if same_key and canonical != mapping.canonical_instrument:
                        raise ValueError("NORMALIZED_MAPPING_CONFLICT")
                    if raw == mapping.provider_symbol and canonical == mapping.canonical_instrument:
                        return "UNCHANGED"
                registered = mapping.kind == Kind.PHYSICAL_FUTURE and await connection.fetchval("""SELECT EXISTS(
                    SELECT 1 FROM futures_contracts WHERE exchange_code=$1 AND instrument_id=$2)""", exchange, instrument)
                if registered:
                    await connection.execute("""INSERT INTO provider_instruments(provider_code,exchange_code,instrument_id,provider_symbol)
                      VALUES($1,$2,$3,$4)""", mapping.provider, exchange, instrument, mapping.provider_symbol)
                else:
                    record = asdict(mapping)
                    record.pop("provider")
                    metadata.setdefault("instrument_aliases", []).append(record)
                    await connection.execute("UPDATE providers SET metadata=$2::jsonb,updated_at=now() WHERE code=$1",
                                             mapping.provider, json.dumps(metadata, default=str))
        return "ADDED"
