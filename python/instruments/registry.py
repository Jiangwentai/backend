"""Vetted identity rules, not a claim that a contract is listed or tradable.

Sources and scope are recorded in docs/instruments.md. No network or SDK import.
"""
from .kinds import InstrumentKind as Kind
from .models import ForeignProductDefinition, ProviderProductDefinition

DOMESTIC_PRODUCTS = {
    "SHFE": "CU AL ZN PB NI SN AU AG RB WR HC FU BU RU SP SS AO BR AD OP".split(),
    "INE": "SC NR LU BC EC".split(),
    "DCE": "C CS A B M Y P FB BB JD L V PP J JM I EG RR EB PG LH LG BZ".split(),
    "CZCE": "WH PM CF SR TA OI RI MA FG RS RM ZC JR LR SF SM CY AP UR CJ SA PK PF PX SH PR PL".split(),
    "CFFEX": "IF IC IH IM T TF TS TL".split(),
    "GFEX": "SI LC PS".split(),
}
DOMESTIC_EXCHANGES = frozenset(DOMESTIC_PRODUCTS)
EXCHANGES = DOMESTIC_EXCHANGES | {"COMEX", "NYMEX", "ICEUS", "ICEEU", "CME", "CBOT", "LME", "SGX"}

# AKShare Eastmoney root + YY + month-code dialect. Other providers must
# explicitly register their dialect; e.g. IBKR localSymbol is not assumed.
FOREIGN_ROOTS = tuple(ForeignProductDefinition("akshare", root, exchange, root.lower())
                      for root, exchange in (("GC", "COMEX"), ("SI", "COMEX"), ("HG", "COMEX"),
                                             ("CL", "NYMEX"), ("NG", "NYMEX"), ("SB", "ICEUS"), ("CT", "ICEUS")))
AKSHARE_REFERENCE_NAMES = {
    "CAD": "LME铜3个月", "ZSD": "LME锌3个月", "AHD": "LME铝3个月",
    "NID": "LME镍3个月", "PBD": "LME铅3个月", "SND": "LME锡3个月",
    "GC": "COMEX黄金", "SI": "COMEX白银", "HG": "COMEX铜",
    "CL": "NYMEX原油", "NG": "NYMEX天然气", "CT": "NYBOT-棉花",
}
PRODUCT_ALIASES = tuple(
    ProviderProductDefinition("akshare", symbol, "LME", product, Kind.ROLLING_TENOR, "P3M",
                              {"source": "AKShare/Sina product reference", "venue_direct": False}, "SINA")
    for symbol, product in (("CAD", "cu"), ("ZSD", "zn"), ("AHD", "al"), ("NID", "ni"), ("PBD", "pb"), ("SND", "sn"))
)
PRODUCT_ALIASES += (
    ProviderProductDefinition("akshare", "LZNT", "LME", "zn", Kind.ROLLING_TENOR, "P3M",
                              {"source":"AKShare/Eastmoney global futures", "venue_direct":False}, "EASTMONEY"),
)
# Root-only foreign quotes are provider product series, not identified physical
# months. Continuous here describes the provider series, not a roll schedule.
PRODUCT_ALIASES += tuple(
    ProviderProductDefinition("akshare", root, exchange, root.lower(), Kind.CONTINUOUS_FUTURE,
                              metadata={"source": "AKShare/Sina product reference", "venue_direct": False,
                                        "roll_policy": "provider-defined; unspecified"})
    for root, exchange in (("GC", "COMEX"), ("SI", "COMEX"), ("HG", "COMEX"),
                           ("CL", "NYMEX"), ("NG", "NYMEX"), ("CT", "ICEUS"))
)
