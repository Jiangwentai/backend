"""Sina display-name aliases from the pinned AKShare futures documentation/cons.

Only explicitly listed product names plus an unchanged contract suffix are
accepted. Never attach identity using the requested row position.
"""
import re

from instruments.normalization import normalize_provider_symbol

DISPLAY_PRODUCTS = {
    "螺纹钢": "RB", "沪铜": "CU", "铜": "CU", "沪锌": "ZN", "锌": "ZN",
    "沪铝": "AL", "铝": "AL", "沪铅": "PB", "铅": "PB", "沪镍": "NI", "镍": "NI",
    "沪锡": "SN", "锡": "SN", "沪金": "AU", "黄金": "AU", "沪银": "AG", "白银": "AG",
    "热卷": "HC", "热轧卷板": "HC", "燃料油": "FU", "沥青": "BU", "橡胶": "RU",
    "纸浆": "SP", "不锈钢": "SS", "氧化铝": "AO", "BR橡胶": "BR",
    "原油": "SC", "20号胶": "NR", "低硫燃料油": "LU", "国际铜": "BC",
    "铁矿石": "I", "玉米": "C", "玉米淀粉": "CS", "豆一": "A", "豆二": "B",
    "豆粕": "M", "豆油": "Y", "棕榈油": "P", "鸡蛋": "JD", "塑料": "L",
    "聚丙烯": "PP", "焦炭": "J", "焦煤": "JM", "乙二醇": "EG", "苯乙烯": "EB",
    "液化石油气": "PG", "生猪": "LH", "白糖": "SR", "郑糖": "SR",
    "郑棉": "CF", "棉花": "CF", "甲醇": "MA", "郑醇": "MA", "玻璃": "FG",
    "菜油": "OI", "菜粕": "RM", "PTA": "TA", "纯碱": "SA", "工业硅": "SI",
    "碳酸锂": "LC", "沪深300": "IF", "上证50": "IH", "中证500": "IC", "中证1000": "IM",
}


def quote_symbol_key(raw: str, exchange_hint: str | None = None) -> str:
    normalized = normalize_provider_symbol("akshare", raw, exchange_hint)
    value = normalized.symbol
    for name in sorted(DISPLAY_PRODUCTS, key=len, reverse=True):
        if value.startswith(name):
            suffix = value[len(name):]
            if re.fullmatch(r"[0-9]{3,4}|0", suffix):
                return DISPLAY_PRODUCTS[name] + suffix
    return value
