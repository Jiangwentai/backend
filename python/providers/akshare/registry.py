from __future__ import annotations

from dataclasses import dataclass

from .models import DatasetType, EndpointStability


@dataclass(frozen=True)
class EndpointDefinition:
    name: str
    function_name: str
    dataset_type: DatasetType
    upstream_source: str | None
    frequency: str
    stability: EndpointStability
    enabled: bool
    required_columns: frozenset[str]
    optional_columns: frozenset[str] = frozenset()
    empty_is_error: bool = True


ENDPOINTS = {
    "futures_daily_sina": EndpointDefinition(
        "futures_daily_sina", "futures_zh_daily_sina", DatasetType.HISTORICAL_BARS,
        "SINA", "1d", EndpointStability.STABLE, True,
        frozenset({"date", "open", "high", "low", "close"}),
        frozenset({"volume", "hold", "settle"}),
    ),
    "futures_1m_sina": EndpointDefinition(
        "futures_1m_sina", "futures_zh_minute_sina", DatasetType.HISTORICAL_BARS,
        "SINA", "1m", EndpointStability.STABLE, True,
        frozenset({"datetime", "open", "high", "low", "close"}),
        frozenset({"volume", "hold"}),
    ),
    "futures_realtime_quote": EndpointDefinition(
        "futures_realtime_quote", "futures_zh_spot", DatasetType.BEST_EFFORT_QUOTES,
        "SINA", "snapshot", EndpointStability.STABLE, True,
        frozenset({"symbol", "current_price"}),
        frozenset({"time", "open", "high", "low", "bid_price", "ask_price", "buy_vol",
                   "sell_vol", "hold", "volume", "exchange", "contract", "date"}),
    ),
    "futures_contracts_qihuo": EndpointDefinition(
        "futures_contracts_qihuo", "futures_comm_info", DatasetType.REFERENCE,
        "9QIHUO", "daily", EndpointStability.STABLE, True,
        frozenset({"交易所名称", "合约名称", "合约代码"}),
        frozenset({"现价","涨停板","跌停板","保证金-买开","保证金-卖开","保证金-每手",
          "手续费标准-开仓-万分之","手续费标准-开仓-元","手续费标准-平昨-万分之","手续费标准-平昨-元",
          "手续费标准-平今-万分之","手续费标准-平今-元","每跳毛利","手续费","每跳净利","备注",
          "手续费更新时间","价格更新时间"}),
    ),
    "futures_contracts_sina": EndpointDefinition(
        "futures_contracts_sina", "futures_display_main_sina", DatasetType.REFERENCE,
        "SINA", "daily", EndpointStability.EXPERIMENTAL, False,
        frozenset({"symbol", "exchange", "name"}),
    ),
    "futures_inventory_99": EndpointDefinition(
        "futures_inventory_99", "futures_inventory_99", DatasetType.INVENTORY,
        "99QH", "publication", EndpointStability.EXPERIMENTAL, False, frozenset(),
    ),
    "futures_positions_sina": EndpointDefinition(
        "futures_positions_sina", "futures_hold_pos_sina", DatasetType.POSITION,
        "SINA", "publication", EndpointStability.EXPERIMENTAL, False, frozenset(),
    ),
}


def endpoint(name: str, *, require_enabled: bool = True) -> EndpointDefinition:
    try:
        value = ENDPOINTS[name]
    except KeyError as exc:
        raise ValueError(f"unsupported AKShare endpoint: {name}") from exc
    if require_enabled and not value.enabled:
        raise ValueError(f"AKShare endpoint is not production-enabled: {name}")
    return value
