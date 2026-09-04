#pragma once

#include "market_data/market_tick.hpp"
#include <variant>

namespace market_data {
struct TradeTick { EventHeader header; double price{}; std::int64_t size{}; };
struct BidAskTick { EventHeader header; double bid_price{}; std::int32_t bid_size{}; double ask_price{}; std::int32_t ask_size{}; };
struct DepthUpdate { EventHeader header; std::uint16_t level{}; bool bid{}; double price{}; std::int32_t size{}; };
struct BarEvent { EventHeader header; std::int64_t interval_us{}; double open{},high{},low{},close{}; std::int64_t volume{}; bool provider_supplied{true}; };
using MarketEvent = std::variant<QuoteSnapshot,TradeTick,BidAskTick,DepthUpdate,BarEvent>;

[[nodiscard]] inline EventHeader event_header(const MarketEvent& event) {
  return std::visit([](const auto& value)->EventHeader {
    if constexpr (std::is_same_v<std::decay_t<decltype(value)>,QuoteSnapshot>) {
      return {value.provider,value.producer_id,value.seq,value.event_ts_us,value.recv_ts_ns,value.instrument_id};
    } else return value.header;
  },event);
}
}
