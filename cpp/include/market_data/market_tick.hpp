#pragma once
#include "market_data/provider.hpp"
#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <string_view>

namespace market_data {
template <std::size_t N> struct FixedString {
  std::array<char, N> data{};
  constexpr FixedString() = default;
  explicit FixedString(std::string_view value) noexcept { assign(value); }
  void assign(std::string_view value) noexcept;
  [[nodiscard]] std::string_view view() const noexcept;
  auto operator<=>(const FixedString&) const = default;
};
using ProducerId = FixedString<37>;
using ExchangeCode = FixedString<9>;
using InstrumentCode = FixedString<32>;
using InstrumentId = FixedString<64>;
using DateCode = FixedString<9>;

struct EventHeader {
  ProviderId provider{ProviderId::synthetic};
  ProducerId producer_id;
  std::uint64_t seq{};
  std::int64_t event_ts_us{};
  std::int64_t recv_ts_ns{};
  InstrumentId instrument_id;
};

struct QuoteSnapshot {
  ProviderId provider{ProviderId::synthetic};
  MarketEventType event_type{MarketEventType::quote_snapshot};
  DataQuality quality{DataQuality::unknown};
  InstrumentId instrument_id;
  std::int64_t event_ts_us{};
  std::int64_t recv_ts_ns{};
  std::uint64_t seq{};
  ProducerId producer_id;
  ExchangeCode exchange;
  InstrumentCode instrument;
  DateCode trading_day;
  DateCode action_day;
  double last_price{std::numeric_limits<double>::quiet_NaN()};
  std::int64_t volume{};
  double turnover{};
  double open_interest{};
  double upper_limit_price{std::numeric_limits<double>::quiet_NaN()};
  double lower_limit_price{std::numeric_limits<double>::quiet_NaN()};
  std::array<double, 5> bid_price{};
  std::array<std::int32_t, 5> bid_volume{};
  std::array<double, 5> ask_price{};
  std::array<std::int32_t, 5> ask_volume{};
};

using MarketTick = QuoteSnapshot;

[[nodiscard]] bool is_valid_price(double value) noexcept;
[[nodiscard]] double normalize_price(double value) noexcept;
// Interprets CTP local exchange time in Asia/Shanghai (UTC+8). Empty action_day
// selects the local date nearest to receive time, resolving midnight crossover.
[[nodiscard]] std::int64_t normalize_ctp_event_ts_us(std::string_view action_day,
  std::string_view update_time, int update_millisec,
  std::chrono::system_clock::time_point local_receive_time);
}
