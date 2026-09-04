#pragma once
#include "market_data/market_tick.hpp"
#include <array>
#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

namespace market_data::ctp {
enum class State {disconnected,connecting,connected,authenticating,authenticated,logging_in,logged_in,subscribing,ready,reconnecting,error};
enum class Action {none,authenticate,login,subscribe};
const char* state_name(State state) noexcept;

struct Config {
  std::string front_address,broker_id,user_id,password,app_id,auth_code,flow_path{"./data/ctp-flow"};
  std::vector<std::string> subscriptions;
  bool authentication_required{false};
};
struct Metrics {
  bool connected{},logged_in{},ready{};
  std::uint64_t ticks_received_total{},invalid_ticks_total{},ingress_rejected_total{},disconnect_total{},reconnect_total{},login_failure_total{},subscription_success_total{},subscription_failure_total{};
  std::int64_t last_tick_timestamp{};
};
struct DepthSnapshot {
  InstrumentCode instrument;ExchangeCode exchange;DateCode trading_day;DateCode action_day;FixedString<9> update_time;
  int update_millisec{};double last_price{},turnover{},open_interest{},upper_limit_price{},lower_limit_price{};std::int64_t volume{};
  std::array<double,5> bid_price{},ask_price{};std::array<std::int32_t,5> bid_volume{},ask_volume{};
};
}
