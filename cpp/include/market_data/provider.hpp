#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace market_data {

enum class ProviderId : std::uint8_t { synthetic, ctp, ibkr, akshare };
enum class ProviderState : std::uint8_t { stopped, starting, connecting, authenticating, ready, degraded, reconnecting, error, stopping };
enum class DataQuality : std::uint8_t { realtime, delayed, frozen, best_effort, unknown };
enum class MarketEventType : std::uint8_t { quote_snapshot, trade_tick, bid_ask_tick, depth_update, bar };
enum class MarketDataKind : std::uint8_t { quote, trade, bid_ask, depth };

[[nodiscard]] std::string_view to_string(ProviderId value) noexcept;
[[nodiscard]] std::string_view to_string(ProviderState value) noexcept;
[[nodiscard]] std::string_view to_string(DataQuality value) noexcept;
[[nodiscard]] std::string_view to_string(MarketEventType value) noexcept;
[[nodiscard]] std::optional<ProviderId> provider_id_from_string(std::string_view value) noexcept;
[[nodiscard]] std::optional<DataQuality> data_quality_from_string(std::string_view value) noexcept;
[[nodiscard]] std::optional<MarketEventType> market_event_type_from_string(std::string_view value) noexcept;

struct ProviderCapabilities {
  bool realtime_quotes{};
  bool tick_by_tick{};
  bool market_depth{};
  bool historical_ticks{};
  bool historical_bars{};
  bool reference_data{};
};

struct ProviderHealth {
  ProviderId provider{ProviderId::synthetic};
  ProviderState state{ProviderState::stopped};
  bool connected{};
  bool ready{};
  std::int64_t last_event_time_ns{};
  std::int64_t last_success_time_ns{};
  std::string last_error;
};

struct Subscription {
  std::string instrument_id;
  MarketDataKind kind{MarketDataKind::quote};
  std::uint16_t depth_levels{1};
};

class IHistoricalDataProvider {
 public:
  virtual ~IHistoricalDataProvider() = default;
  [[nodiscard]] virtual ProviderId id() const noexcept = 0;
  [[nodiscard]] virtual ProviderCapabilities capabilities() const noexcept = 0;
};

class IReferenceDataProvider {
 public:
  virtual ~IReferenceDataProvider() = default;
  [[nodiscard]] virtual ProviderId id() const noexcept = 0;
  [[nodiscard]] virtual ProviderCapabilities capabilities() const noexcept = 0;
};

}
