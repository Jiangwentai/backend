#pragma once

#include "market_data/realtime_provider.hpp"
#include <memory>
#include <vector>

namespace market_data {
class ProviderManager {
 public:
  void add(std::unique_ptr<IRealtimeMarketDataProvider> provider);
  void start_all();
  void stop_all()noexcept;
  void subscribe(ProviderId provider,const std::vector<Subscription>& subscriptions);
  [[nodiscard]]std::vector<ProviderHealth> health()const;
  [[nodiscard]]std::vector<ProviderCapabilities> capabilities()const;
  [[nodiscard]]std::size_t size()const noexcept{return providers_.size();}
 private:std::vector<std::unique_ptr<IRealtimeMarketDataProvider>>providers_;
};
}
