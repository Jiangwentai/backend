#pragma once

#include "market_data/provider.hpp"

namespace market_data {
class IRealtimeMarketDataProvider {
 public:
  virtual ~IRealtimeMarketDataProvider() = default;
  [[nodiscard]]virtual ProviderId id()const noexcept=0;
  [[nodiscard]]virtual ProviderCapabilities capabilities()const noexcept=0;
  virtual void start()=0;
  virtual void stop()=0;
  virtual void subscribe(const std::vector<Subscription>& subscriptions)=0;
  virtual void unsubscribe(const std::vector<Subscription>& subscriptions)=0;
  [[nodiscard]]virtual ProviderHealth health()const=0;
};
}
