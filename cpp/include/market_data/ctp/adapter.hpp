#pragma once
#include "market_data/ctp/types.hpp"
#include "market_data/producer_identity.hpp"
#include "market_data/spsc_queue.hpp"
#include "market_data/event_sink.hpp"
#include "market_data/realtime_provider.hpp"
#include <memory>
namespace market_data::ctp {
class MarketDataAdapter final : public IRealtimeMarketDataProvider {
 public:MarketDataAdapter(SpscQueue<MarketTick>&,ProducerIdentity&,Config);~MarketDataAdapter();MarketDataAdapter(const MarketDataAdapter&)=delete;MarketDataAdapter&operator=(const MarketDataAdapter&)=delete;void start()override;void stop()override;void subscribe(const std::vector<Subscription>&)override;void unsubscribe(const std::vector<Subscription>&)override;[[nodiscard]]ProviderId id()const noexcept override{return ProviderId::ctp;}[[nodiscard]]ProviderCapabilities capabilities()const noexcept override{return{true,false,true,false,false,false};}[[nodiscard]]ProviderHealth health()const override;[[nodiscard]]State state()const noexcept;[[nodiscard]]Metrics metrics()const noexcept;
 private:struct Impl;std::unique_ptr<Impl> impl_;
};
}
