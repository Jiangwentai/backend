#pragma once
#include "market_data/ctp/types.hpp"
#include "market_data/producer_identity.hpp"
#include "market_data/spsc_queue.hpp"
#include <memory>
namespace market_data::ctp {
class MarketDataAdapter {
 public:MarketDataAdapter(SpscQueue<MarketTick>&,ProducerIdentity&,Config);~MarketDataAdapter();MarketDataAdapter(const MarketDataAdapter&)=delete;MarketDataAdapter&operator=(const MarketDataAdapter&)=delete;void start();void stop();[[nodiscard]]State state()const noexcept;[[nodiscard]]Metrics metrics()const noexcept;
 private:struct Impl;std::unique_ptr<Impl> impl_;
};
}
