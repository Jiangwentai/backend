#pragma once

#include "market_data/market_event.hpp"
#include "market_data/spsc_queue.hpp"
#include <atomic>

namespace market_data {
class IMarketEventSink {
 public:
  virtual ~IMarketEventSink() = default;
  virtual bool publish(MarketEvent&& event) noexcept = 0;
};

class QueueMarketEventSink final : public IMarketEventSink {
 public:
  explicit QueueMarketEventSink(SpscQueue<MarketTick>& queue):queue_(queue){}
  bool publish(MarketEvent&& event) noexcept override {
    auto* quote=std::get_if<QuoteSnapshot>(&event);
    if(quote==nullptr){++unsupported_;return false;}
    return queue_.try_push(*quote);
  }
  [[nodiscard]]std::uint64_t unsupported_total()const noexcept{return unsupported_;}
 private:
  SpscQueue<MarketTick>&queue_;std::atomic<std::uint64_t>unsupported_{0};
};
}
