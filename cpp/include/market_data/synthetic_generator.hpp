#pragma once
#include "market_data/market_tick.hpp"
#include "market_data/producer_identity.hpp"
#include "market_data/event_sink.hpp"
#include "market_data/realtime_provider.hpp"
#include "market_data/spsc_queue.hpp"
#include <atomic>
#include <string>
#include <thread>
#include <vector>
namespace market_data {
class SyntheticGenerator final : public IRealtimeMarketDataProvider {
 public:SyntheticGenerator(SpscQueue<MarketTick>&q,ProducerIdentity&id,std::uint32_t rate,std::vector<std::string> symbols);~SyntheticGenerator();void start();void request_stop()noexcept;void join();void run();[[nodiscard]]std::uint64_t generated()const noexcept{return generated_;}
  [[nodiscard]]ProviderId id()const noexcept override{return ProviderId::synthetic;}
  [[nodiscard]]ProviderCapabilities capabilities()const noexcept override{return{true,false,true,false,false,false};}
  void stop()override{request_stop();join();}
  void subscribe(const std::vector<Subscription>& subscriptions)override;
  void unsubscribe(const std::vector<Subscription>& subscriptions)override;
  [[nodiscard]]ProviderHealth health()const override;
 private:SpscQueue<MarketTick>&q_;QueueMarketEventSink sink_;ProducerIdentity&id_;std::uint32_t rate_;std::vector<std::string>symbols_;std::atomic<bool>stop_{false},running_{false};std::atomic<std::uint64_t>generated_{0};std::atomic<std::int64_t>last_event_ns_{0};std::thread thread_;
}; }
