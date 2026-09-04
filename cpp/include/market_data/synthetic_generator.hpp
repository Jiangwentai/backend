#pragma once
#include "market_data/market_tick.hpp"
#include "market_data/producer_identity.hpp"
#include "market_data/spsc_queue.hpp"
#include <atomic>
#include <string>
#include <thread>
#include <vector>
namespace market_data {
class SyntheticGenerator {
 public:SyntheticGenerator(SpscQueue<MarketTick>&q,ProducerIdentity&id,std::uint32_t rate,std::vector<std::string> symbols);~SyntheticGenerator();void start();void request_stop()noexcept;void join();void run();[[nodiscard]]std::uint64_t generated()const noexcept{return generated_;}
 private:SpscQueue<MarketTick>&q_;ProducerIdentity&id_;std::uint32_t rate_;std::vector<std::string>symbols_;std::atomic<bool>stop_{false};std::atomic<std::uint64_t>generated_{0};std::thread thread_;
}; }

