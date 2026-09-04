#pragma once
#include "market_data/market_tick.hpp"
#include "market_data/spsc_queue.hpp"
#include "market_data/live_queue.hpp"
#include <atomic>
#include <thread>
#include <vector>
namespace market_data {
class Dispatcher {
 public:
  Dispatcher(SpscQueue<MarketTick>& ingress,SpscQueue<MarketTick>& persistence,LiveQueue* live=nullptr):persistence_(persistence),live_(live){ingresses_.push_back(&ingress);}
  Dispatcher(SpscQueue<MarketTick>& persistence,LiveQueue* live=nullptr):persistence_(persistence),live_(live){}
  void add_ingress(SpscQueue<MarketTick>& ingress);
  ~Dispatcher(); void start(); void request_stop() noexcept; void join(); void run();
  [[nodiscard]] bool degraded() const noexcept{return degraded_.load();}
 private:[[nodiscard]]bool ingresses_empty()const;std::vector<SpscQueue<MarketTick>*>ingresses_;SpscQueue<MarketTick>& persistence_;LiveQueue* live_;std::atomic<bool> stopping_{false},degraded_{false};std::thread thread_;
}; }
