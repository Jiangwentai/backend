#pragma once
#include "market_data/market_tick.hpp"
#include "market_data/spsc_queue.hpp"
#include "market_data/live_queue.hpp"
#include <atomic>
#include <thread>
namespace market_data {
class Dispatcher {
 public:
  Dispatcher(SpscQueue<MarketTick>& ingress,SpscQueue<MarketTick>& persistence,LiveQueue* live=nullptr):ingress_(ingress),persistence_(persistence),live_(live){}
  ~Dispatcher(); void start(); void request_stop() noexcept; void join(); void run();
  [[nodiscard]] bool degraded() const noexcept{return degraded_.load();}
 private:SpscQueue<MarketTick>& ingress_;SpscQueue<MarketTick>& persistence_;LiveQueue* live_;std::atomic<bool> stopping_{false},degraded_{false};std::thread thread_;
}; }
