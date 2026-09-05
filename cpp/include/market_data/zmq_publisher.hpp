#pragma once
#include "market_data/live_queue.hpp"
#include <atomic>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

namespace market_data {
struct ZmqMetrics {
  // Successful socket submissions, not subscriber delivery or HWM loss counts.
  std::uint64_t messages_sent_total{}, send_failures_total{};
};
class ZmqPublisher {
public:
  ZmqPublisher(LiveQueue& queue, std::string endpoint, int sndhwm = 1000)
      : queue_(queue), endpoint_(std::move(endpoint)), sndhwm_(sndhwm) {
    if (sndhwm < 1) throw std::invalid_argument("ZMQ_SNDHWM must be positive");
  }
  ~ZmqPublisher();
  void start();
  void request_stop() noexcept;
  void join();
  void run();
  ZmqMetrics metrics() const { return {sent_, failures_}; }
private:
  LiveQueue& queue_;
  std::string endpoint_;
  int sndhwm_;
  std::atomic<bool> stop_{false};
  std::atomic<std::uint64_t> sent_{0}, failures_{0};
  std::thread thread_;
};
}
