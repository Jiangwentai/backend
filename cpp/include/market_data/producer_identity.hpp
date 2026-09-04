#pragma once
#include "market_data/market_tick.hpp"
#include <atomic>
namespace market_data {
class ProducerIdentity {
 public:
  ProducerIdentity();
  explicit ProducerIdentity(ProducerId id) : id_(id) {}
  [[nodiscard]] const ProducerId& id() const noexcept { return id_; }
  [[nodiscard]] std::uint64_t next_seq() noexcept { return seq_.fetch_add(1, std::memory_order_relaxed) + 1; }
 private: ProducerId id_; std::atomic<std::uint64_t> seq_{0};
};
}

