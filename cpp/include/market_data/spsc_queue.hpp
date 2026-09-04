#pragma once
#include <atomic>
#include <cstddef>
#include <memory>
#include <stdexcept>
namespace market_data {
struct QueueSnapshot { std::size_t capacity{}, size{}, high_water_mark{}; double usage_ratio{}; std::uint64_t push_total{}, pop_total{}, push_failed_total{}; };
template <typename T> class SpscQueue {
 public:
  explicit SpscQueue(std::size_t capacity) : capacity_(capacity), slots_(std::make_unique<T[]>(capacity+1)) { if(capacity<1) throw std::invalid_argument("capacity must be positive"); }
  bool try_push(const T& value) noexcept {
    const auto h=head_.load(std::memory_order_relaxed), n=(h+1)%(capacity_+1); if(n==tail_.load(std::memory_order_acquire)){failed_.fetch_add(1);return false;}
    slots_[h]=value; head_.store(n,std::memory_order_release); pushes_.fetch_add(1); update_high(size()); return true;
  }
  bool try_pop(T& out) noexcept { const auto t=tail_.load(std::memory_order_relaxed); if(t==head_.load(std::memory_order_acquire)) return false; out=slots_[t]; tail_.store((t+1)%(capacity_+1),std::memory_order_release); pops_.fetch_add(1); return true; }
  [[nodiscard]] std::size_t size() const noexcept { auto h=head_.load(std::memory_order_acquire),t=tail_.load(std::memory_order_acquire); return h>=t?h-t:capacity_+1-t+h; }
  [[nodiscard]] bool empty() const noexcept { return size()==0; }
  [[nodiscard]] QueueSnapshot metrics() const noexcept { auto s=size(); return {capacity_,s,high_.load(),static_cast<double>(s)/static_cast<double>(capacity_),pushes_.load(),pops_.load(),failed_.load()}; }
 private:
  void update_high(std::size_t v) noexcept { auto old=high_.load(); while(v>old&&!high_.compare_exchange_weak(old,v)){} }
  const std::size_t capacity_; std::unique_ptr<T[]> slots_; alignas(64) std::atomic<std::size_t> head_{0}; alignas(64) std::atomic<std::size_t> tail_{0};
  std::atomic<std::uint64_t> pushes_{0},pops_{0},failed_{0}; std::atomic<std::size_t> high_{0};
};
}
