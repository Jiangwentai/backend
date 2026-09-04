#pragma once
#include "market_data/market_tick.hpp"
#include <atomic>
#include <deque>
#include <mutex>
#include <stdexcept>
namespace market_data {
struct LiveQueueMetrics{std::size_t size{},capacity{},high_water_mark{};std::uint64_t dropped_total{},push_total{},pop_total{};};
class LiveQueue{
 public:explicit LiveQueue(std::size_t capacity):capacity_(capacity){if(!capacity)throw std::invalid_argument("live capacity must be positive");}
 bool try_push_latest(const MarketTick&t)noexcept{std::scoped_lock lock(mutex_);if(queue_.size()==capacity_){queue_.pop_front();++dropped_;}queue_.push_back(t);++pushes_;auto h=high_.load();while(queue_.size()>h&&!high_.compare_exchange_weak(h,queue_.size())){}return true;}
 bool try_pop(MarketTick&t)noexcept{std::unique_lock lock(mutex_,std::try_to_lock);if(!lock.owns_lock()||queue_.empty())return false;t=queue_.front();queue_.pop_front();++pops_;return true;}
 [[nodiscard]]bool empty()const{std::scoped_lock lock(mutex_);return queue_.empty();}
 [[nodiscard]]LiveQueueMetrics metrics()const{std::scoped_lock lock(mutex_);return{queue_.size(),capacity_,high_,dropped_,pushes_,pops_};}
 private:const std::size_t capacity_;mutable std::mutex mutex_;std::deque<MarketTick>queue_;std::atomic<std::size_t>high_{0};std::atomic<std::uint64_t>dropped_{0},pushes_{0},pops_{0};
};}
