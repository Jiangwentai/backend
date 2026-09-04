#pragma once
#include "market_data/market_tick.hpp"
#include "market_data/spsc_queue.hpp"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
namespace market_data {
class BatchPolicy {
 public: BatchPolicy(std::size_t rows,std::chrono::milliseconds latency):rows_(rows),latency_(latency){} [[nodiscard]]bool should_flush(std::size_t rows,std::chrono::steady_clock::duration age)const noexcept{return rows>=rows_||(rows>0&&age>=latency_);}
 private:std::size_t rows_;std::chrono::milliseconds latency_;
};
struct WriterMetrics {std::uint64_t accepted_rows{},acked_batches{},failures{};std::int64_t last_flush_us{};bool healthy{true};};
class QuestDbWriter {
 public:QuestDbWriter(SpscQueue<MarketTick>&queue,std::string connection,std::size_t batch_rows,std::chrono::milliseconds latency,std::chrono::milliseconds ack_timeout);~QuestDbWriter();void start();void request_stop()noexcept;void join();void run();[[nodiscard]]WriterMetrics metrics()const noexcept;
 private:struct Impl;SpscQueue<MarketTick>&queue_;std::string connection_;BatchPolicy policy_;std::chrono::milliseconds ack_timeout_;std::atomic<bool>stop_{false};std::thread thread_;std::atomic<std::uint64_t>accepted_{0},acked_{0},failures_{0};std::atomic<std::int64_t>flush_us_{0};std::atomic<bool>healthy_{true};
}; }

