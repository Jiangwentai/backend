#pragma once
#include "market_data/live_queue.hpp"
#include <atomic>
#include <string>
#include <thread>
#include <utility>
namespace market_data {struct ZmqMetrics{std::uint64_t messages_sent_total{},send_failures_total{};};class ZmqPublisher{public:ZmqPublisher(LiveQueue&q,std::string endpoint):queue_(q),endpoint_(std::move(endpoint)){}~ZmqPublisher();void start();void request_stop()noexcept;void join();void run();ZmqMetrics metrics()const{return{sent_,failures_};}private:LiveQueue&queue_;std::string endpoint_;std::atomic<bool>stop_{false};std::atomic<std::uint64_t>sent_{0},failures_{0};std::thread thread_;};}
