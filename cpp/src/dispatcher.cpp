#include "market_data/dispatcher.hpp"
#include <spdlog/spdlog.h>
#include <chrono>
namespace market_data {
Dispatcher::~Dispatcher(){request_stop();join();} void Dispatcher::start(){thread_=std::thread(&Dispatcher::run,this);} void Dispatcher::request_stop()noexcept{stopping_=true;} void Dispatcher::join(){if(thread_.joinable())thread_.join();}
void Dispatcher::run(){MarketTick t;bool pressure_logged=false;while(!stopping_.load()||!ingress_.empty()){if(!ingress_.try_pop(t)){std::this_thread::yield();continue;}while(!persistence_.try_push(t)){degraded_=true;if(!pressure_logged){const auto m=persistence_.metrics();spdlog::critical("persistence_queue_full size={} capacity={}",m.size,m.capacity);pressure_logged=true;}if(stopping_.load())std::this_thread::yield();else std::this_thread::sleep_for(std::chrono::milliseconds(1));}pressure_logged=false;if(live_)live_->try_push_latest(t);}}
}
