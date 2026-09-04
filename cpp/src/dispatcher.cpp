#include "market_data/dispatcher.hpp"
#include <spdlog/spdlog.h>
#include <chrono>
#include <stdexcept>
namespace market_data {
Dispatcher::~Dispatcher(){request_stop();join();} void Dispatcher::start(){if(ingresses_.empty())throw std::logic_error("dispatcher requires at least one ingress");thread_=std::thread(&Dispatcher::run,this);} void Dispatcher::request_stop()noexcept{stopping_=true;} void Dispatcher::join(){if(thread_.joinable())thread_.join();}
void Dispatcher::add_ingress(SpscQueue<MarketTick>& ingress){if(thread_.joinable())throw std::logic_error("cannot add ingress after dispatcher start");ingresses_.push_back(&ingress);}
bool Dispatcher::ingresses_empty()const{for(const auto* ingress:ingresses_)if(!ingress->empty())return false;return true;}
void Dispatcher::run(){MarketTick t;bool pressure_logged=false;std::size_t next=0;while(!stopping_.load()||!ingresses_empty()){bool found=false;for(std::size_t checked=0;checked<ingresses_.size();++checked){auto& ingress=*ingresses_[(next+checked)%ingresses_.size()];if(ingress.try_pop(t)){next=(next+checked+1)%ingresses_.size();found=true;break;}}if(!found){std::this_thread::yield();continue;}while(!persistence_.try_push(t)){degraded_=true;if(!pressure_logged){const auto m=persistence_.metrics();spdlog::critical("persistence_queue_full size={} capacity={}",m.size,m.capacity);pressure_logged=true;}if(stopping_.load())std::this_thread::yield();else std::this_thread::sleep_for(std::chrono::milliseconds(1));}pressure_logged=false;if(live_)live_->try_push_latest(t);}}
}
