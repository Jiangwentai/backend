#include "market_data/pipeline.hpp"
#include <spdlog/spdlog.h>
#include <utility>
namespace market_data {
Pipeline::Pipeline(AppConfig c):config_(std::move(c)),ingress_(config_.ingress_capacity),persistence_(config_.persistence_capacity),live_(config_.live_capacity),dispatcher_(ingress_,persistence_,&live_),generator_(ingress_,identity_,config_.synthetic_rate,config_.synthetic_symbols),writer_(persistence_,config_.qdb_connection_string(identity_.id().view()),config_.max_batch_rows,std::chrono::milliseconds(config_.max_batch_latency_ms),std::chrono::milliseconds(config_.ack_timeout_ms)),publisher_(live_,config_.zmq_pub_endpoint){}
Pipeline::~Pipeline(){shutdown();}void Pipeline::start(){if(started_)return;writer_.start();publisher_.start();dispatcher_.start();generator_.start();started_=true;spdlog::info("pipeline_started producer_id={} zmq_endpoint={}",identity_.id().view(),config_.zmq_pub_endpoint);}
void Pipeline::shutdown(){if(!started_)return;generator_.request_stop();generator_.join();dispatcher_.request_stop();dispatcher_.join();writer_.request_stop();publisher_.request_stop();writer_.join();publisher_.join();started_=false;auto lm=live_.metrics();auto zm=publisher_.metrics();spdlog::info("pipeline_stopped generated={} live_sent={} live_dropped={} live_high_water={}",generator_.generated(),zm.messages_sent_total,lm.dropped_total,lm.high_water_mark);}
}
