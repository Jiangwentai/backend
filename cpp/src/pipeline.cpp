#include "market_data/pipeline.hpp"
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <utility>
namespace market_data {
Pipeline::Pipeline(AppConfig c):config_(std::move(c)),ingress_(config_.ingress_capacity),persistence_(config_.persistence_capacity),live_(config_.live_capacity),dispatcher_(ingress_,persistence_,&live_),writer_(persistence_,config_.qdb_connection_string(identity_.id().view()),config_.max_batch_rows,std::chrono::milliseconds(config_.max_batch_latency_ms),std::chrono::milliseconds(config_.ack_timeout_ms)),publisher_(live_,config_.zmq_pub_endpoint){
 if(config_.source=="synthetic")generator_=std::make_unique<SyntheticGenerator>(ingress_,identity_,config_.synthetic_rate,config_.synthetic_symbols);
 else {
#ifdef MD_ENABLE_CTP
  ctp::Config ctp_config{config_.ctp_front_address,config_.ctp_broker_id,config_.ctp_user_id,config_.ctp_password,config_.ctp_app_id,config_.ctp_auth_code,config_.ctp_flow_path,config_.ctp_subscriptions,config_.ctp_authentication_required};ctp_adapter_=std::make_unique<ctp::MarketDataAdapter>(ingress_,identity_,std::move(ctp_config));
#else
  throw std::runtime_error("CTP source requested, but this binary was built with ENABLE_CTP=OFF");
#endif
 }
}
Pipeline::~Pipeline(){shutdown();}void Pipeline::start(){if(started_)return;writer_.start();publisher_.start();dispatcher_.start();if(generator_)generator_->start();
#ifdef MD_ENABLE_CTP
if(ctp_adapter_)ctp_adapter_->start();
#endif
started_=true;spdlog::info("pipeline_started source={} producer_id={} zmq_endpoint={}",config_.source,identity_.id().view(),config_.zmq_pub_endpoint);}
PipelineMetrics Pipeline::metrics()const noexcept{InputMetrics input;
 if(generator_)input.received_total=generator_->generated();
#ifdef MD_ENABLE_CTP
 if(ctp_adapter_){const auto value=ctp_adapter_->metrics();input={value.ticks_received_total,value.invalid_ticks_total,value.ingress_rejected_total,value.connected,value.ready};}
#endif
 return{ingress_.metrics(),persistence_.metrics(),live_.metrics(),writer_.metrics(),publisher_.metrics(),input,dispatcher_.degraded()};}
void Pipeline::shutdown(){if(!started_)return;
#ifdef MD_ENABLE_CTP
if(ctp_adapter_)ctp_adapter_->stop();
#endif
if(generator_){generator_->request_stop();generator_->join();}dispatcher_.request_stop();dispatcher_.join();writer_.request_stop();publisher_.request_stop();writer_.join();publisher_.join();started_=false;const auto m=metrics();spdlog::info("pipeline_stopped input_received={} input_invalid={} input_rejected={} ingress_push={} ingress_failed={} ingress_high_water={} persistence_push={} persistence_failed={} persistence_high_water={} dispatcher_degraded={} questdb_accepted={} questdb_acked_batches={} questdb_failures={} questdb_last_flush_us={} questdb_healthy={} live_push={} live_sent={} live_dropped={} live_high_water={} zmq_failures={}",m.input.received_total,m.input.invalid_total,m.input.rejected_total,m.ingress.push_total,m.ingress.push_failed_total,m.ingress.high_water_mark,m.persistence.push_total,m.persistence.push_failed_total,m.persistence.high_water_mark,m.dispatcher_degraded,m.questdb.accepted_rows,m.questdb.acked_batches,m.questdb.failures,m.questdb.last_flush_us,m.questdb.healthy,m.live.push_total,m.zeromq.messages_sent_total,m.live.dropped_total,m.live.high_water_mark,m.zeromq.send_failures_total);}
}
