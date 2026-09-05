#include "market_data/pipeline.hpp"
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <utility>
namespace market_data {
Pipeline::Pipeline(AppConfig c):config_(std::move(c)),persistence_(config_.persistence_capacity),live_(config_.live_capacity),dispatcher_(persistence_,&live_),writer_(persistence_,config_.qdb_connection_string(identity_.id().view()),config_.max_batch_rows,std::chrono::milliseconds(config_.max_batch_latency_ms),std::chrono::milliseconds(config_.ack_timeout_ms)),publisher_(live_,config_.zmq_pub_endpoint,config_.zmq_sndhwm){
 const bool synthetic=config_.providers_explicit?config_.synthetic_enabled:config_.source=="synthetic";const bool ctp=config_.providers_explicit?config_.ctp_enabled:config_.source=="ctp";
 auto add_boundary=[this](){provider_identities_.push_back(std::make_unique<ProducerIdentity>());ingresses_.push_back(std::make_unique<SpscQueue<MarketTick>>(config_.ingress_capacity));dispatcher_.add_ingress(*ingresses_.back());};
 if(synthetic){add_boundary();auto provider=std::make_unique<SyntheticGenerator>(*ingresses_.back(),*provider_identities_.back(),config_.synthetic_rate,config_.synthetic_symbols);generator_=provider.get();providers_.add(std::move(provider));}
 if(ctp) {
#ifdef MD_ENABLE_CTP
  add_boundary();ctp::Config ctp_config{config_.ctp_front_address,config_.ctp_broker_id,config_.ctp_user_id,config_.ctp_password,config_.ctp_app_id,config_.ctp_auth_code,config_.ctp_flow_path,config_.ctp_subscriptions,config_.ctp_authentication_required};auto provider=std::make_unique<ctp::MarketDataAdapter>(*ingresses_.back(),*provider_identities_.back(),std::move(ctp_config));ctp_adapter_=provider.get();providers_.add(std::move(provider));
#else
  throw std::runtime_error("CTP provider requested, but this binary was built with ENABLE_CTP=OFF");
#endif
 }
}
Pipeline::~Pipeline(){shutdown();}void Pipeline::start(){if(started_)return;writer_.start();publisher_.start();dispatcher_.start();try{providers_.start_all();}catch(...){dispatcher_.request_stop();dispatcher_.join();writer_.request_stop();publisher_.request_stop();writer_.join();publisher_.join();throw;}started_=true;spdlog::info("pipeline_started providers={} producer_id={} zmq_endpoint={}",providers_.size(),identity_.id().view(),config_.zmq_pub_endpoint);}
PipelineMetrics Pipeline::metrics()const noexcept{InputMetrics input;
 if(generator_)input.received_total=generator_->generated();
#ifdef MD_ENABLE_CTP
 if(ctp_adapter_){const auto value=ctp_adapter_->metrics();input.received_total+=value.ticks_received_total;input.invalid_total+=value.invalid_ticks_total;input.rejected_total+=value.ingress_rejected_total;input.connected=input.connected&&value.connected;input.ready=input.ready&&value.ready;}
#endif
 QueueSnapshot ingress;for(const auto& queue:ingresses_){const auto value=queue->metrics();ingress.size+=value.size;ingress.capacity+=value.capacity;ingress.high_water_mark+=value.high_water_mark;ingress.push_total+=value.push_total;ingress.pop_total+=value.pop_total;ingress.push_failed_total+=value.push_failed_total;}ingress.usage_ratio=ingress.capacity==0?0.0:static_cast<double>(ingress.size)/static_cast<double>(ingress.capacity);
 return{ingress,persistence_.metrics(),live_.metrics(),writer_.metrics(),publisher_.metrics(),input,dispatcher_.degraded()};}
void Pipeline::shutdown(){if(!started_)return;
providers_.stop_all();dispatcher_.request_stop();dispatcher_.join();writer_.request_stop();publisher_.request_stop();writer_.join();publisher_.join();started_=false;const auto m=metrics();spdlog::info("pipeline_stopped input_received={} input_invalid={} input_rejected={} ingress_push={} ingress_failed={} ingress_high_water={} persistence_push={} persistence_failed={} persistence_high_water={} dispatcher_degraded={} questdb_accepted={} questdb_acked_batches={} questdb_failures={} questdb_last_flush_us={} questdb_healthy={} live_push={} live_sent={} live_dropped={} live_high_water={} zmq_failures={}",m.input.received_total,m.input.invalid_total,m.input.rejected_total,m.ingress.push_total,m.ingress.push_failed_total,m.ingress.high_water_mark,m.persistence.push_total,m.persistence.push_failed_total,m.persistence.high_water_mark,m.dispatcher_degraded,m.questdb.accepted_rows,m.questdb.acked_batches,m.questdb.failures,m.questdb.last_flush_us,m.questdb.healthy,m.live.push_total,m.zeromq.messages_sent_total,m.live.dropped_total,m.live.high_water_mark,m.zeromq.send_failures_total);}
}
