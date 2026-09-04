#pragma once
#include "market_data/config.hpp"
#include "market_data/dispatcher.hpp"
#include "market_data/producer_identity.hpp"
#include "market_data/questdb_writer.hpp"
#include "market_data/synthetic_generator.hpp"
#include "market_data/zmq_publisher.hpp"
#ifdef MD_ENABLE_CTP
#include "market_data/ctp/adapter.hpp"
#endif
#include <cstdint>
#include <memory>
namespace market_data {
struct InputMetrics {std::uint64_t received_total{},invalid_total{},rejected_total{};bool connected{true},ready{true};};
struct PipelineMetrics {QueueSnapshot ingress{},persistence{};LiveQueueMetrics live{};WriterMetrics questdb{};ZmqMetrics zeromq{};InputMetrics input{};bool dispatcher_degraded{};};
class Pipeline {public:explicit Pipeline(AppConfig config);~Pipeline();void start();void shutdown();[[nodiscard]]const ProducerIdentity&identity()const{return identity_;}[[nodiscard]]PipelineMetrics metrics()const noexcept;
 private:AppConfig config_;ProducerIdentity identity_;SpscQueue<MarketTick> ingress_,persistence_;LiveQueue live_;Dispatcher dispatcher_;std::unique_ptr<SyntheticGenerator> generator_;
#ifdef MD_ENABLE_CTP
 std::unique_ptr<ctp::MarketDataAdapter> ctp_adapter_;
#endif
 QuestDbWriter writer_;ZmqPublisher publisher_;bool started_{false};}; }
