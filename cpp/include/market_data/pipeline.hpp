#pragma once
#include "market_data/config.hpp"
#include "market_data/dispatcher.hpp"
#include "market_data/producer_identity.hpp"
#include "market_data/provider_manager.hpp"
#include "market_data/questdb_writer.hpp"
#include "market_data/synthetic_generator.hpp"
#include "market_data/zmq_publisher.hpp"
#ifdef MD_ENABLE_CTP
#include "market_data/ctp/adapter.hpp"
#endif
#include <cstdint>
#include <memory>
#include <vector>
namespace market_data {
struct InputMetrics {std::uint64_t received_total{},invalid_total{},rejected_total{};bool connected{true},ready{true};};
struct PipelineMetrics {QueueSnapshot ingress{},persistence{};LiveQueueMetrics live{};WriterMetrics questdb{};ZmqMetrics zeromq{};InputMetrics input{};bool dispatcher_degraded{};};
class Pipeline {public:explicit Pipeline(AppConfig config);~Pipeline();void start();void shutdown();[[nodiscard]]const ProducerIdentity&identity()const{return identity_;}[[nodiscard]]PipelineMetrics metrics()const noexcept;
 private:AppConfig config_;ProducerIdentity identity_;std::vector<std::unique_ptr<ProducerIdentity>>provider_identities_;std::vector<std::unique_ptr<SpscQueue<MarketTick>>>ingresses_;SpscQueue<MarketTick> persistence_;LiveQueue live_;Dispatcher dispatcher_;ProviderManager providers_;SyntheticGenerator* generator_{};
#ifdef MD_ENABLE_CTP
 ctp::MarketDataAdapter* ctp_adapter_{};
#endif
 QuestDbWriter writer_;ZmqPublisher publisher_;bool started_{false};}; }
