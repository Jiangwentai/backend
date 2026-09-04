#pragma once
#include "market_data/config.hpp"
#include "market_data/dispatcher.hpp"
#include "market_data/producer_identity.hpp"
#include "market_data/questdb_writer.hpp"
#include "market_data/synthetic_generator.hpp"
#include "market_data/zmq_publisher.hpp"
#include <memory>
namespace market_data {
class Pipeline {public:explicit Pipeline(AppConfig config);~Pipeline();void start();void shutdown();[[nodiscard]]const ProducerIdentity&identity()const{return identity_;}
 private:AppConfig config_;ProducerIdentity identity_;SpscQueue<MarketTick> ingress_,persistence_;LiveQueue live_;Dispatcher dispatcher_;SyntheticGenerator generator_;QuestDbWriter writer_;ZmqPublisher publisher_;bool started_{false};}; }
