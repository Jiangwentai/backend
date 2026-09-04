#include "market_data/dispatcher.hpp"
#include "market_data/producer_identity.hpp"
#include "market_data/questdb_writer.hpp"
#include "market_data/synthetic_generator.hpp"
#include "market_data/zmq_publisher.hpp"
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>

namespace {
std::string environment_or(const char* name, const char* fallback) {
  if (const char* value = std::getenv(name); value != nullptr && *value != '\0') return value;
  return fallback;
}
}

int main() {
  using namespace std::chrono_literals;
  market_data::SpscQueue<market_data::MarketTick> ingress(131072), persistence(131072);
  market_data::LiveQueue live(65536);
  market_data::ProducerIdentity identity;
  market_data::Dispatcher dispatcher(ingress, persistence, &live);
  market_data::QuestDbWriter writer(
      persistence, environment_or("QDB_CONNECTION", "ws::addr=localhost:9000;sf_dir=/tmp/qwp-combined;sender_id=combined-benchmark;sender_pool_min=1;sender_pool_max=1;"),
      5000, 20ms, 10s);
  market_data::ZmqPublisher publisher(
      live, environment_or("ZMQ_PUB_ENDPOINT", "tcp://127.0.0.1:15557"));
  market_data::SyntheticGenerator generator(ingress, identity, 10000, {"SHFE.zn2610"});
  writer.start(); publisher.start(); dispatcher.start();
  // Allow PUB/SUB subscription propagation so benchmark counts steady-state
  // transport rather than ZeroMQ's intentional slow-joiner loss.
  std::this_thread::sleep_for(300ms);
  generator.start();
  std::this_thread::sleep_for(3s);
  generator.request_stop(); generator.join();
  dispatcher.request_stop(); dispatcher.join();
  writer.request_stop(); publisher.request_stop(); writer.join(); publisher.join();
  const auto wm=writer.metrics(); const auto lm=live.metrics(); const auto pm=publisher.metrics();
  const auto generated=generator.generated();
  std::cout<<"generated="<<generated<<" questdb_accepted="<<wm.accepted_rows
           <<" questdb_healthy="<<wm.healthy<<" live_sent="<<pm.messages_sent_total
           <<" live_failures="<<pm.send_failures_total<<" live_high_water="<<lm.high_water_mark
           <<" live_dropped="<<lm.dropped_total
           <<" persistence_push_failed="<<persistence.metrics().push_failed_total<<'\n';
  return wm.healthy&&wm.accepted_rows==generated&&persistence.metrics().push_failed_total==0?0:1;
}
