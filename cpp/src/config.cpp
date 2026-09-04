#include "market_data/config.hpp"
#include <yaml-cpp/yaml.h>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
namespace market_data {
namespace { template<class T> void env(const char* k,T& out); template<> void env(const char* k,std::string& out){if(auto*p=std::getenv(k))out=p;} template<> void env(const char*k,std::size_t& out){if(auto*p=std::getenv(k))out=std::stoull(p);} template<> void env(const char*k,std::uint32_t& out){if(auto*p=std::getenv(k))out=static_cast<std::uint32_t>(std::stoul(p));} template<> void env(const char*k,std::uint16_t& out){if(auto*p=std::getenv(k))out=static_cast<std::uint16_t>(std::stoul(p));} }
void AppConfig::validate() const { if(ingress_capacity<1||persistence_capacity<1||live_capacity<1||max_batch_rows<1||max_batch_latency_ms<1||ack_timeout_ms<1) throw std::invalid_argument("queue/batch values must be positive"); if(synthetic_rate<1||synthetic_rate>50'000) throw std::invalid_argument("synthetic rate must be 1..50000"); if(synthetic_symbols.empty()) throw std::invalid_argument("at least one synthetic symbol is required"); if(qdb_host.empty()||qdb_sf_dir.empty()||zmq_pub_endpoint.empty()) throw std::invalid_argument("QDB and live endpoints are required"); }
std::string AppConfig::qdb_connection_string(std::string_view producer) const { return "ws::addr="+qdb_host+":"+std::to_string(qdb_port)+";sf_dir="+qdb_sf_dir+";sender_id="+std::string(producer)+";drain_orphans=on;sf_durability=periodic;lazy_connect=true;initial_connect_retry=async;sender_pool_min=1;sender_pool_max=1;"; }
AppConfig load_config(const std::string& path) {
  AppConfig c; auto y=YAML::LoadFile(path);
  if(auto n=y["queues"]){if(n["ingress_capacity"])c.ingress_capacity=n["ingress_capacity"].as<std::size_t>();if(n["persistence_capacity"])c.persistence_capacity=n["persistence_capacity"].as<std::size_t>();if(n["live_capacity"])c.live_capacity=n["live_capacity"].as<std::size_t>();}
  if(auto n=y["questdb"]){if(n["host"])c.qdb_host=n["host"].as<std::string>();if(n["port"])c.qdb_port=n["port"].as<std::uint16_t>();if(n["sf_dir"])c.qdb_sf_dir=n["sf_dir"].as<std::string>();if(n["max_batch_rows"])c.max_batch_rows=n["max_batch_rows"].as<std::size_t>();if(n["max_batch_latency_ms"])c.max_batch_latency_ms=n["max_batch_latency_ms"].as<std::uint32_t>();if(n["ack_timeout_ms"])c.ack_timeout_ms=n["ack_timeout_ms"].as<std::uint32_t>();}
  if(auto n=y["synthetic"]){if(n["rate"])c.synthetic_rate=n["rate"].as<std::uint32_t>();if(n["symbols"])c.synthetic_symbols=n["symbols"].as<std::vector<std::string>>();}
  if(y["logging"]&&y["logging"]["level"])c.log_level=y["logging"]["level"].as<std::string>();
  if(y["live"]&&y["live"]["pub_endpoint"])c.zmq_pub_endpoint=y["live"]["pub_endpoint"].as<std::string>();
  env("QDB_HOST",c.qdb_host);env("QDB_PORT",c.qdb_port);env("QDB_SF_DIR",c.qdb_sf_dir);env("MAX_BATCH_ROWS",c.max_batch_rows);env("MAX_BATCH_LATENCY_MS",c.max_batch_latency_ms);env("INGRESS_QUEUE_CAPACITY",c.ingress_capacity);env("PERSISTENCE_QUEUE_CAPACITY",c.persistence_capacity);env("LIVE_QUEUE_CAPACITY",c.live_capacity);env("SYNTHETIC_RATE",c.synthetic_rate);env("LOG_LEVEL",c.log_level);env("ZMQ_PUB_ENDPOINT",c.zmq_pub_endpoint);
  std::filesystem::create_directories(c.qdb_sf_dir); c.validate(); return c;
}
}
