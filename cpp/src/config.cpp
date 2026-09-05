#include "market_data/config.hpp"
#include <yaml-cpp/yaml.h>
#include <cstdlib>
#include <charconv>
#include <filesystem>
#include <stdexcept>
namespace market_data {
namespace {
int parse_hwm(const char* value) {
  const std::string_view text{value};
  int result{};
  const auto parsed = std::from_chars(text.data(), text.data() + text.size(), result);
  if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() || result < 1)
    throw std::invalid_argument("ZMQ_SNDHWM must be an integer in 1..2147483647");
  return result;
}
template<class T> void env(const char* k,T& out); template<> void env(const char* k,std::string& out){if(auto*p=std::getenv(k))out=p;} template<> void env(const char*k,std::size_t& out){if(auto*p=std::getenv(k))out=std::stoull(p);} template<> void env(const char*k,std::uint32_t& out){if(auto*p=std::getenv(k))out=static_cast<std::uint32_t>(std::stoul(p));} template<> void env(const char*k,std::uint16_t& out){if(auto*p=std::getenv(k))out=static_cast<std::uint16_t>(std::stoul(p));} template<> void env(const char*k,bool& out){if(auto*p=std::getenv(k)){std::string value{p};if(value=="1"||value=="true"||value=="TRUE")out=true;else if(value=="0"||value=="false"||value=="FALSE")out=false;else throw std::invalid_argument(std::string{k}+" must be true or false");}} }
void AppConfig::validate() const { if(zmq_sndhwm<1)throw std::invalid_argument("ZMQ_SNDHWM must be positive (zero is unbounded)"); if(source!="synthetic"&&source!="ctp")throw std::invalid_argument("source must be synthetic or ctp");const bool synthetic=providers_explicit?synthetic_enabled:source=="synthetic";const bool ctp=providers_explicit?ctp_enabled:source=="ctp";if(!synthetic&&!ctp)throw std::invalid_argument("at least one realtime provider must be enabled");if(ingress_capacity<1||persistence_capacity<1||live_capacity<1||max_batch_rows<1||max_batch_latency_ms<1||ack_timeout_ms<1) throw std::invalid_argument("queue/batch values must be positive"); if(synthetic&&(synthetic_rate<1||synthetic_rate>50'000)) throw std::invalid_argument("synthetic rate must be 1..50000"); if(synthetic&&synthetic_symbols.empty()) throw std::invalid_argument("at least one synthetic symbol is required");if(ctp&&(ctp_front_address.empty()||ctp_broker_id.empty()||ctp_user_id.empty()||ctp_password.empty()||ctp_subscriptions.empty()))throw std::invalid_argument("CTP source requires front, broker, user, password, and subscriptions");if(ctp&&ctp_authentication_required&&(ctp_app_id.empty()||ctp_auth_code.empty()))throw std::invalid_argument("CTP authentication requires app_id and auth_code"); if(qdb_host.empty()||qdb_sf_dir.empty()||zmq_pub_endpoint.empty()) throw std::invalid_argument("QDB and live endpoints are required"); }
std::string AppConfig::qdb_connection_string(std::string_view producer) const { return "ws::addr="+qdb_host+":"+std::to_string(qdb_port)+";sf_dir="+qdb_sf_dir+";sender_id="+std::string(producer)+";drain_orphans=on;sf_durability=periodic;lazy_connect=true;initial_connect_retry=async;sender_pool_min=1;sender_pool_max=1;"; }
AppConfig load_config(const std::string& path) {
  AppConfig c; auto y=YAML::LoadFile(path);
  if(y["source"])c.source=y["source"].as<std::string>();
  if(auto providers=y["providers"]){c.providers_explicit=true;if(providers["synthetic"]&&providers["synthetic"]["enabled"])c.synthetic_enabled=providers["synthetic"]["enabled"].as<bool>();if(providers["ctp"]&&providers["ctp"]["enabled"])c.ctp_enabled=providers["ctp"]["enabled"].as<bool>();}
  if(auto n=y["queues"]){if(n["ingress_capacity"])c.ingress_capacity=n["ingress_capacity"].as<std::size_t>();if(n["persistence_capacity"])c.persistence_capacity=n["persistence_capacity"].as<std::size_t>();if(n["live_capacity"])c.live_capacity=n["live_capacity"].as<std::size_t>();}
  if(auto n=y["questdb"]){if(n["host"])c.qdb_host=n["host"].as<std::string>();if(n["port"])c.qdb_port=n["port"].as<std::uint16_t>();if(n["sf_dir"])c.qdb_sf_dir=n["sf_dir"].as<std::string>();if(n["max_batch_rows"])c.max_batch_rows=n["max_batch_rows"].as<std::size_t>();if(n["max_batch_latency_ms"])c.max_batch_latency_ms=n["max_batch_latency_ms"].as<std::uint32_t>();if(n["ack_timeout_ms"])c.ack_timeout_ms=n["ack_timeout_ms"].as<std::uint32_t>();}
  if(auto n=y["synthetic"]){if(n["rate"])c.synthetic_rate=n["rate"].as<std::uint32_t>();if(n["symbols"])c.synthetic_symbols=n["symbols"].as<std::vector<std::string>>();}
  if(auto n=y["ctp"]){if(n["front_address"])c.ctp_front_address=n["front_address"].as<std::string>();if(n["broker_id"])c.ctp_broker_id=n["broker_id"].as<std::string>();if(n["user_id"])c.ctp_user_id=n["user_id"].as<std::string>();if(n["flow_path"])c.ctp_flow_path=n["flow_path"].as<std::string>();if(n["subscriptions"])c.ctp_subscriptions=n["subscriptions"].as<std::vector<std::string>>();if(n["authentication_required"])c.ctp_authentication_required=n["authentication_required"].as<bool>();}
  if(y["logging"]&&y["logging"]["level"])c.log_level=y["logging"]["level"].as<std::string>();
  if(y["live"]&&y["live"]["pub_endpoint"])c.zmq_pub_endpoint=y["live"]["pub_endpoint"].as<std::string>();
  if(y["live"]&&y["live"]["sndhwm"])c.zmq_sndhwm=y["live"]["sndhwm"].as<int>();
  if(const auto* value=std::getenv("ZMQ_SNDHWM"))c.zmq_sndhwm=parse_hwm(value);
  env("MARKET_DATA_SOURCE",c.source);if(std::getenv("SYNTHETIC_PROVIDER_ENABLED")||std::getenv("CTP_PROVIDER_ENABLED"))c.providers_explicit=true;env("SYNTHETIC_PROVIDER_ENABLED",c.synthetic_enabled);env("CTP_PROVIDER_ENABLED",c.ctp_enabled);env("QDB_HOST",c.qdb_host);env("QDB_PORT",c.qdb_port);env("QDB_SF_DIR",c.qdb_sf_dir);env("MAX_BATCH_ROWS",c.max_batch_rows);env("MAX_BATCH_LATENCY_MS",c.max_batch_latency_ms);env("INGRESS_QUEUE_CAPACITY",c.ingress_capacity);env("PERSISTENCE_QUEUE_CAPACITY",c.persistence_capacity);env("LIVE_QUEUE_CAPACITY",c.live_capacity);env("SYNTHETIC_RATE",c.synthetic_rate);env("LOG_LEVEL",c.log_level);env("ZMQ_PUB_ENDPOINT",c.zmq_pub_endpoint);env("CTP_FRONT_ADDRESS",c.ctp_front_address);env("CTP_BROKER_ID",c.ctp_broker_id);env("CTP_USER_ID",c.ctp_user_id);env("CTP_PASSWORD",c.ctp_password);env("CTP_APP_ID",c.ctp_app_id);env("CTP_AUTH_CODE",c.ctp_auth_code);env("CTP_FLOW_PATH",c.ctp_flow_path);env("CTP_AUTHENTICATION_REQUIRED",c.ctp_authentication_required);
  c.validate();std::filesystem::create_directories(c.qdb_sf_dir);if((c.providers_explicit&&c.ctp_enabled)||(!c.providers_explicit&&c.source=="ctp"))std::filesystem::create_directories(c.ctp_flow_path);return c;
}
}
