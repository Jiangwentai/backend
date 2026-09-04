#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
namespace market_data {
struct AppConfig {
  std::string source{"synthetic"};
  std::size_t ingress_capacity{1'048'576};
  std::size_t persistence_capacity{1'048'576};
  std::size_t live_capacity{65'536};
  std::size_t max_batch_rows{500};
  std::uint32_t max_batch_latency_ms{20};
  std::uint32_t ack_timeout_ms{30'000};
  std::uint32_t synthetic_rate{10'000};
  std::vector<std::string> synthetic_symbols{"SHFE.zn2610"};
  std::string qdb_host{"127.0.0.1"};
  std::uint16_t qdb_port{9000};
  std::string qdb_sf_dir{"./data/qwp-spool"};
  std::string log_level{"info"};
  std::string zmq_pub_endpoint{"tcp://127.0.0.1:5556"};
  std::string ctp_front_address;
  std::string ctp_broker_id;
  std::string ctp_user_id;
  std::string ctp_password;
  std::string ctp_app_id;
  std::string ctp_auth_code;
  std::string ctp_flow_path{"./data/ctp-flow"};
  std::vector<std::string> ctp_subscriptions;
  bool ctp_authentication_required{false};
  [[nodiscard]] std::string qdb_connection_string(std::string_view producer_id) const;
  void validate() const;
};
AppConfig load_config(const std::string& path);
}
