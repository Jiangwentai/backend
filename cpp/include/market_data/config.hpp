#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
namespace market_data {
struct AppConfig {
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
  [[nodiscard]] std::string qdb_connection_string(std::string_view producer_id) const;
  void validate() const;
};
AppConfig load_config(const std::string& path);
}
