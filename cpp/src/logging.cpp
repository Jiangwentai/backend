#include "market_data/logging.hpp"
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
namespace market_data { void init_logging(std::string_view level) { auto log=spdlog::stdout_color_mt("market_data"); spdlog::set_default_logger(log); spdlog::set_pattern(R"({"ts":"%Y-%m-%dT%H:%M:%S.%e%z","level":"%l","thread":%t,"message":"%v"})"); spdlog::set_level(spdlog::level::from_str(std::string(level))); } }

