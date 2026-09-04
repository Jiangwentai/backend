#pragma once
#include "market_data/ctp/types.hpp"
#include <chrono>
#include <optional>
namespace market_data::ctp {
std::optional<MarketTick> normalize(const DepthSnapshot&,std::int64_t recv_ts_ns,std::chrono::system_clock::time_point recv_time,const ProducerId&,std::uint64_t seq) noexcept;
}
