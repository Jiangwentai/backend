#pragma once
#include "market_data/ctp/types.hpp"
#include "market_data/producer_identity.hpp"
#include "market_data/spsc_queue.hpp"
#include <chrono>
#include <optional>
#include <string_view>
namespace market_data::ctp {
std::optional<MarketTick> normalize(const DepthSnapshot&,std::int64_t recv_ts_ns,std::chrono::system_clock::time_point recv_time,const ProducerId&,std::uint64_t seq,std::string_view fallback_exchange={}) noexcept;
enum class IngressResult {accepted,invalid,queue_full};
IngressResult normalize_and_enqueue(const DepthSnapshot&,std::int64_t recv_ts_ns,std::chrono::system_clock::time_point recv_time,ProducerIdentity&,SpscQueue<MarketTick>&,std::string_view fallback_exchange={})noexcept;
}
