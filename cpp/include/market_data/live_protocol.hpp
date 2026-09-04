#pragma once
#include "market_data/market_tick.hpp"
#include <cstdint>
#include <string>
#include <vector>
namespace market_data {inline constexpr std::uint32_t live_schema_version=2;std::string live_topic(const MarketTick&);std::vector<std::uint8_t>encode_live_tick(const MarketTick&);MarketTick decode_live_tick(const std::uint8_t*,std::size_t);}
