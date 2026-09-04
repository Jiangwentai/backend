#include "market_data/instrument_mapping.hpp"
#include <stdexcept>

namespace market_data {
namespace {std::string key(ProviderId provider,std::string_view symbol){return std::string{to_string(provider)}+":"+std::string{symbol};}}
void InstrumentMapping::add(ProviderInstrument mapping){if(mapping.instrument_id.empty()||mapping.provider_symbol.empty())throw std::invalid_argument("instrument mapping requires canonical and provider symbols");const auto [_,inserted]=mappings_.try_emplace(key(mapping.provider,mapping.provider_symbol),std::move(mapping));if(!inserted)throw std::invalid_argument("duplicate provider instrument mapping");}
const ProviderInstrument* InstrumentMapping::by_provider_symbol(ProviderId provider,std::string_view symbol)const noexcept{const auto found=mappings_.find(key(provider,symbol));return found==mappings_.end()?nullptr:&found->second;}
}
