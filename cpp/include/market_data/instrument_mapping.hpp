#pragma once

#include "market_data/provider.hpp"
#include <string>
#include <string_view>
#include <unordered_map>

namespace market_data {
struct ProviderInstrument {
  ProviderId provider{ProviderId::synthetic};
  std::string instrument_id;
  std::string provider_symbol;
  std::string provider_instrument_id;
  std::string exchange_code;
};

class InstrumentMapping {
 public:
  void add(ProviderInstrument mapping);
  [[nodiscard]]const ProviderInstrument* by_provider_symbol(ProviderId provider,std::string_view symbol)const noexcept;
 private:std::unordered_map<std::string,ProviderInstrument>mappings_;
};
}
