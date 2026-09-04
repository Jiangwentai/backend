#include "market_data/provider_manager.hpp"
#include <stdexcept>

namespace market_data {
void ProviderManager::add(std::unique_ptr<IRealtimeMarketDataProvider> provider){if(!provider)throw std::invalid_argument("provider must not be null");for(const auto& existing:providers_)if(existing->id()==provider->id())throw std::invalid_argument("provider already registered");providers_.push_back(std::move(provider));}
void ProviderManager::start_all(){std::size_t started=0;try{for(;started<providers_.size();++started)providers_[started]->start();}catch(...){while(started>0)providers_[--started]->stop();throw;}}
void ProviderManager::stop_all()noexcept{for(auto it=providers_.rbegin();it!=providers_.rend();++it)(*it)->stop();}
void ProviderManager::subscribe(ProviderId provider,const std::vector<Subscription>& subscriptions){for(auto& value:providers_)if(value->id()==provider){value->subscribe(subscriptions);return;}throw std::out_of_range("provider is not registered");}
std::vector<ProviderHealth> ProviderManager::health()const{std::vector<ProviderHealth> result;result.reserve(providers_.size());for(const auto& provider:providers_)result.push_back(provider->health());return result;}
std::vector<ProviderCapabilities> ProviderManager::capabilities()const{std::vector<ProviderCapabilities> result;result.reserve(providers_.size());for(const auto& provider:providers_)result.push_back(provider->capabilities());return result;}
}
