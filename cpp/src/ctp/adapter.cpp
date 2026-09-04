#include "market_data/ctp/adapter.hpp"
#include "market_data/ctp/normalizer.hpp"
#include "market_data/ctp/state_machine.hpp"
#include <ThostFtdcMdApi.h>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace market_data::ctp {
namespace {
template <std::size_t N> void copy_text(char (&destination)[N], const std::string& source) noexcept {
  std::memset(destination, 0, N);
  std::memcpy(destination, source.data(), std::min(source.size(), N - 1));
}
template <std::size_t N> std::string_view field_view(const char (&field)[N]) noexcept {
  std::size_t size=0;while(size<N&&field[size]!='\0')++size;return {field,size};
}
bool response_ok(const CThostFtdcRspInfoField* info) noexcept { return info == nullptr || info->ErrorID == 0; }
struct TransparentStringHash {
  using is_transparent = void;
  std::size_t operator()(std::string_view value) const noexcept { return std::hash<std::string_view>{}(value); }
  std::size_t operator()(const std::string& value) const noexcept { return (*this)(std::string_view{value}); }
};
}

struct MarketDataAdapter::Impl final : CThostFtdcMdSpi {
  Impl(SpscQueue<MarketTick>& queue, ProducerIdentity& identity, Config value)
      : queue_(queue), identity_(identity), config_(std::move(value)), machine_(config_.authentication_required) {
    for (const auto& subscription : config_.subscriptions) {
      const auto dot = subscription.find('.');
      const auto instrument = dot == std::string::npos ? subscription : subscription.substr(dot + 1);
      const auto exchange = dot == std::string::npos ? std::string{} : subscription.substr(0, dot);
      if (!instrument.empty() && !exchange.empty()) {
        const auto [entry, inserted] = exchange_by_instrument_.try_emplace(instrument, exchange);
        if (!inserted && entry->second != exchange) {
          throw std::invalid_argument("CTP instrument is configured with conflicting exchanges: " + instrument);
        }
      }
      if (!instrument.empty() && desired_set_.insert(instrument).second) desired_.push_back(instrument);
    }
    if (desired_.empty()) throw std::invalid_argument("CTP subscriptions contain no instrument IDs");
#ifndef MD_CTP_HAS_AUTHENTICATE
    if (config_.authentication_required) throw std::runtime_error("supplied CTP SDK does not expose ReqAuthenticate");
#endif
  }

  ~Impl() { stop(); }

  void set_state(State state) noexcept {
    state_.store(state, std::memory_order_release);
    spdlog::info("ctp_state state={}", state_name(state));
  }

  void start() {
    if (api_ != nullptr) return;
    std::filesystem::create_directories(config_.flow_path);
    machine_.start();
    set_state(machine_.state());
    api_ = CThostFtdcMdApi::CreateFtdcMdApi(config_.flow_path.c_str());
    if (api_ == nullptr) throw std::runtime_error("CThostFtdcMdApi::CreateFtdcMdApi returned null");
    api_->RegisterSpi(this);
    front_address_ = config_.front_address;
    api_->RegisterFront(front_address_.data());
    spdlog::info("ctp_api_start front={}", config_.front_address);
    api_->Init();
  }

  void stop() noexcept {
    auto* api = std::exchange(api_, nullptr);
    if (api == nullptr) return;
    api->RegisterSpi(nullptr);
    api->Release();
    connected_.store(false);
    logged_in_.store(false);
    ready_.store(false);
    set_state(State::disconnected);
    spdlog::info("ctp_api_stopped");
  }

  void request_login() noexcept {
    CThostFtdcReqUserLoginField request{};
    copy_text(request.BrokerID, config_.broker_id);
    copy_text(request.UserID, config_.user_id);
    copy_text(request.Password, config_.password);
    spdlog::info("ctp_login_attempt user_id={}", config_.user_id);
    if (api_->ReqUserLogin(&request, next_request_id()) != 0) {
      ++login_failures_;
      set_state(State::error);
      spdlog::error("ctp_login_request_rejected");
    }
  }

#ifdef MD_CTP_HAS_AUTHENTICATE
  void request_authenticate() noexcept {
    CThostFtdcReqAuthenticateField request{};
    copy_text(request.BrokerID, config_.broker_id);
    copy_text(request.UserID, config_.user_id);
    copy_text(request.AppID, config_.app_id);
    copy_text(request.AuthCode, config_.auth_code);
    spdlog::info("ctp_authentication_attempt user_id={}", config_.user_id);
    if (api_->ReqAuthenticate(&request, next_request_id()) != 0) {
      set_state(State::error);
      spdlog::error("ctp_authentication_request_rejected");
    }
  }
#endif

  void subscribe() noexcept {
    active_.clear();
    std::vector<char*> instruments;
    instruments.reserve(desired_.size());
    for (auto& value : desired_) instruments.push_back(value.data());
    spdlog::info("ctp_subscription_attempt count={}", instruments.size());
    if (api_->SubscribeMarketData(instruments.data(), static_cast<int>(instruments.size())) != 0) {
      subscription_failures_.fetch_add(instruments.size());
      set_state(State::error);
      spdlog::error("ctp_subscription_request_rejected count={}", instruments.size());
    }
  }

  void apply(Action action) noexcept {
    if (action == Action::login) request_login();
    else if (action == Action::subscribe) subscribe();
#ifdef MD_CTP_HAS_AUTHENTICATE
    else if (action == Action::authenticate) request_authenticate();
#endif
  }

  void OnFrontConnected() override {
    connected_.store(true);
    const auto reconnect = ever_connected_.exchange(true);
    if (reconnect) ++reconnects_;
    set_state(State::connected);
    const auto action = machine_.front_connected();
    set_state(machine_.state());
    spdlog::info("ctp_front_connected reconnect={}", reconnect);
    apply(action);
  }

  void OnFrontDisconnected(int reason) override {
    connected_.store(false);logged_in_.store(false);ready_.store(false);active_.clear();++disconnects_;
    machine_.front_disconnected();set_state(machine_.state());
    spdlog::warn("ctp_front_disconnected reason={}", reason);
  }

#ifdef MD_CTP_HAS_AUTHENTICATE
  void OnRspAuthenticate(CThostFtdcRspAuthenticateField*, CThostFtdcRspInfoField* info, int, bool) override {
    const auto ok = response_ok(info);if(ok)set_state(State::authenticated);const auto action = machine_.authentication_result(ok);set_state(machine_.state());
    if (ok) spdlog::info("ctp_authentication_succeeded"); else spdlog::error("ctp_authentication_failed error_id={}", info == nullptr ? 0 : info->ErrorID);
    apply(action);
  }
#endif

  void OnRspUserLogin(CThostFtdcRspUserLoginField* response, CThostFtdcRspInfoField* info, int, bool) override {
    const auto ok=response_ok(info);if(!ok)++login_failures_;logged_in_.store(ok);if(ok)set_state(State::logged_in);const auto action=machine_.login_result(ok);set_state(machine_.state());
    if(ok)spdlog::info("ctp_login_succeeded trading_day={} login_time={}",response==nullptr?std::string_view{}:field_view(response->TradingDay),response==nullptr?std::string_view{}:field_view(response->LoginTime));else spdlog::error("ctp_login_failed error_id={}",info==nullptr?0:info->ErrorID);
    apply(action);
  }

  void OnRspSubMarketData(CThostFtdcSpecificInstrumentField* instrument, CThostFtdcRspInfoField* info, int, bool last) override {
    const auto ok=response_ok(info);if(ok){++subscription_successes_;if(instrument!=nullptr)active_.insert(std::string{field_view(instrument->InstrumentID)});}else ++subscription_failures_;
    machine_.subscription_result(ok,last);set_state(machine_.state());if(last)ready_.store(machine_.state()==State::ready);
    if(!ok)spdlog::error("ctp_subscription_failed error_id={}",info==nullptr?0:info->ErrorID);
    else spdlog::info("ctp_subscription_succeeded instrument={} last={}",instrument==nullptr?std::string_view{}:field_view(instrument->InstrumentID),last);
    if(ok&&last)spdlog::info("ctp_subscriptions_ready active={} desired={}",active_.size(),desired_.size());
  }

  void OnRspError(CThostFtdcRspInfoField* info, int, bool) override {
    spdlog::error("ctp_response_error error_id={}",info==nullptr?0:info->ErrorID);
  }

  void OnRtnDepthMarketData(CThostFtdcDepthMarketDataField* raw) override {
    const auto recv_time=std::chrono::system_clock::now();const auto recv_ns=std::chrono::duration_cast<std::chrono::nanoseconds>(recv_time.time_since_epoch()).count();++ticks_received_;
    if(raw==nullptr){++invalid_ticks_;return;}
    DepthSnapshot snapshot;snapshot.instrument.assign(field_view(raw->InstrumentID));snapshot.exchange.assign(field_view(raw->ExchangeID));snapshot.trading_day.assign(field_view(raw->TradingDay));snapshot.action_day.assign(field_view(raw->ActionDay));snapshot.update_time.assign(field_view(raw->UpdateTime));snapshot.update_millisec=raw->UpdateMillisec;snapshot.last_price=raw->LastPrice;snapshot.volume=raw->Volume;snapshot.turnover=raw->Turnover;snapshot.open_interest=raw->OpenInterest;snapshot.upper_limit_price=raw->UpperLimitPrice;snapshot.lower_limit_price=raw->LowerLimitPrice;
    const double bid_prices[]{raw->BidPrice1,raw->BidPrice2,raw->BidPrice3,raw->BidPrice4,raw->BidPrice5};const int bid_volumes[]{raw->BidVolume1,raw->BidVolume2,raw->BidVolume3,raw->BidVolume4,raw->BidVolume5};const double ask_prices[]{raw->AskPrice1,raw->AskPrice2,raw->AskPrice3,raw->AskPrice4,raw->AskPrice5};const int ask_volumes[]{raw->AskVolume1,raw->AskVolume2,raw->AskVolume3,raw->AskVolume4,raw->AskVolume5};
    for(std::size_t i=0;i<5;++i){snapshot.bid_price[i]=bid_prices[i];snapshot.bid_volume[i]=bid_volumes[i];snapshot.ask_price[i]=ask_prices[i];snapshot.ask_volume[i]=ask_volumes[i];}
    const auto exchange=exchange_by_instrument_.find(snapshot.instrument.view());
    const auto fallback_exchange=exchange==exchange_by_instrument_.end()?std::string_view{}:std::string_view{exchange->second};
    const auto result=normalize_and_enqueue(snapshot,recv_ns,recv_time,identity_,queue_,fallback_exchange);if(result==IngressResult::invalid){++invalid_ticks_;return;}if(result==IngressResult::queue_full){++ingress_rejected_;return;}last_tick_ns_.store(recv_ns);
  }

  int next_request_id() noexcept { return request_id_.fetch_add(1); }
  Metrics metrics() const noexcept {return{connected_,logged_in_,ready_,ticks_received_,invalid_ticks_,ingress_rejected_,disconnects_,reconnects_,login_failures_,subscription_successes_,subscription_failures_,last_tick_ns_};}

  SpscQueue<MarketTick>& queue_;ProducerIdentity& identity_;Config config_;StateMachine machine_;CThostFtdcMdApi* api_{};std::string front_address_;std::vector<std::string> desired_;std::unordered_map<std::string,std::string,TransparentStringHash,std::equal_to<>> exchange_by_instrument_;std::unordered_set<std::string> desired_set_,active_;std::atomic<State> state_{State::disconnected};std::atomic<int> request_id_{1};std::atomic<bool> connected_{false},logged_in_{false},ready_{false},ever_connected_{false};std::atomic<std::uint64_t> ticks_received_{0},invalid_ticks_{0},ingress_rejected_{0},disconnects_{0},reconnects_{0},login_failures_{0},subscription_successes_{0},subscription_failures_{0};std::atomic<std::int64_t> last_tick_ns_{0};
};

MarketDataAdapter::MarketDataAdapter(SpscQueue<MarketTick>& queue,ProducerIdentity& identity,Config config):impl_(std::make_unique<Impl>(queue,identity,std::move(config))){}
MarketDataAdapter::~MarketDataAdapter()=default;
void MarketDataAdapter::start(){impl_->start();}
void MarketDataAdapter::stop(){impl_->stop();}
State MarketDataAdapter::state()const noexcept{return impl_->state_.load();}
Metrics MarketDataAdapter::metrics()const noexcept{return impl_->metrics();}
}
