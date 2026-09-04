#pragma once
#include "market_data/ctp/types.hpp"
#include <cstddef>
namespace market_data::ctp {
class StateMachine {
 public:
  explicit StateMachine(bool authentication_required):authentication_required_(authentication_required){}
  Action start() noexcept;Action front_connected() noexcept;Action authentication_result(bool success) noexcept;Action login_result(bool success) noexcept;Action subscription_result(bool success,bool last) noexcept;void front_disconnected() noexcept;
  [[nodiscard]]State state()const noexcept{return state_;}[[nodiscard]]bool is_reconnect()const noexcept{return ever_connected_;}
 private:State state_{State::disconnected};bool authentication_required_{},ever_connected_{};
};
}
