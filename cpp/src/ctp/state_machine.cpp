#include "market_data/ctp/state_machine.hpp"
namespace market_data::ctp {
const char* state_name(State s)noexcept{switch(s){case State::disconnected:return"DISCONNECTED";case State::connecting:return"CONNECTING";case State::connected:return"CONNECTED";case State::authenticating:return"AUTHENTICATING";case State::authenticated:return"AUTHENTICATED";case State::logging_in:return"LOGGING_IN";case State::logged_in:return"LOGGED_IN";case State::subscribing:return"SUBSCRIBING";case State::ready:return"READY";case State::reconnecting:return"RECONNECTING";case State::error:return"ERROR";}return"ERROR";}
Action StateMachine::start()noexcept{reconnecting_=false;subscription_failed_=false;state_=State::connecting;return Action::none;}
Action StateMachine::front_connected()noexcept{reconnecting_=ever_connected_;ever_connected_=true;subscription_failed_=false;state_=State::connected;if(authentication_required_){state_=State::authenticating;return Action::authenticate;}state_=State::logging_in;return Action::login;}
Action StateMachine::authentication_result(bool ok)noexcept{if(state_!=State::authenticating||!ok){state_=State::error;return Action::none;}state_=State::authenticated;state_=State::logging_in;return Action::login;}
Action StateMachine::login_result(bool ok)noexcept{if(state_!=State::logging_in||!ok){state_=State::error;return Action::none;}state_=State::logged_in;state_=State::subscribing;return Action::subscribe;}
Action StateMachine::subscription_result(bool ok,bool last)noexcept{if(state_!=State::subscribing)return Action::none;if(!ok)subscription_failed_=true;if(last)state_=subscription_failed_?State::error:State::ready;return Action::none;}
void StateMachine::front_disconnected()noexcept{subscription_failed_=false;state_=ever_connected_?State::reconnecting:State::disconnected;}
}
