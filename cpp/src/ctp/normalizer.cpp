#include "market_data/ctp/normalizer.hpp"
namespace market_data::ctp {
std::optional<MarketTick> normalize(const DepthSnapshot&s,std::int64_t recv_ns,std::chrono::system_clock::time_point recv,const ProducerId&id,std::uint64_t seq) noexcept{
 try{if(s.instrument.view().empty()||s.exchange.view().empty()||s.trading_day.view().empty())return std::nullopt;MarketTick t;t.recv_ts_ns=recv_ns;t.event_ts_us=normalize_ctp_event_ts_us(s.action_day.view(),s.update_time.view(),s.update_millisec,recv);t.producer_id=id;t.seq=seq;t.instrument=s.instrument;t.exchange=s.exchange;t.trading_day=s.trading_day;t.action_day=s.action_day;t.last_price=normalize_price(s.last_price);t.volume=s.volume;t.turnover=s.turnover;t.open_interest=s.open_interest;t.upper_limit_price=normalize_price(s.upper_limit_price);t.lower_limit_price=normalize_price(s.lower_limit_price);for(std::size_t i=0;i<5;++i){t.bid_price[i]=normalize_price(s.bid_price[i]);t.ask_price[i]=normalize_price(s.ask_price[i]);t.bid_volume[i]=s.bid_volume[i];t.ask_volume[i]=s.ask_volume[i];}return t;}catch(...){return std::nullopt;}
}
}
