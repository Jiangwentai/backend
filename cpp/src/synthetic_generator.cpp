#include "market_data/synthetic_generator.hpp"
#include <chrono>
#include <thread>
#include <utility>
namespace market_data {
SyntheticGenerator::SyntheticGenerator(SpscQueue<MarketTick>&q,ProducerIdentity&id,std::uint32_t r,std::vector<std::string>s):q_(q),id_(id),rate_(r),symbols_(std::move(s)){} SyntheticGenerator::~SyntheticGenerator(){request_stop();join();}void SyntheticGenerator::start(){thread_=std::thread(&SyntheticGenerator::run,this);}void SyntheticGenerator::request_stop()noexcept{stop_=true;}void SyntheticGenerator::join(){if(thread_.joinable())thread_.join();}
void SyntheticGenerator::run(){using namespace std::chrono;auto next=steady_clock::now();std::size_t i=0;while(!stop_){MarketTick t;t.recv_ts_ns=duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();t.event_ts_us=t.recv_ts_ns/1000;t.producer_id=id_.id();t.seq=id_.next_seq();auto s=std::string_view(symbols_[i++%symbols_.size()]);auto dot=s.find('.');t.exchange.assign(s.substr(0,dot));t.instrument.assign(dot==s.npos?s:s.substr(dot+1));t.trading_day.assign("20260904");t.action_day.assign("20260904");t.last_price=1000.0+static_cast<double>(t.seq%100);t.volume=static_cast<std::int64_t>(t.seq);t.turnover=t.last_price*static_cast<double>(t.volume);t.open_interest=10000;for(std::size_t l=0;l<5;++l){t.bid_price[l]=t.last_price-0.2-static_cast<double>(l);t.ask_price[l]=t.last_price+0.2+static_cast<double>(l);t.bid_volume[l]=10;t.ask_volume[l]=10;}while(!q_.try_push(t)&&!stop_)std::this_thread::yield();++generated_;next+=nanoseconds{1'000'000'000/rate_};std::this_thread::sleep_until(next);}}
}
