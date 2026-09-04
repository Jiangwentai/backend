#include "market_data/questdb_writer.hpp"
#include <questdb/ingress/qwp_sender.hpp>
#include <questdb/ingress/line_sender.hpp>
#include <spdlog/spdlog.h>
#include <cmath>
#include <thread>
namespace market_data {
namespace q=questdb::ingress;
namespace {
q::utf8_view uv(std::string_view s){return q::utf8_view{s.data(),s.size()};}
void add_price(q::line_sender_buffer&b,q::column_name_view n,double v){b.column(n,is_valid_price(v)?v:std::numeric_limits<double>::quiet_NaN());}
void append(q::line_sender_buffer&b,const MarketTick&t){
 using namespace q::literals;
 b.table("ctp_market_data"_tn)
  .symbol("provider"_cn,uv(to_string(t.provider)))
  .symbol("event_type"_cn,uv(to_string(t.event_type)))
  .symbol("instrument_id"_cn,uv(t.instrument_id.view()))
  .symbol("quality"_cn,uv(to_string(t.quality)))
  .symbol("producer_id"_cn,uv(t.producer_id.view()))
  .symbol("exchange"_cn,uv(t.exchange.view()))
  .symbol("instrument"_cn,uv(t.instrument.view()))
  .symbol("trading_day"_cn,uv(t.trading_day.view()))
  .symbol("action_day"_cn,uv(t.action_day.view()))
  .column("seq"_cn,static_cast<std::int64_t>(t.seq))
  .column("recv_ts"_cn,q::timestamp_nanos{t.recv_ts_ns});
 add_price(b,"last_price"_cn,t.last_price);b.column("volume"_cn,t.volume).column("turnover"_cn,t.turnover).column("open_interest"_cn,t.open_interest);add_price(b,"upper_limit_price"_cn,t.upper_limit_price);add_price(b,"lower_limit_price"_cn,t.lower_limit_price);
 for(std::size_t i=0;i<5;++i){static constexpr const char* bp[]={"bid_price1","bid_price2","bid_price3","bid_price4","bid_price5"};static constexpr const char* bv[]={"bid_volume1","bid_volume2","bid_volume3","bid_volume4","bid_volume5"};static constexpr const char* ap[]={"ask_price1","ask_price2","ask_price3","ask_price4","ask_price5"};static constexpr const char* av[]={"ask_volume1","ask_volume2","ask_volume3","ask_volume4","ask_volume5"};add_price(b,q::column_name_view{bp[i]},t.bid_price[i]);b.column_i32(q::column_name_view{bv[i]},t.bid_volume[i]);add_price(b,q::column_name_view{ap[i]},t.ask_price[i]);b.column_i32(q::column_name_view{av[i]},t.ask_volume[i]);}
 b.at(q::timestamp_micros{t.event_ts_us});
}
}
QuestDbWriter::QuestDbWriter(SpscQueue<MarketTick>&q,std::string c,std::size_t rows,std::chrono::milliseconds lat,std::chrono::milliseconds ack):queue_(q),connection_(std::move(c)),policy_(rows,lat),ack_timeout_(ack){}QuestDbWriter::~QuestDbWriter(){request_stop();join();}void QuestDbWriter::start(){thread_=std::thread(&QuestDbWriter::run,this);}void QuestDbWriter::request_stop()noexcept{stop_=true;}void QuestDbWriter::join(){if(thread_.joinable())thread_.join();}WriterMetrics QuestDbWriter::metrics()const noexcept{return{accepted_,acked_,failures_,flush_us_,healthy_};}
void QuestDbWriter::run(){
 try{questdb::pool pool{connection_};auto sender=pool.borrow_sender();auto buffer=sender.new_buffer();std::size_t rows=0;auto since=std::chrono::steady_clock::now();MarketTick t;
  auto flush=[&]{if(!rows)return;auto begin=std::chrono::steady_clock::now();sender.flush(buffer);accepted_+=rows;rows=0;since=std::chrono::steady_clock::now();flush_us_=std::chrono::duration_cast<std::chrono::microseconds>(since-begin).count();};
  while(!stop_.load()||!queue_.empty()){if(queue_.try_pop(t)){append(buffer,t);++rows;}else std::this_thread::sleep_for(std::chrono::microseconds(50));if(policy_.should_flush(rows,std::chrono::steady_clock::now()-since))flush();}
  flush();sender.wait(q::qwpws_ack_level::ok,ack_timeout_);++acked_;spdlog::info("questdb_shutdown_ack accepted_rows={}",accepted_.load());
 }catch(const std::exception&e){healthy_=false;++failures_;spdlog::critical("questdb_writer_failure error={}",e.what());}
}
}
