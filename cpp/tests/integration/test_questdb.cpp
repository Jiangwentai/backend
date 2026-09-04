#include "market_data/questdb_writer.hpp"
#include <questdb/egress/qwp_reader.hpp>
#include <questdb/ingress/qwp_sender.hpp>
#include <gtest/gtest.h>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <string>
namespace {
using namespace std::chrono_literals;
std::string conf(){const char* c=std::getenv("QDB_TEST_CONF");return c?c:"";}
void execute(std::string_view c,std::string_view sql){questdb::pool p{c};auto r=p.borrow_reader();auto cursor=r.execute(sql);while(cursor.next_batch()){} }
std::int64_t scalar(std::string_view c,std::string_view sql){questdb::pool p{c};auto r=p.borrow_reader();auto cursor=r.execute(sql);while(auto batch=cursor.next_batch()){if(batch->row_count())if(auto v=batch->column(0).get<std::int64_t>(0))return *v;}return -1;}
market_data::MarketTick tick(std::uint64_t seq){market_data::MarketTick t;t.event_ts_us=1788485400123000;t.recv_ts_ns=1788485400123456789;t.producer_id.assign("11111111-2222-4333-8444-555555555555");t.seq=seq;t.exchange.assign("SHFE");t.instrument.assign("zn2610");t.trading_day.assign("20260904");t.action_day.assign("20260904");t.last_price=25000.5;t.volume=100;t.turnover=2500050;t.open_interest=9000;for(std::size_t i=0;i<5;++i){t.bid_price[i]=25000-i;t.ask_price[i]=25001+i;t.bid_volume[i]=10;t.ask_volume[i]=11;}return t;}
}
TEST(QuestDbIntegration,QwpReplayDedupAndFeedDuplicate){
 auto c=conf();if(c.empty())GTEST_SKIP()<<"set QDB_TEST_CONF to run integration tests";
 execute(c,"TRUNCATE TABLE ctp_market_data");market_data::SpscQueue<market_data::MarketTick>q(16);ASSERT_TRUE(q.try_push(tick(100)));ASSERT_TRUE(q.try_push(tick(100)));ASSERT_TRUE(q.try_push(tick(101)));
 market_data::QuestDbWriter w(q,c,2,5ms,30s);w.start();w.request_stop();w.join();EXPECT_TRUE(w.metrics().healthy);
 for(int i=0;i<100&&scalar(c,"SELECT count() FROM ctp_market_data")!=2;++i)std::this_thread::sleep_for(50ms);
 EXPECT_EQ(scalar(c,"SELECT count() FROM ctp_market_data"),2);EXPECT_EQ(scalar(c,"SELECT count() FROM ctp_market_data WHERE seq=100"),1);EXPECT_EQ(scalar(c,"SELECT count() FROM ctp_market_data WHERE seq IN (100,101)"),2);
}

