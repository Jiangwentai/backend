#include "market_data/market_tick.hpp"
#include "market_data/producer_identity.hpp"
#include <gtest/gtest.h>
#include <chrono>
#include <cfloat>
using namespace std::chrono;
TEST(MarketTick, InvalidPricesBecomeNan){EXPECT_FALSE(market_data::is_valid_price(DBL_MAX));EXPECT_TRUE(std::isnan(market_data::normalize_price(DBL_MAX)));EXPECT_TRUE(market_data::is_valid_price(123.5));}
TEST(MarketTick, ExplicitActionDayConvertsChinaTimeToUtc){auto got=market_data::normalize_ctp_event_ts_us("20260904","09:30:00",123,system_clock::time_point{});auto expected=sys_days{year{2026}/9/4}+hours{1}+minutes{30}+milliseconds{123};EXPECT_EQ(got,duration_cast<microseconds>(expected.time_since_epoch()).count());}
TEST(MarketTick, EmptyActionDayResolvesPreviousDayAtMidnight){auto recv_local=sys_days{year{2026}/9/4}+minutes{1};auto recv_utc=recv_local-hours{8};auto got=market_data::normalize_ctp_event_ts_us("","23:59:59",900,recv_utc);auto expected=sys_days{year{2026}/9/3}+hours{15}+minutes{59}+seconds{59}+milliseconds{900};EXPECT_EQ(got,duration_cast<microseconds>(expected.time_since_epoch()).count());}
TEST(MarketTick, EmptyActionDayResolvesNextDayBeforeMidnight){auto recv_local=sys_days{year{2026}/9/3}+hours{23}+minutes{59};auto got=market_data::normalize_ctp_event_ts_us("","00:00:01",100,recv_local-hours{8});auto expected=sys_days{year{2026}/9/3}+hours{16}+seconds{1}+milliseconds{100};EXPECT_EQ(got,duration_cast<microseconds>(expected.time_since_epoch()).count());}
TEST(ProducerIdentity, IsUuidAndSequenceIsGlobalMonotonic){market_data::ProducerIdentity id;EXPECT_EQ(id.id().view().size(),36);EXPECT_EQ(id.next_seq(),1);EXPECT_EQ(id.next_seq(),2);}
