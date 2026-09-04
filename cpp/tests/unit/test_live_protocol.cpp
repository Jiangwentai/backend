#include "market_data/live_protocol.hpp"
#include <gtest/gtest.h>
TEST(LiveProtocol, TopicAndRoundTrip){market_data::MarketTick t;t.exchange.assign("SHFE");t.instrument.assign("zn2610");t.producer_id.assign("p");t.seq=9;t.event_ts_us=10;t.recv_ts_ns=11;auto b=market_data::encode_live_tick(t);auto d=market_data::decode_live_tick(b.data(),b.size());EXPECT_EQ(market_data::live_topic(t),"SHFE.zn2610");EXPECT_EQ(d.seq,9);EXPECT_EQ(d.recv_ts_ns,11);EXPECT_EQ(d.instrument.view(),"zn2610");}
