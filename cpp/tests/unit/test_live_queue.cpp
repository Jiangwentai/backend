#include "market_data/live_queue.hpp"
#include <gtest/gtest.h>
TEST(LiveQueue, KeepsFreshestAndCountsDrop){market_data::LiveQueue q(2);market_data::MarketTick t;t.seq=1;q.try_push_latest(t);t.seq=2;q.try_push_latest(t);t.seq=3;q.try_push_latest(t);market_data::MarketTick out;ASSERT_TRUE(q.try_pop(out));EXPECT_EQ(out.seq,2);EXPECT_EQ(q.metrics().dropped_total,1);}
