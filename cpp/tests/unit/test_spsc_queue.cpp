#include "market_data/spsc_queue.hpp"
#include <gtest/gtest.h>
TEST(SpscQueue, NeverOverwritesUnread){market_data::SpscQueue<int>q(2);EXPECT_TRUE(q.try_push(1));EXPECT_TRUE(q.try_push(2));EXPECT_FALSE(q.try_push(3));int x;EXPECT_TRUE(q.try_pop(x));EXPECT_EQ(x,1);EXPECT_TRUE(q.try_pop(x));EXPECT_EQ(x,2);auto m=q.metrics();EXPECT_EQ(m.push_failed_total,1);EXPECT_EQ(m.high_water_mark,2);}

