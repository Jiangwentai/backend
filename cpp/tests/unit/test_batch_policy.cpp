#include "market_data/questdb_writer.hpp"
#include <gtest/gtest.h>
using namespace std::chrono_literals;
TEST(BatchPolicy, FlushesByRowsOrLatency){market_data::BatchPolicy p(500,20ms);EXPECT_FALSE(p.should_flush(499,19ms));EXPECT_TRUE(p.should_flush(500,1ms));EXPECT_TRUE(p.should_flush(1,20ms));EXPECT_FALSE(p.should_flush(0,30ms));}

