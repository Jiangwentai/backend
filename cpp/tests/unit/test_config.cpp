#include "market_data/config.hpp"
#include <gtest/gtest.h>
TEST(Config, QwpConnectionEnablesDiskStoreForward){market_data::AppConfig c;c.qdb_sf_dir="/tmp/market-data-test-spool";auto s=c.qdb_connection_string("abc");EXPECT_NE(s.find("ws::"),std::string::npos);EXPECT_NE(s.find("sf_dir=/tmp/market-data-test-spool"),std::string::npos);EXPECT_NE(s.find("sender_id=abc"),std::string::npos);}
TEST(Config, StoreForwardRecoversOrphanSlots){market_data::AppConfig c;auto s=c.qdb_connection_string("abc");EXPECT_NE(s.find("drain_orphans=on"),std::string::npos);EXPECT_NE(s.find("sf_durability=periodic"),std::string::npos);}
TEST(Config, MultipleProvidersCanBeEnabledWithoutChangingLegacySource){market_data::AppConfig c;c.providers_explicit=true;c.synthetic_enabled=true;c.ctp_enabled=true;c.ctp_front_address="tcp://front";c.ctp_broker_id="broker";c.ctp_user_id="user";c.ctp_password="password";c.ctp_subscriptions={"SHFE.zn2610"};EXPECT_NO_THROW(c.validate());}
