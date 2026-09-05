#include "market_data/config.hpp"
#include <gtest/gtest.h>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
TEST(Config, QwpConnectionEnablesDiskStoreForward){market_data::AppConfig c;c.qdb_sf_dir="/tmp/market-data-test-spool";auto s=c.qdb_connection_string("abc");EXPECT_NE(s.find("ws::"),std::string::npos);EXPECT_NE(s.find("sf_dir=/tmp/market-data-test-spool"),std::string::npos);EXPECT_NE(s.find("sender_id=abc"),std::string::npos);}
TEST(Config, StoreForwardRecoversOrphanSlots){market_data::AppConfig c;auto s=c.qdb_connection_string("abc");EXPECT_NE(s.find("drain_orphans=on"),std::string::npos);EXPECT_NE(s.find("sf_durability=periodic"),std::string::npos);}
TEST(Config, MultipleProvidersCanBeEnabledWithoutChangingLegacySource){market_data::AppConfig c;c.providers_explicit=true;c.synthetic_enabled=true;c.ctp_enabled=true;c.ctp_front_address="tcp://front";c.ctp_broker_id="broker";c.ctp_user_id="user";c.ctp_password="password";c.ctp_subscriptions={"SHFE.zn2610"};EXPECT_NO_THROW(c.validate());}

TEST(Config, LiveHwmMustBeBounded) {
  market_data::AppConfig config;
  EXPECT_EQ(config.zmq_sndhwm, 1000);
  config.zmq_sndhwm = 0;
  EXPECT_THROW(config.validate(), std::invalid_argument);
  config.zmq_sndhwm = -1;
  EXPECT_THROW(config.validate(), std::invalid_argument);
  config.zmq_sndhwm = 4096;
  EXPECT_NO_THROW(config.validate());
}

TEST(Config, LiveHwmYamlAndStrictEnvironmentOverride) {
  struct Fixture {
    std::optional<std::string> old_env;
    std::filesystem::path root;
    Fixture() {
      if (const auto* value = std::getenv("ZMQ_SNDHWM")) old_env = value;
      char pattern[] = "/tmp/market-data-hwm-XXXXXX";
      const auto* directory = mkdtemp(pattern);
      if (!directory) throw std::runtime_error("cannot create test directory");
      root = directory;
      unsetenv("ZMQ_SNDHWM");
      std::ofstream config(root / "app.yaml");
      config << "questdb:\n  sf_dir: " << (root / "spool").string()
             << "\nlive:\n  sndhwm: 17\n";
    }
    ~Fixture() {
      if (old_env) setenv("ZMQ_SNDHWM", old_env->c_str(), 1);
      else unsetenv("ZMQ_SNDHWM");
      std::filesystem::remove_all(root);
    }
  } fixture;
  const auto path = (fixture.root / "app.yaml").string();
  EXPECT_EQ(market_data::load_config(path).zmq_sndhwm, 17);
  setenv("ZMQ_SNDHWM", "4096", 1);
  EXPECT_EQ(market_data::load_config(path).zmq_sndhwm, 4096);
  for (const auto* value : {"0", "-1", "2147483648", "4294967297", "17junk", "1.5", ""}) {
    setenv("ZMQ_SNDHWM", value, 1);
    EXPECT_THROW(market_data::load_config(path), std::invalid_argument) << value;
  }
}
