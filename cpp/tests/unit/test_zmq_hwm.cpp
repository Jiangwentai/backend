#include "market_data/zmq_publisher.hpp"
#include <gtest/gtest.h>
#include <zmq.hpp>
#include <zmq_addon.hpp>
#include <array>
#include <chrono>
#include <string>
#include <vector>

TEST(ZmqHwm, PublisherRejectsUnboundedOrNegativeBuffers) {
  market_data::LiveQueue queue(8);
  EXPECT_THROW((market_data::ZmqPublisher{queue, "inproc://unused", 0}), std::invalid_argument);
  EXPECT_THROW((market_data::ZmqPublisher{queue, "inproc://unused", -1}), std::invalid_argument);
}

TEST(ZmqHwm, SlowSubscriberDropsWholeMessagesDespiteSuccessfulSendsAndRecovers) {
  // Inproc avoids TCP/kernel buffering and makes saturation deterministic.
  // Both sockets share a thread here; this tests pinned libzmq PUB semantics.
  zmq::context_t context(1);
  zmq::socket_t pub(context, zmq::socket_type::pub);
  zmq::socket_t sub(context, zmq::socket_type::sub);
  pub.set(zmq::sockopt::linger, 0);
  sub.set(zmq::sockopt::linger, 0);
  pub.set(zmq::sockopt::sndhwm, 2);
  sub.set(zmq::sockopt::rcvhwm, 2);
  sub.set(zmq::sockopt::subscribe, "topic");
  sub.set(zmq::sockopt::rcvtimeo, 1000);
  pub.bind("inproc://hwm-test");
  sub.connect("inproc://hwm-test");
  auto send = [&](const std::string& body) {
    const std::array<zmq::const_buffer, 2> frames{
        zmq::buffer("topic", 5), zmq::buffer(body)};
    const auto result = zmq::send_multipart(pub, frames, zmq::send_flags::dontwait);
    EXPECT_TRUE(result.has_value());
    if (result) EXPECT_EQ(*result, 2u);
  };
  // Synchronize the subscription without assuming a TCP slow-joiner delay.
  send("ready");
  std::vector<zmq::message_t> ready;
  ASSERT_TRUE(zmq::recv_multipart(sub, std::back_inserter(ready)).has_value());
  ASSERT_EQ(ready.size(), 2u);
  ASSERT_EQ(ready[1].to_string(), "ready");
  for (int i = 0; i < 100; ++i) send(std::to_string(i));
  int received = 0;
  while (true) {
    std::vector<zmq::message_t> frames;
    if (!zmq::recv_multipart(sub, std::back_inserter(frames), zmq::recv_flags::dontwait)) break;
    ASSERT_EQ(frames.size(), 2u);
    EXPECT_EQ(frames[0].to_string(), "topic");
    EXPECT_EQ(frames[1].to_string(), std::to_string(received++));
  }
  EXPECT_GT(received, 0);
  EXPECT_LT(received, 100);
  // Pipe reactivation is asynchronous; a just-drained pipe need not accept
  // the very next send. Keep publishing new observations until it reactivates.
  sub.set(zmq::sockopt::rcvtimeo, 10);
  std::vector<zmq::message_t> fresh;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
  do {
    send("fresh");
    if (zmq::recv_multipart(sub, std::back_inserter(fresh))) break;
  } while (std::chrono::steady_clock::now() < deadline);
  ASSERT_EQ(fresh.size(), 2u);
  EXPECT_EQ(fresh[1].to_string(), "fresh");
}
