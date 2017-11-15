/**
 * This test file test the Distributed Reactors API on ONLY one process (no real
 * networking).
 * In other words, we send a message from one process to itself.
 */

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "communication/reactor/common_messages.hpp"
#include "communication/reactor/reactor_distributed.hpp"
#include "gtest/gtest.h"

using namespace communication::reactor;
using namespace std::literals::chrono_literals;

struct MessageInt : public Message {
  MessageInt() {}  // cereal needs this
  MessageInt(int x) : x(x) {}
  int x;

  template <class Archive>
  void serialize(Archive &ar) {
    ar(cereal::virtual_base_class<Message>(this), x);
  }
};
CEREAL_REGISTER_TYPE(MessageInt);

struct RequestMessage : public ReturnAddressMessage {
  RequestMessage() {}
  RequestMessage(std::string reactor, std::string channel, int x)
      : ReturnAddressMessage(reactor, channel), x(x){};

  template <class Archive>
  void serialize(Archive &ar) {
    ar(cereal::virtual_base_class<ReturnAddressMessage>(this), x);
  }

  friend class cereal::access;
  int x;
};
CEREAL_REGISTER_TYPE(RequestMessage);

/**
  * Test do the services start up without crashes.
  */
TEST(SimpleTests, StartAndStopServices) {
  DistributedSystem system;
  // do nothing
  std::this_thread::sleep_for(500ms);
  system.StopServices();
}

/**
  * Test simple message reception.
  *
  * Data flow:
  * (1) Send an empty message from Master to Worker/main
  */
TEST(SimpleTests, SendEmptyMessage) {
  DistributedSystem system;

  auto master = system.Spawn("master", [](Reactor &r) {
    std::this_thread::sleep_for(100ms);
    auto writer = r.system_.FindChannel("127.0.0.1", 10000, "worker", "main");
    writer->Send<Message>();
    r.CloseChannel("main");
  });

  auto worker = system.Spawn("worker", [](Reactor &r) {
    r.main_.first->OnEventOnce().ChainOnce<Message>(
        [&](const Message &, const Subscription &subscription) {
          // if this message isn't delivered, the main channel will never be
          // closed and we infinite loop
          subscription.CloseChannel();  // close "main"
        });
  });

  std::this_thread::sleep_for(400ms);
  system.StopServices();
}

/**
  * Test ReturnAddressMsg functionality.
  *
  * Data flow:
  * (1) Send an empty message from Master to Worker/main
  * (2) Send an empty message from Worker to Master/main
  */
TEST(SimpleTests, SendReturnAddressMessage) {
  DistributedSystem system;

  auto master = system.Spawn("master", [](Reactor &r) {
    std::this_thread::sleep_for(100ms);
    auto writer = r.system_.FindChannel("127.0.0.1", 10000, "worker", "main");
    writer->Send<ReturnAddressMessage>(r.name(), "main");
    r.main_.first->OnEvent<MessageInt>(
        [&](const MessageInt &message, const Subscription &) {
          EXPECT_EQ(message.x, 5);
          r.CloseChannel("main");
        });
  });
  auto worker = system.Spawn("worker", [](Reactor &r) {
    r.main_.first->OnEvent<ReturnAddressMessage>(
        [&](const ReturnAddressMessage &message, const Subscription &) {
          message.FindChannel(r.system_)->Send<MessageInt>(5);
          r.CloseChannel("main");
        });
  });

  std::this_thread::sleep_for(400ms);
  system.StopServices();
}

/**
  * Test serializability of a complex message over the network layer.
  *
  * Data flow:
  * (1) Send ("hi", 123) from Master to Worker/main
  * (2) Send ("hi back", 779) from Worker to Master/main
  */
TEST(SimpleTests, SendSerializableMessage) {
  DistributedSystem system;

  auto master = system.Spawn("master", [](Reactor &r) {
    std::this_thread::sleep_for(100ms);
    auto writer = r.system_.FindChannel("127.0.0.1", 10000, "worker", "main");
    writer->Send<RequestMessage>(r.name(), "main", 123);
    r.main_.first->OnEvent<MessageInt>(
        [&](const MessageInt &message, const Subscription &) {
          ASSERT_EQ(message.x, 779);
          r.CloseChannel("main");
        });
  });

  auto worker = system.Spawn("worker", [](Reactor &r) {
    r.main_.first->OnEvent<RequestMessage>(
        [&](const RequestMessage &message, const Subscription &) {
          ASSERT_EQ(message.x, 123);
          message.FindChannel(r.system_)->Send<MessageInt>(779);
          r.CloseChannel("main");
        });
  });

  std::this_thread::sleep_for(400ms);
  system.StopServices();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
