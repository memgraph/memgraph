#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "communication/messaging/distributed.hpp"
#include "gtest/gtest.h"

using namespace communication::messaging;
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

#define GET_X(p) dynamic_cast<MessageInt *>((p).get())->x

/**
  * Test do the services start up without crashes.
  */
TEST(SimpleTests, StartAndShutdown) {
  System system("127.0.0.1", 10000);
  // do nothing
  std::this_thread::sleep_for(500ms);
  system.Shutdown();
}

TEST(Messaging, Pop) {
  System master_system("127.0.0.1", 10000);
  System slave_system("127.0.0.1", 10001);
  auto stream = master_system.Open("main");
  Writer writer(slave_system, "127.0.0.1", 10000, "main");
  std::this_thread::sleep_for(100ms);

  EXPECT_EQ(stream->Poll(), nullptr);
  writer.Send<MessageInt>(10);
  EXPECT_EQ(GET_X(stream->Await()), 10);
  master_system.Shutdown();
  slave_system.Shutdown();
}

TEST(Messaging, Await) {
  System master_system("127.0.0.1", 10000);
  System slave_system("127.0.0.1", 10001);
  auto stream = master_system.Open("main");
  Writer writer(slave_system, "127.0.0.1", 10000, "main");
  std::this_thread::sleep_for(100ms);

  std::thread t([&] {
    std::this_thread::sleep_for(100ms);
    stream->Shutdown();
    std::this_thread::sleep_for(100ms);
    writer.Send<MessageInt>(20);
  });

  EXPECT_EQ(stream->Poll(), nullptr);
  EXPECT_EQ(stream->Await(), nullptr);
  t.join();
  master_system.Shutdown();
  slave_system.Shutdown();
}

TEST(Messaging, RecreateChannelAfterClosing) {
  System master_system("127.0.0.1", 10000);
  System slave_system("127.0.0.1", 10001);
  auto stream = master_system.Open("main");
  Writer writer(slave_system, "127.0.0.1", 10000, "main");
  std::this_thread::sleep_for(100ms);

  writer.Send<MessageInt>(10);
  EXPECT_EQ(GET_X(stream->Await()), 10);

  stream = nullptr;
  writer.Send<MessageInt>(20);
  std::this_thread::sleep_for(100ms);

  stream = master_system.Open("main");
  std::this_thread::sleep_for(100ms);
  EXPECT_EQ(stream->Poll(), nullptr);
  writer.Send<MessageInt>(30);
  EXPECT_EQ(GET_X(stream->Await()), 30);

  master_system.Shutdown();
  slave_system.Shutdown();
}
