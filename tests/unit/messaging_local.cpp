#include <chrono>
#include <iostream>
#include <thread>

#include "communication/messaging/local.hpp"
#include "gtest/gtest.h"

using namespace std::literals::chrono_literals;
using namespace communication::messaging;

struct MessageInt : public Message {
  MessageInt(int xx) : x(xx) {}
  int x;
};

#define GET_X(p) dynamic_cast<MessageInt *>((p).get())->x

TEST(LocalMessaging, Pop) {
  LocalSystem system;
  auto stream = system.Open("main");
  LocalWriter writer(system, "main");

  EXPECT_EQ(stream->Poll(), nullptr);
  writer.Send<MessageInt>(10);
  EXPECT_EQ(GET_X(stream->Poll()), 10);
}

TEST(LocalMessaging, Await) {
  LocalSystem system;
  auto stream = system.Open("main");
  LocalWriter writer(system, "main");

  std::thread t([&] {
    std::this_thread::sleep_for(100ms);
    stream->Shutdown();
    std::this_thread::sleep_for(100ms);
    writer.Send<MessageInt>(20);
  });

  EXPECT_EQ(stream->Poll(), nullptr);
  EXPECT_EQ(stream->Await(), nullptr);
  t.join();
}

TEST(LocalMessaging, AwaitTimeout) {
  LocalSystem system;
  auto stream = system.Open("main");

  EXPECT_EQ(stream->Poll(), nullptr);
  EXPECT_EQ(stream->Await(100ms), nullptr);
}

TEST(LocalMessaging, RecreateChannelAfterClosing) {
  LocalSystem system;
  auto stream = system.Open("main");
  LocalWriter writer(system, "main");

  writer.Send<MessageInt>(10);
  EXPECT_EQ(GET_X(stream->Poll()), 10);

  stream = nullptr;
  writer.Send<MessageInt>(20);
  stream = system.Open("main");
  EXPECT_EQ(stream->Poll(), nullptr);
  writer.Send<MessageInt>(30);
  EXPECT_EQ(GET_X(stream->Poll()), 30);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
