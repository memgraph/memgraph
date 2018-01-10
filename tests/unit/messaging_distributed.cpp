#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/archive/text_iarchive.hpp"
#include "boost/archive/text_oarchive.hpp"
#include "boost/serialization/access.hpp"
#include "boost/serialization/base_object.hpp"
#include "boost/serialization/export.hpp"
#include "gtest/gtest.h"

#include "communication/messaging/distributed.hpp"

using namespace communication::messaging;
using namespace std::literals::chrono_literals;

struct MessageInt : public Message {
  MessageInt(int x) : x(x) {}
  int x;

 private:
  friend class boost::serialization::access;
  MessageInt() {}  // Needed for serialization

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<Message>(*this);
    ar &x;
  }
};
BOOST_CLASS_EXPORT(MessageInt);

#define GET_X(p) dynamic_cast<MessageInt *>((p).get())->x

/**
 * Test do the services start up without crashes.
 */
TEST(SimpleTests, StartAndShutdown) {
  System system("127.0.0.1", 0);
  // do nothing
  std::this_thread::sleep_for(500ms);
}

TEST(Messaging, Pop) {
  System master_system("127.0.0.1", 0);
  System slave_system("127.0.0.1", 0);
  auto stream = master_system.Open("main");
  Writer writer(slave_system, "127.0.0.1", master_system.endpoint().port(),
                "main");
  std::this_thread::sleep_for(100ms);

  EXPECT_EQ(stream->Poll(), nullptr);
  writer.Send<MessageInt>(10);
  EXPECT_EQ(GET_X(stream->Await()), 10);
}

TEST(Messaging, Await) {
  System master_system("127.0.0.1", 0);
  System slave_system("127.0.0.1", 0);
  auto stream = master_system.Open("main");
  Writer writer(slave_system, "127.0.0.1", master_system.endpoint().port(),
                "main");
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
}

TEST(Messaging, RecreateChannelAfterClosing) {
  System master_system("127.0.0.1", 0);
  System slave_system("127.0.0.1", 0);
  auto stream = master_system.Open("main");
  Writer writer(slave_system, "127.0.0.1", master_system.endpoint().port(),
                "main");
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
}
