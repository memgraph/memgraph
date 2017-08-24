/**
 * This test file test the Distributed Reactors API on ONLY one process (no real networking).
 * In other words, we send a message from one process to itself.
 */

#include "gtest/gtest.h"
#include "reactors_distributed.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <future>

/**
  * Test do the services start up without crashes.
  */
TEST(SimpleTests, StartAndStopServices) {
  System &system = System::GetInstance();
  Distributed &distributed = Distributed::GetInstance();
  distributed.StartServices();

  // do nothing
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  system.AwaitShutdown();
  distributed.StopServices();
}

/**
  * Test simple message reception.
  *
  * Data flow:
  * (1) Send an empty message from Master to Worker/main
  */
TEST(SimpleTests, SendEmptyMessage) {
  struct Master : public Reactor {
    Master(std::string name) : Reactor(name) {}

    virtual void Run() {
      Distributed::GetInstance().FindChannel("127.0.0.1", 10000, "worker", "main")
        ->OnEventOnce()
        .ChainOnce<ChannelResolvedMessage>([this](const ChannelResolvedMessage& msg,
                                                  const Subscription& subscription) {
            msg.channelWriter()->Send<Message>();
            subscription.CloseChannel();
          });

      CloseChannel("main");
    }
  };

  struct Worker : public Reactor {
    Worker(std::string name) : Reactor(name) {}

    virtual void Run() {
      main_.first->OnEventOnce()
        .ChainOnce<Message>([this](const Message&, const Subscription& subscription) {
            // if this message isn't delivered, the main channel will never be closed and we infinite loop
            subscription.CloseChannel(); // close "main"
          });
    }
  };

  // emulate flags like it's a multiprocess system, these may be alredy set by default
  FLAGS_address = "127.0.0.1";
  FLAGS_port = 10000;

  System &system = System::GetInstance();
  Distributed &distributed = Distributed::GetInstance();
  distributed.StartServices();

  system.Spawn<Master>("master");
  system.Spawn<Worker>("worker");

  system.AwaitShutdown(); // this must be called before StopServices
  distributed.StopServices();
}

/**
  * Test ReturnAddressMsg functionality.
  *
  * Data flow:
  * (1) Send an empty message from Master to Worker/main
  * (2) Send an empty message from Worker to Master/main
  */
TEST(SimpleTests, SendReturnAddressMessage) {
  struct Master : public Reactor {
    Master(std::string name) : Reactor(name) {}

    virtual void Run() {
      Distributed::GetInstance().FindChannel("127.0.0.1", 10000, "worker", "main")
        ->OnEventOnce()
        .ChainOnce<ChannelResolvedMessage>([this](const ChannelResolvedMessage& msg,
                                                  const Subscription& sub) {
            // send a message that will be returned to "main"
            msg.channelWriter()->Send<ReturnAddressMsg>(this->name(), "main");
            // close this anonymous channel
            sub.CloseChannel();
          });

      main_.first->OnEventOnce()
        .ChainOnce<Message>([this](const Message&, const Subscription& sub) {
            // if this message isn't delivered, the main channel will never be closed and we infinite loop
            // close the "main" channel
            sub.CloseChannel();
          });
    }
  };

  struct Worker : public Reactor {
    Worker(std::string name) : Reactor(name) {}

    virtual void Run() {
      main_.first->OnEventOnce()
        .ChainOnce<ReturnAddressMsg>([this](const ReturnAddressMsg &msg, const Subscription& sub) {
            msg.GetReturnChannelWriter()->Send<Message>();
            sub.CloseChannel(); // close "main"
          });
    }
  };

  // emulate flags like it's a multiprocess system, these may be alredy set by default
  FLAGS_address = "127.0.0.1";
  FLAGS_port = 10000;

  System &system = System::GetInstance();
  Distributed &distributed = Distributed::GetInstance();
  distributed.StartServices();

  system.Spawn<Master>("master");
  system.Spawn<Worker>("worker");

  system.AwaitShutdown(); // this must be called before StopServices
  distributed.StopServices();
}

// Apparently templates cannot be declared inside local classes, figure out how to move it in?
// For that reason I obscured the name.
struct SerializableMessage_TextMessage : public ReturnAddressMsg {
  SerializableMessage_TextMessage(std::string channel, std::string arg_text, int arg_val)
    : ReturnAddressMsg(channel), text(arg_text), val(arg_val) {}
  std::string text;
  int val;

  template<class Archive>
  void serialize(Archive &ar) {
    ar(cereal::virtual_base_class<ReturnAddressMsg>(this), text, val);
  }

 protected:
  friend class cereal::access;
  SerializableMessage_TextMessage() {} // Cereal needs access to a default constructor.
};
CEREAL_REGISTER_TYPE(SerializableMessage_TextMessage);

/**
  * Test serializability of a complex message over the network layer.
  *
  * Data flow:
  * (1) Send ("hi", 123) from Master to Worker/main
  * (2) Send ("hi back", 779) from Worker to Master/main
  */
TEST(SimpleTests, SendSerializableMessage) {
  struct Master : public Reactor {
    Master(std::string name) : Reactor(name) {}

    virtual void Run() {
      Distributed::GetInstance().FindChannel("127.0.0.1", 10000, "worker", "main")
        ->OnEventOnce()
        .ChainOnce<ChannelResolvedMessage>([this](const ChannelResolvedMessage& msg,
                                                  const Subscription& sub) {
            // send a message that will be returned to "main"
            msg.channelWriter()->Send<SerializableMessage_TextMessage>("main", "hi", 123);
            // close this anonymous channel
            sub.CloseChannel();
          });

      main_.first->OnEventOnce()
        .ChainOnce<SerializableMessage_TextMessage>([this](const SerializableMessage_TextMessage& msg, const Subscription& sub) {
            ASSERT_EQ(msg.text, "hi back");
            ASSERT_EQ(msg.val, 779);
            // if this message isn't delivered, the main channel will never be closed and we infinite loop
            // close the "main" channel
            sub.CloseChannel();
          });
    }
  };

  struct Worker : public Reactor {
    Worker(std::string name) : Reactor(name) {}

    virtual void Run() {
      main_.first->OnEventOnce()
        .ChainOnce<SerializableMessage_TextMessage>([this](const SerializableMessage_TextMessage &msg, const Subscription& sub) {
            ASSERT_EQ(msg.text, "hi");
            ASSERT_EQ(msg.val, 123);
            msg.GetReturnChannelWriter()->Send<SerializableMessage_TextMessage>
              ("no channel, dont use this", "hi back", 779);
            sub.CloseChannel(); // close "main"
          });
    }
  };

  // emulate flags like it's a multiprocess system, these may be alredy set by default
  FLAGS_address = "127.0.0.1";
  FLAGS_port = 10000;

  System &system = System::GetInstance();
  Distributed &distributed = Distributed::GetInstance();
  distributed.StartServices();

  system.Spawn<Master>("master");
  system.Spawn<Worker>("worker");

  system.AwaitShutdown(); // this must be called before StopServices
  distributed.StopServices();
}


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
