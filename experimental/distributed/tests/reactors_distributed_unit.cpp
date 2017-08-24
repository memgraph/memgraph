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

TEST(SimpleTests, StartAndStopServices) {
  System &system = System::GetInstance();
  Distributed &distributed = Distributed::GetInstance();
  distributed.StartServices();

  // do nothing
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  system.AwaitShutdown();
  distributed.StopServices();
}

TEST(SimpleTests, SendEmptyMessage) {
  struct Master : public Reactor {
    Master(std::string name) : Reactor(name) {}

    virtual void Run() {
      Distributed::GetInstance().FindChannel("127.0.0.1", 10000, "worker", "main")
        ->OnEventOnce()
        .ChainOnce<ChannelResolvedMessage>([this](const ChannelResolvedMessage& msg,
                                                  const Subscription& subscription) {
            msg.channelWriter()->Send<Message>();
            subscription.Close();
          });

      CloseChannel("main");
    }
  };

  struct Worker : public Reactor {
    Worker(std::string name) : Reactor(name) {}

    virtual void Run() {
      main_.first->OnEventOnce()
        .ChainOnce<Message>([this](const Message&, const Subscription& subscription) {
            // if this message isn't delivered, the main channel will never be closed
            subscription.Close(); // close "main"
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
