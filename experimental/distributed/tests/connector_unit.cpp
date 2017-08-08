#include "gtest/gtest.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "communication.hpp"

TEST(SystemTest, ReturnWithoutThrowing) {
  struct Master : public Reactor {
    Master(System *system, std::string name) : Reactor(system, name) {}
    virtual void Run() {
      CloseConnector("main");
    }
  };

  System system;
  ASSERT_NO_THROW(system.StartServices());
  ASSERT_NO_THROW(system.Spawn<Master>("master"));
  ASSERT_NO_THROW(system.AwaitShutdown());
}


TEST(ChannelCreationTest, ThrowOnReusingChannelName) {
  struct Master : public Reactor {
    Master(System *system, std::string name) : Reactor(system, name) {}
    virtual void Run() {
      Open("channel");
      ASSERT_THROW(Open("channel"), std::runtime_error);
      CloseConnector("main");
      CloseConnector("channel");
    }
  };

  System system;
  system.Spawn<Master>("master");
  system.AwaitShutdown();
}


TEST(ConnectorSetUpTest, CheckMainChannelIsSet) {
  struct Master : public Reactor {
    Master(System *system, std::string name) : Reactor(system, name) {}
    virtual void Run() {
      std::shared_ptr<Channel> channel;
      while (!(channel = system_->FindChannel("worker", "main")))
        std::this_thread::sleep_for(std::chrono::seconds(1));
      std::this_thread::sleep_for(std::chrono::seconds(1));
      CloseConnector("main");
    }
  };

  struct Worker : public Reactor {
    Worker(System *system, std::string name) : Reactor(system, name) {}
    virtual void Run() {
      std::shared_ptr<Channel> channel;
      while (!(channel = system_->FindChannel("master", "main")))
        std::this_thread::sleep_for(std::chrono::seconds(1));
      std::this_thread::sleep_for(std::chrono::seconds(1));
      CloseConnector("main");
    }
  };

  System system;
  system.Spawn<Master>("master");
  system.Spawn<Worker>("worker");
  system.AwaitShutdown();
}


TEST(SimpleSendTest, OneSimpleSend) {
  struct MessageInt : public Message {
    MessageInt(int xx) : x(xx) {}
    int x;
  };

  struct Master : public Reactor {
    Master(System *system, std::string name) : Reactor(system, name) {}
    virtual void Run() {
      std::shared_ptr<Channel> channel;
      while (!(channel = system_->FindChannel("worker", "main")))
        std::this_thread::sleep_for(std::chrono::seconds(1));
      channel->Send(std::make_unique<MessageInt>(123));
      CloseConnector("main"); // Write-end doesn't need to be closed because it's in RAII.
    }
  };

  struct Worker : public Reactor {
    Worker(System *system, std::string name) : Reactor(system, name) {}
    virtual void Run() {
      EventStream* stream = main_.first;
      std::unique_ptr<Message> m_uptr = stream->AwaitEvent();
      CloseConnector("main");
      MessageInt* msg = dynamic_cast<MessageInt *>(m_uptr.get());
      ASSERT_NE(msg, nullptr);
      ASSERT_EQ(msg->x, 123);
    }
  };

  System system;
  system.Spawn<Master>("master");
  system.Spawn<Worker>("worker");
  system.AwaitShutdown();
}


TEST(SimpleSendTest, IgnoreAfterClose) {
  struct MessageInt : public Message {
    MessageInt(int xx) : x(xx) {}
    int x;
  };

  struct Master : public Reactor {
    Master(System *system, std::string name) : Reactor(system, name) {}
    virtual void Run() {
      std::shared_ptr<Channel> channel;
      while (!(channel = system_->FindChannel("worker", "main")))
        std::this_thread::sleep_for(std::chrono::seconds(1));
      channel->Send(std::make_unique<MessageInt>(101));
      channel->Send(std::make_unique<MessageInt>(102));
      std::this_thread::sleep_for(std::chrono::seconds(1));
      channel->Send(std::make_unique<MessageInt>(103)); // these ones should be ignored
      channel->Send(std::make_unique<MessageInt>(104));
      CloseConnector("main"); // Write-end doesn't need to be closed because it's in RAII.
    }
  };

  struct Worker : public Reactor {
    Worker(System *system, std::string name) : Reactor(system, name) {}
    virtual void Run() {
      EventStream* stream = main_.first;
      std::unique_ptr<Message> m_uptr = stream->AwaitEvent();
      CloseConnector("main");
      MessageInt* msg = dynamic_cast<MessageInt *>(m_uptr.get());
      ASSERT_NE(msg, nullptr);
      ASSERT_EQ(msg->x, 101);
    }
  };

  System system;
  system.Spawn<Master>("master");
  system.Spawn<Worker>("worker");
  system.AwaitShutdown();
}





int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
