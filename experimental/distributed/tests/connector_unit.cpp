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
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
      CloseConnector("main");
    }
  };

  struct Worker : public Reactor {
    Worker(System *system, std::string name) : Reactor(system, name) {}
    virtual void Run() {
      std::shared_ptr<Channel> channel;
      while (!(channel = system_->FindChannel("master", "main")))
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
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
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
      channel->Send<MessageInt>(123);
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

TEST(SimpleSendTest, OneCallback) {
  struct MessageInt : public Message {
    MessageInt(int xx) : x(xx) {}
    int x;
  };

  struct Master : public Reactor {
    Master(System *system, std::string name) : Reactor(system, name) {}
    virtual void Run() {
      std::shared_ptr<Channel> channel;
      while (!(channel = system_->FindChannel("worker", "main")))
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
      channel->Send<MessageInt>(888);
      CloseConnector("main");
    }
  };

  struct Worker : public Reactor {
    Worker(System *system, std::string name) : Reactor(system, name) {}
    virtual void Run() {
      EventStream* stream = main_.first;

      stream->OnEvent<MessageInt>([this](const MessageInt& msg, const EventStream::Subscription&) {
          ASSERT_EQ(msg.x, 888);
          CloseConnector("main");
        });
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
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
      channel->Send<MessageInt>(101);
      channel->Send<MessageInt>(102);
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
      channel->Send<MessageInt>(103); // should be ignored
      channel->Send<MessageInt>(104); // should be ignored
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


TEST(MultipleSendTest, UnsubscribeService) {
  struct MessageInt : public Message {
    MessageInt(int xx) : x(xx) {}
    int x;
  };
  struct MessageChar : public Message {
    MessageChar(char xx) : x(xx) {}
    char x;
  };

  struct Master : public Reactor {
    Master(System *system, std::string name) : Reactor(system, name) {}
    virtual void Run() {
      std::shared_ptr<Channel> channel;
      while (!(channel = system_->FindChannel("worker", "main")))
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
      channel->Send<MessageInt>(55);
      channel->Send<MessageInt>(66);
      channel->Send<MessageInt>(77);
      channel->Send<MessageInt>(88);
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
      channel->Send<MessageChar>('a');
      channel->Send<MessageChar>('b');
      channel->Send<MessageChar>('c');
      channel->Send<MessageChar>('d');
      CloseConnector("main");
    }
  };

  struct Worker : public Reactor {
    Worker(System *system, std::string name) : Reactor(system, name) {}

    int num_msgs_received = 0;

    virtual void Run() {
      EventStream* stream = main_.first;

      stream->OnEvent<MessageInt>([this](const MessageInt& msgint, const EventStream::Subscription& subscription) {
          ASSERT_TRUE(msgint.x == 55 || msgint.x == 66);
          ++num_msgs_received;
          if (msgint.x == 66) {
            subscription.unsubscribe(); // receive only two of them
          }
        });
      stream->OnEvent<MessageChar>([this](const MessageChar& msgchar, const EventStream::Subscription& subscription) {
          char c = msgchar.x;
          ++num_msgs_received;
          ASSERT_TRUE(c == 'a' || c == 'b' || c == 'c');
          if (num_msgs_received == 5) {
            subscription.unsubscribe();
            CloseConnector("main");
          }
        });
    }
  };

  System system;
  system.Spawn<Master>("master");
  system.Spawn<Worker>("worker");
  system.AwaitShutdown();
}


TEST(MultipleSendTest, OnEvent) {
  struct MessageInt : public Message {
    MessageInt(int xx) : x(xx) {}
    int x;
  };
  struct MessageChar : public Message {
    MessageChar(char xx) : x(xx) {}
    char x;
  };

  struct Master : public Reactor {
    Master(System *system, std::string name) : Reactor(system, name) {}
    virtual void Run() {
      std::shared_ptr<Channel> channel;
      while (!(channel = system_->FindChannel("worker", "main")))
        std::this_thread::sleep_for(std::chrono::milliseconds(300));

      channel->Send<MessageInt>(101);
      channel->Send<MessageChar>('a');
      channel->Send<MessageInt>(103);
      channel->Send<MessageChar>('b');
      CloseConnector("main");
    }
  };

  struct Worker : public Reactor {
    Worker(System *system, std::string name) : Reactor(system, name) {}

    struct EndMessage : Message {};
    int correct_vals = 0;

    virtual void Run() {
      EventStream* stream = main_.first;
      correct_vals = 0;

      stream->OnEvent<MessageInt>([this](const MessageInt& msgint, const EventStream::Subscription&) {
          ASSERT_TRUE(msgint.x == 101 || msgint.x == 103);
          ++correct_vals;
          main_.second->Send<EndMessage>();
        });

      stream->OnEvent<MessageChar>([this](const MessageChar& msgchar, const EventStream::Subscription&) {
          ASSERT_TRUE(msgchar.x == 'a' || msgchar.x == 'b');
          ++correct_vals;
          main_.second->Send<EndMessage>();
        });

      stream->OnEvent<EndMessage>([this](const EndMessage&, const EventStream::Subscription&) {
          ASSERT_LE(correct_vals, 4);
          if (correct_vals == 4) {
            CloseConnector("main");
          }
        });
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
