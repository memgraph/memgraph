#include "gtest/gtest.h"
#include "reactors_local.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <future>

TEST(SystemTest, ReturnWithoutThrowing) {
  struct Master : public Reactor {
    Master(std::string name) : Reactor(name) {}
    virtual void Run() {
      CloseChannel("main");
    }
  };

  System &system = System::GetInstance();
  ASSERT_NO_THROW(system.Spawn<Master>("master"));
  ASSERT_NO_THROW(system.AwaitShutdown());
}


TEST(ChannelCreationTest, ThrowOnReusingChannelName) {
  struct Master : public Reactor {
    Master(std::string name) : Reactor(name) {}
    virtual void Run() {
      Open("channel");
      ASSERT_THROW(Open("channel"), std::runtime_error);
      CloseChannel("main");
      CloseChannel("channel");
    }
  };

  System &system = System::GetInstance();
  system.Spawn<Master>("master");
  system.AwaitShutdown();
}


TEST(ChannelSetUpTest, CheckMainChannelIsSet) {
  struct Master : public Reactor {
    Master(std::string name) : Reactor(name) {}
    virtual void Run() {
      std::shared_ptr<ChannelWriter> channel_writer;
      while (!(channel_writer = System::GetInstance().FindChannel("worker", "main")))
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
      CloseChannel("main");
    }
  };

  struct Worker : public Reactor {
    Worker(std::string name) : Reactor(name) {}
    virtual void Run() {
      std::shared_ptr<ChannelWriter> channel_writer;
      while (!(channel_writer = System::GetInstance().FindChannel("master", "main")))
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
      CloseChannel("main");
    }
  };

  System &system = System::GetInstance();
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
    Master(std::string name) : Reactor(name) {}
    virtual void Run() {
      std::shared_ptr<ChannelWriter> channel_writer;
      while (!(channel_writer = System::GetInstance().FindChannel("worker", "main")))
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
      channel_writer->Send<MessageInt>(888);
      CloseChannel("main");
    }
  };

  struct Worker : public Reactor {
    Worker(std::string name) : Reactor(name) {}
    virtual void Run() {
      EventStream* stream = main_.first;

      stream->OnEvent<MessageInt>([this](const MessageInt &msg, const Subscription&) {
          ASSERT_EQ(msg.x, 888);
          CloseChannel("main");
        });
    }
  };

  System &system = System::GetInstance();
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
    Master(std::string name) : Reactor(name) {}
    virtual void Run() {
      std::shared_ptr<ChannelWriter> channel_writer;
      while (!(channel_writer = System::GetInstance().FindChannel("worker", "main")))
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
      channel_writer->Send<MessageInt>(101);
      channel_writer->Send<MessageInt>(102); // should be ignored
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
      channel_writer->Send<MessageInt>(103); // should be ignored
      channel_writer->Send<MessageInt>(104); // should be ignored
      CloseChannel("main"); // Write-end doesn't need to be closed because it's in RAII.
    }
  };

  struct Worker : public Reactor {
    Worker(std::string name) : Reactor(name) {}
    virtual void Run() {
      EventStream* stream = main_.first;

      stream->OnEvent<MessageInt>([this](const MessageInt& msg, const Subscription&) {
          CloseChannel("main");
          ASSERT_EQ(msg.x, 101);
        });
    }
  };

  System &system = System::GetInstance();
  system.Spawn<Master>("master");
  system.Spawn<Worker>("worker");
  system.AwaitShutdown();
}

TEST(SimpleSendTest, DuringFirstEvent) {
  struct MessageInt : public Message {
    MessageInt(int xx) : x(xx) {}
    int x;
  };

  struct Master : public Reactor {
    Master(std::string name, std::promise<int> p) : Reactor(name), p_(std::move(p)) {}
    virtual void Run() {
      EventStream* stream = main_.first;

      stream->OnEvent<MessageInt>([this](const Message &msg, const Subscription &subscription) {
        const MessageInt &msgint = dynamic_cast<const MessageInt&>(msg);
        if (msgint.x == 101)
          FindChannel("main")->Send<MessageInt>(102);
        if (msgint.x == 102) {
          subscription.Unsubscribe();
          CloseChannel("main");
          p_.set_value(777);
        }
      });

      std::shared_ptr<ChannelWriter> channel_writer = FindChannel("main");
      channel_writer->Send<MessageInt>(101);
    }
    std::promise<int> p_;
  };

  System &system = System::GetInstance();
  std::promise<int> p;
  auto f = p.get_future();
  system.Spawn<Master>("master", std::move(p));
  f.wait();
  ASSERT_EQ(f.get(), 777);
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
    Master(std::string name) : Reactor(name) {}
    virtual void Run() {
      std::shared_ptr<ChannelWriter> channel_writer;
      while (!(channel_writer = System::GetInstance().FindChannel("worker", "main")))
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
      channel_writer->Send<MessageInt>(55);
      channel_writer->Send<MessageInt>(66);
      channel_writer->Send<MessageInt>(77);
      channel_writer->Send<MessageInt>(88);
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
      channel_writer->Send<MessageChar>('a');
      channel_writer->Send<MessageChar>('b');
      channel_writer->Send<MessageChar>('c');
      channel_writer->Send<MessageChar>('d');
      CloseChannel("main");
    }
  };

  struct Worker : public Reactor {
    Worker(std::string name) : Reactor(name) {}

    int num_msgs_received = 0;

    virtual void Run() {
      EventStream* stream = main_.first;

      stream->OnEvent<MessageInt>([this](const MessageInt &msgint, const Subscription &subscription) {
          ASSERT_TRUE(msgint.x == 55 || msgint.x == 66);
          ++num_msgs_received;
          if (msgint.x == 66) {
            subscription.Unsubscribe(); // receive only two of them
          }
        });
      stream->OnEvent<MessageChar>([this](const MessageChar &msgchar, const Subscription &subscription) {
          char c = msgchar.x;
          ++num_msgs_received;
          ASSERT_TRUE(c == 'a' || c == 'b' || c == 'c');
          if (num_msgs_received == 5) {
            subscription.Unsubscribe();
            CloseChannel("main");
          }
        });
    }
  };

  System &system = System::GetInstance();
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
    Master(std::string name) : Reactor(name) {}
    virtual void Run() {
      std::shared_ptr<ChannelWriter> channel_writer;
      while (!(channel_writer = System::GetInstance().FindChannel("worker", "main")))
        std::this_thread::sleep_for(std::chrono::milliseconds(300));

      channel_writer->Send<MessageInt>(101);
      channel_writer->Send<MessageChar>('a');
      channel_writer->Send<MessageInt>(103);
      channel_writer->Send<MessageChar>('b');
      CloseChannel("main");
    }
  };

  struct Worker : public Reactor {
    Worker(std::string name) : Reactor(name) {}

    struct EndMessage : Message {};
    int correct_vals = 0;

    virtual void Run() {
      EventStream* stream = main_.first;
      correct_vals = 0;

      stream->OnEvent<MessageInt>([this](const MessageInt &msgint, const Subscription&) {
          ASSERT_TRUE(msgint.x == 101 || msgint.x == 103);
          ++correct_vals;
          main_.second->Send<EndMessage>();
        });

      stream->OnEvent<MessageChar>([this](const MessageChar &msgchar, const Subscription&) {
          ASSERT_TRUE(msgchar.x == 'a' || msgchar.x == 'b');
          ++correct_vals;
          main_.second->Send<EndMessage>();
        });

      stream->OnEvent<EndMessage>([this](const EndMessage&, const Subscription&) {
          ASSERT_LE(correct_vals, 4);
          if (correct_vals == 4) {
            CloseChannel("main");
          }
        });
    }
  };

  System &system = System::GetInstance();
  system.Spawn<Master>("master");
  system.Spawn<Worker>("worker");
  system.AwaitShutdown();
}

TEST(MultipleSendTest, Chaining) {
  struct MessageInt : public Message {
    MessageInt(int xx) : x(xx) {}
    int x;
  };

  struct Master : public Reactor {
    Master(std::string name) : Reactor(name) {}
    virtual void Run() {
      std::shared_ptr<ChannelWriter> channel_writer;
      while (!(channel_writer = System::GetInstance().FindChannel("worker", "main")))
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
      channel_writer->Send<MessageInt>(55);
      channel_writer->Send<MessageInt>(66);
      channel_writer->Send<MessageInt>(77);
      CloseChannel("main");
    }
  };

  struct Worker : public Reactor {
    Worker(std::string name) : Reactor(name) {}

    virtual void Run() {
      EventStream* stream = main_.first;

      stream->OnEventOnce()
        .ChainOnce<MessageInt>([this](const MessageInt &msg, const Subscription&) {
            ASSERT_EQ(msg.x, 55);
          })
        .ChainOnce<MessageInt>([](const MessageInt &msg, const Subscription&) {
            ASSERT_EQ(msg.x, 66);
          })
        .ChainOnce<MessageInt>([this](const MessageInt &msg, const Subscription&) {
            ASSERT_EQ(msg.x, 77);
            CloseChannel("main");
          });
    }
  };

  System &system = System::GetInstance();
  system.Spawn<Master>("master");
  system.Spawn<Worker>("worker");
  system.AwaitShutdown();
}


TEST(MultipleSendTest, ChainingInRightOrder) {
  struct MessageInt : public Message {
    MessageInt(int xx) : x(xx) {}
    int x;
  };

  struct MessageChar : public Message {
    MessageChar(char xx) : x(xx) {}
    char x;
  };

  struct Master : public Reactor {
    Master(std::string name) : Reactor(name) {}
    virtual void Run() {
      std::shared_ptr<ChannelWriter> channel_writer;
      while (!(channel_writer = System::GetInstance().FindChannel("worker", "main")))
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
      channel_writer->Send<MessageChar>('a');
      channel_writer->Send<MessageInt>(55);
      channel_writer->Send<MessageChar>('b');
      channel_writer->Send<MessageInt>(77);
      CloseChannel("main");
    }
  };

  struct Worker : public Reactor {
    Worker(std::string name) : Reactor(name) {}

    virtual void Run() {
      EventStream* stream = main_.first;

      stream->OnEventOnce()
        .ChainOnce<MessageInt>([this](const MessageInt &msg, const Subscription&) {
            ASSERT_EQ(msg.x, 55);
          })
        .ChainOnce<MessageChar>([](const MessageChar &msg, const Subscription&) {
            ASSERT_EQ(msg.x, 'b');
          })
        .ChainOnce<MessageInt>([this](const MessageInt &msg, const Subscription&) {
            ASSERT_EQ(msg.x, 77);
            CloseChannel("main");
          });
    }
  };

  System &system = System::GetInstance();
  system.Spawn<Master>("master");
  system.Spawn<Worker>("worker");
  system.AwaitShutdown();
}


TEST(MultipleSendTest, ProcessManyMessages) {
  const static int num_tests = 100;

  struct MessageInt : public Message {
    MessageInt(int xx) : x(xx) {}
    int x;
  };

  struct Master : public Reactor {
    Master(std::string name) : Reactor(name) {}
    virtual void Run() {
      std::shared_ptr<ChannelWriter> channel_writer;
      while (!(channel_writer = System::GetInstance().FindChannel("worker", "main")))
        std::this_thread::sleep_for(std::chrono::milliseconds(300));

      std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 100));
      for (int i = 0; i < num_tests; ++i) {
        channel_writer->Send<MessageInt>(rand());
        std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 5));
      }
      CloseChannel("main");
    }
  };

  struct Worker : public Reactor {
    Worker(std::string name) : Reactor(name) {}

    struct EndMessage : Message {};
    int vals = 0;

    virtual void Run() {
      EventStream* stream = main_.first;
      vals = 0;

      stream->OnEvent<MessageInt>([this](const Message&, const Subscription&) {
          ++vals;
          main_.second->Send<EndMessage>();
        });

      stream->OnEvent<EndMessage>([this](const Message&, const Subscription&) {
          ASSERT_LE(vals, num_tests);
          if (vals == num_tests) {
            CloseChannel("main");
          }
        });
    }
  };

  System &system = System::GetInstance();
  system.Spawn<Master>("master");
  system.Spawn<Worker>("worker");
  system.AwaitShutdown();
}





int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
