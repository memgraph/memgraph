#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "communication/reactor/reactor_local.hpp"
#include "gtest/gtest.h"
#include "utils/exceptions.hpp"

using namespace std::literals::chrono_literals;
using namespace communication::reactor;
using Subscription = EventStream::Subscription;

TEST(SystemTest, ReturnWithoutThrowing) {
  System system;
  auto master =
      system.Spawn("master", [](Reactor &r) { r.CloseChannel("main"); });
  std::this_thread::sleep_for(100ms);
}

TEST(ChannelCreationTest, ThrowOnReusingChannelName) {
  System system;
  auto master = system.Spawn("master", [](Reactor &r) {
    r.Open("channel");
    ASSERT_THROW(r.Open("channel"), utils::BasicException);
    r.CloseChannel("main");
    r.CloseChannel("channel");
  });
  std::this_thread::sleep_for(100ms);
}

TEST(SimpleSendTest, OneCallback) {
  struct MessageInt : public Message {
    MessageInt(int xx) : x(xx) {}
    int x;
  };

  System system;
  auto master = system.Spawn("master", [](Reactor &r) {
    std::this_thread::sleep_for(100ms);
    auto channel_writer = r.system_.FindChannel("worker", "main");
    channel_writer->Send<MessageInt>(888);
    r.CloseChannel("main");
  });

  auto worker = system.Spawn("worker", [](Reactor &r) {
    EventStream *stream = r.main_.first;

    stream->OnEvent<MessageInt>(
        [&r](const MessageInt &msg, const Subscription &) {
          ASSERT_EQ(msg.x, 888);
          r.CloseChannel("main");
        });
  });
  std::this_thread::sleep_for(200ms);
}

TEST(SimpleSendTest, IgnoreAfterClose) {
  struct MessageInt : public Message {
    MessageInt(int xx) : x(xx) {}
    int x;
  };

  System system;

  auto master = system.Spawn("master", [](Reactor &r) {
    std::this_thread::sleep_for(100ms);
    std::shared_ptr<ChannelWriter> channel_writer =
        r.system_.FindChannel("worker", "main");
    channel_writer->Send<MessageInt>(101);
    channel_writer->Send<MessageInt>(102);  // should be ignored
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    channel_writer->Send<MessageInt>(103);  // should be ignored
    channel_writer->Send<MessageInt>(104);  // should be ignored
    // Write-end doesn't need to be closed because it's in RAII.
    r.CloseChannel("main");
  });

  auto worker = system.Spawn("worker", [](Reactor &r) {
    EventStream *stream = r.main_.first;
    stream->OnEvent<MessageInt>(
        [&r](const MessageInt &msg, const Subscription &) {
          r.CloseChannel("main");
          ASSERT_EQ(msg.x, 101);
        });
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(500));
}

TEST(SimpleSendTest, RecreateChannelAfterClosing) {
  struct MessageInt : public Message {
    MessageInt(int xx) : x(xx) {}
    int x;
  };

  System system;

  auto master = system.Spawn("master", [](Reactor &r) {
    std::this_thread::sleep_for(100ms);
    auto channel_writer = r.system_.FindChannel("worker", "main");
    // Original "worker" reactor will die after it process this message.
    channel_writer->Send<MessageInt>(101);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // This message will be dropped since there is no reactor with name
    // "worker".
    channel_writer->Send<MessageInt>(102);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // This message should recieved by new "worker" reactor.
    channel_writer->Send<MessageInt>(103);
    r.CloseChannel("main");
  });

  auto worker = system.Spawn("worker", [](Reactor &r) {
    EventStream *stream = r.main_.first;
    stream->OnEvent<MessageInt>(
        [&r](const MessageInt &msg, const Subscription &) {
          r.CloseChannel("main");
          ASSERT_EQ(msg.x, 101);
        });
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  auto worker2 = system.Spawn("worker", [](Reactor &r) {
    EventStream *stream = r.main_.first;
    stream->OnEvent<MessageInt>(
        [&r](const MessageInt &msg, const Subscription &) {
          r.CloseChannel("main");
          ASSERT_EQ(msg.x, 103);
        });
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(300));
}

TEST(SimpleSendTest, DuringFirstEvent) {
  struct MessageInt : public Message {
    MessageInt(int xx) : x(xx) {}
    int x;
  };

  System system;

  std::promise<int> p;
  auto f = p.get_future();
  auto master = system.Spawn("master", [&p](Reactor &r) mutable {
    EventStream *stream = r.main_.first;

    stream->OnEvent<MessageInt>(
        [&](const Message &msg, const Subscription &subscription) {
          const MessageInt &msgint = dynamic_cast<const MessageInt &>(msg);
          if (msgint.x == 101) r.FindChannel("main")->Send<MessageInt>(102);
          if (msgint.x == 102) {
            subscription.Unsubscribe();
            r.CloseChannel("main");
            p.set_value(777);
          }
        });

    std::shared_ptr<ChannelWriter> channel_writer = r.FindChannel("main");
    channel_writer->Send<MessageInt>(101);
  });

  f.wait();
  ASSERT_EQ(f.get(), 777);
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

  System system;

  auto master = system.Spawn("master", [](Reactor &r) {
    std::this_thread::sleep_for(100ms);
    std::shared_ptr<ChannelWriter> channel_writer =
        r.system_.FindChannel("worker", "main");
    channel_writer->Send<MessageInt>(55);
    channel_writer->Send<MessageInt>(66);
    channel_writer->Send<MessageInt>(77);
    channel_writer->Send<MessageInt>(88);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    channel_writer->Send<MessageChar>('a');
    channel_writer->Send<MessageChar>('b');
    channel_writer->Send<MessageChar>('c');
    channel_writer->Send<MessageChar>('d');
    r.CloseChannel("main");
  });

  auto worker =
      system.Spawn("worker", [num_received_messages = 0](Reactor & r) mutable {
        EventStream *stream = r.main_.first;

        stream->OnEvent<MessageInt>(
            [&](const MessageInt &msgint, const Subscription &subscription) {
              ASSERT_TRUE(msgint.x == 55 || msgint.x == 66);
              ++num_received_messages;
              if (msgint.x == 66) {
                subscription.Unsubscribe();  // receive only two of them
              }
            });
        stream->OnEvent<MessageChar>(
            [&](const MessageChar &msgchar, const Subscription &subscription) {
              char c = msgchar.x;
              ++num_received_messages;
              ASSERT_TRUE(c == 'a' || c == 'b' || c == 'c');
              if (num_received_messages == 5) {
                subscription.Unsubscribe();
                r.CloseChannel("main");
              }
            });
      });

  std::this_thread::sleep_for(std::chrono::milliseconds(500));
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

  System system;
  auto master = system.Spawn("master", [](Reactor &r) {
    std::this_thread::sleep_for(100ms);
    std::shared_ptr<ChannelWriter> channel_writer =
        r.system_.FindChannel("worker", "main");

    channel_writer->Send<MessageInt>(101);
    channel_writer->Send<MessageChar>('a');
    channel_writer->Send<MessageInt>(103);
    channel_writer->Send<MessageChar>('b');
    r.CloseChannel("main");
  });

  auto worker = system.Spawn("worker", [correct_vals = 0](Reactor & r) mutable {
    struct EndMessage : Message {};
    EventStream *stream = r.main_.first;

    stream->OnEvent<MessageInt>(
        [&](const MessageInt &msgint, const Subscription &) {
          ASSERT_TRUE(msgint.x == 101 || msgint.x == 103);
          ++correct_vals;
          r.main_.second->Send<EndMessage>();
        });

    stream->OnEvent<MessageChar>(
        [&](const MessageChar &msgchar, const Subscription &) {
          ASSERT_TRUE(msgchar.x == 'a' || msgchar.x == 'b');
          ++correct_vals;
          r.main_.second->Send<EndMessage>();
        });

    stream->OnEvent<EndMessage>([&](const EndMessage &, const Subscription &) {
      ASSERT_LE(correct_vals, 4);
      if (correct_vals == 4) {
        r.CloseChannel("main");
      }
    });
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(300));
}

TEST(MultipleSendTest, Chaining) {
  struct MessageInt : public Message {
    MessageInt(int xx) : x(xx) {}
    int x;
  };

  System system;

  auto master = system.Spawn("master", [](Reactor &r) {
    std::this_thread::sleep_for(100ms);
    std::shared_ptr<ChannelWriter> channel_writer =
        r.system_.FindChannel("worker", "main");
    channel_writer->Send<MessageInt>(55);
    channel_writer->Send<MessageInt>(66);
    channel_writer->Send<MessageInt>(77);
    r.CloseChannel("main");
  });

  auto worker = system.Spawn("worker", [](Reactor &r) {
    EventStream *stream = r.main_.first;

    stream->OnEventOnce()
        .ChainOnce<MessageInt>([](const MessageInt &msg, const Subscription &) {
          ASSERT_EQ(msg.x, 55);
        })
        .ChainOnce<MessageInt>([](const MessageInt &msg, const Subscription &) {
          ASSERT_EQ(msg.x, 66);
        })
        .ChainOnce<MessageInt>(
            [&](const MessageInt &msg, const Subscription &) {
              ASSERT_EQ(msg.x, 77);
              r.CloseChannel("main");
            });
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
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

  System system;

  auto master = system.Spawn("master", [](Reactor &r) {
    std::this_thread::sleep_for(100ms);
    std::shared_ptr<ChannelWriter> channel_writer =
        r.system_.FindChannel("worker", "main");
    channel_writer->Send<MessageChar>('a');
    channel_writer->Send<MessageInt>(55);
    channel_writer->Send<MessageChar>('b');
    channel_writer->Send<MessageInt>(77);
    r.CloseChannel("main");
  });

  auto worker = system.Spawn("worker", [](Reactor &r) {
    std::this_thread::sleep_for(100ms);
    EventStream *stream = r.main_.first;
    stream->OnEventOnce()
        .ChainOnce<MessageInt>([](const MessageInt &msg, const Subscription &) {
          ASSERT_EQ(msg.x, 55);
        })
        .ChainOnce<MessageChar>(
            [](const MessageChar &msg, const Subscription &) {
              ASSERT_EQ(msg.x, 'b');
            })
        .ChainOnce<MessageInt>(
            [&](const MessageInt &msg, const Subscription &) {
              ASSERT_EQ(msg.x, 77);
              r.CloseChannel("main");
            });
  });

  std::this_thread::sleep_for(300ms);
}

TEST(MultipleSendTest, ProcessManyMessages) {
  const static int kNumTests = 100;

  struct MessageInt : public Message {
    MessageInt(int xx) : x(xx) {}
    int x;
  };

  System system;

  auto master = system.Spawn("master", [](Reactor &r) {
    std::this_thread::sleep_for(100ms);
    std::shared_ptr<ChannelWriter> channel_writer =
        r.system_.FindChannel("worker", "main");

    for (int i = 0; i < kNumTests; ++i) {
      channel_writer->Send<MessageInt>(rand());
      std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 5));
    }
    r.CloseChannel("main");
  });

  auto worker = system.Spawn("worker", [vals = 0](Reactor & r) mutable {
    struct EndMessage : Message {};
    EventStream *stream = r.main_.first;
    vals = 0;

    stream->OnEvent<MessageInt>([&](const Message &, const Subscription &) {
      ++vals;
      r.main_.second->Send<EndMessage>();
    });

    stream->OnEvent<EndMessage>([&](const Message &, const Subscription &) {
      ASSERT_LE(vals, kNumTests);
      if (vals == kNumTests) {
        r.CloseChannel("main");
      }
    });
  });

  std::this_thread::sleep_for(1000ms);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
