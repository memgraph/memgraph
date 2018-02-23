#include <thread>

#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/archive/text_iarchive.hpp"
#include "boost/archive/text_oarchive.hpp"
#include "boost/serialization/access.hpp"
#include "boost/serialization/base_object.hpp"
#include "boost/serialization/export.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "communication/rpc/client.hpp"
#include "communication/rpc/client_pool.hpp"
#include "communication/rpc/messages.hpp"
#include "communication/rpc/server.hpp"
#include "utils/timer.hpp"

using namespace communication::rpc;
using namespace std::literals::chrono_literals;

struct SumReq : public Message {
  SumReq(int x, int y) : x(x), y(y) {}
  int x;
  int y;

 private:
  friend class boost::serialization::access;
  SumReq() {}  // Needed for serialization.

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<Message>(*this);
    ar &x;
    ar &y;
  }
};
BOOST_CLASS_EXPORT(SumReq);

struct SumRes : public Message {
  SumRes(int sum) : sum(sum) {}
  int sum;

 private:
  friend class boost::serialization::access;
  SumRes() {}  // Needed for serialization.

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<Message>(*this);
    ar &sum;
  }
};
BOOST_CLASS_EXPORT(SumRes);
using Sum = RequestResponse<SumReq, SumRes>;

struct EchoMessage : public Message {
  EchoMessage(const std::string &data) : data(data) {}
  std::string data;

 private:
  friend class boost::serialization::access;
  EchoMessage() {}  // Needed for serialization.

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<Message>(*this);
    ar &data;
  }
};
BOOST_CLASS_EXPORT(EchoMessage);

using Echo = RequestResponse<EchoMessage, EchoMessage>;

TEST(Rpc, Call) {
  Server server({"127.0.0.1", 0});
  server.Register<Sum>([](const SumReq &request) {
    return std::make_unique<SumRes>(request.x + request.y);
  });
  std::this_thread::sleep_for(100ms);

  Client client(server.endpoint());
  auto sum = client.Call<Sum>(10, 20);
  EXPECT_EQ(sum->sum, 30);
}

TEST(Rpc, Abort) {
  Server server({"127.0.0.1", 0});
  server.Register<Sum>([](const SumReq &request) {
    std::this_thread::sleep_for(500ms);
    return std::make_unique<SumRes>(request.x + request.y);
  });
  std::this_thread::sleep_for(100ms);

  Client client(server.endpoint());

  std::thread thread([&client]() {
    std::this_thread::sleep_for(100ms);
    LOG(INFO) << "Shutting down the connection!";
    client.Abort();
  });

  utils::Timer timer;
  auto sum = client.Call<Sum>(10, 20);
  EXPECT_EQ(sum, nullptr);
  EXPECT_LT(timer.Elapsed(), 200ms);

  thread.join();
}

TEST(Rpc, ClientPool) {
  Server server({"127.0.0.1", 0});
  server.Register<Sum>([](const SumReq &request) {
    std::this_thread::sleep_for(100ms);
    return std::make_unique<SumRes>(request.x + request.y);
  });
  std::this_thread::sleep_for(100ms);


  Client client(server.endpoint());

  /* these calls should take more than 400ms because we're using a regular
   * client */
  auto get_sum_client = [&client](int x, int y) {
    auto sum = client.Call<Sum>(x, y);
    ASSERT_TRUE(sum != nullptr);
    EXPECT_EQ(sum->sum, x + y);
  };

  utils::Timer t1;
  std::vector<std::thread> threads;
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back(get_sum_client, 2 * i, 2 * i + 1);
  }
  for (int i = 0; i < 4; ++i) {
    threads[i].join();
  }
  threads.clear();

  EXPECT_GE(t1.Elapsed(), 400ms);

  ClientPool pool(server.endpoint());

  /* these calls shouldn't take much more that 100ms because they execute in
   * parallel */
  auto get_sum = [&pool](int x, int y) {
    auto sum = pool.Call<Sum>(x, y);
    ASSERT_TRUE(sum != nullptr);
    EXPECT_EQ(sum->sum, x + y);
  };

  utils::Timer t2;
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back(get_sum, 2 * i, 2 * i + 1);
  }
  for (int i = 0; i < 4; ++i) {
    threads[i].join();
  }
  EXPECT_LE(t2.Elapsed(), 200ms);
}

TEST(Rpc, LargeMessage) {
  Server server({"127.0.0.1", 0});
  server.Register<Echo>([](const EchoMessage &request) {
    return std::make_unique<EchoMessage>(request.data);
  });
  std::this_thread::sleep_for(100ms);

  std::string testdata(100000, 'a');

  Client client(server.endpoint());
  auto echo = client.Call<Echo>(testdata);
  EXPECT_EQ(echo->data, testdata);
}
