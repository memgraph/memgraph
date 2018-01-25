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

TEST(Rpc, Call) {
  System server_system({"127.0.0.1", 0});
  Server server(server_system, "main");
  server.Register<Sum>([](const SumReq &request) {
    return std::make_unique<SumRes>(request.x + request.y);
  });
  std::this_thread::sleep_for(100ms);

  Client client(server_system.endpoint(), "main");
  auto sum = client.Call<Sum>(10, 20);
  EXPECT_EQ(sum->sum, 30);
}

TEST(Rpc, Abort) {
  System server_system({"127.0.0.1", 0});
  Server server(server_system, "main");
  server.Register<Sum>([](const SumReq &request) {
    std::this_thread::sleep_for(500ms);
    return std::make_unique<SumRes>(request.x + request.y);
  });
  std::this_thread::sleep_for(100ms);

  Client client(server_system.endpoint(), "main");

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
  System server_system({"127.0.0.1", 0});
  Server server(server_system, "main", 4);
  server.Register<Sum>([](const SumReq &request) {
    std::this_thread::sleep_for(100ms);
    return std::make_unique<SumRes>(request.x + request.y);
  });
  std::this_thread::sleep_for(100ms);


  Client client(server_system.endpoint(), "main");

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

  ClientPool pool(server_system.endpoint(), "main");

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
