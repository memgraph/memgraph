#include <thread>

#include "capnp/serialize.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "communication/rpc/client.hpp"
#include "communication/rpc/client_pool.hpp"
#include "communication/rpc/messages.hpp"
#include "communication/rpc/server.hpp"
#include "utils/timer.hpp"

using namespace communication::rpc;
using namespace std::literals::chrono_literals;

struct SumReq {
  using Capnp = ::capnp::AnyPointer;
  static const MessageType TypeInfo;

  SumReq() {}  // Needed for serialization.
  SumReq(int x, int y) : x(x), y(y) {}
  int x;
  int y;
};

void Save(const SumReq &sum, ::capnp::AnyPointer::Builder *builder) {
  auto list_builder = builder->initAs<::capnp::List<int>>(2);
  list_builder.set(0, sum.x);
  list_builder.set(1, sum.y);
}

void Load(SumReq *sum, const ::capnp::AnyPointer::Reader &reader) {
  auto list_reader = reader.getAs<::capnp::List<int>>();
  sum->x = list_reader[0];
  sum->y = list_reader[1];
}

const MessageType SumReq::TypeInfo{0, "SumReq"};

struct SumRes {
  using Capnp = ::capnp::AnyPointer;
  static const MessageType TypeInfo;

  SumRes() {}  // Needed for serialization.
  SumRes(int sum) : sum(sum) {}

  int sum;
};

void Save(const SumRes &res, ::capnp::AnyPointer::Builder *builder) {
  auto list_builder = builder->initAs<::capnp::List<int>>(1);
  list_builder.set(0, res.sum);
}

void Load(SumRes *res, const ::capnp::AnyPointer::Reader &reader) {
  auto list_reader = reader.getAs<::capnp::List<int>>();
  res->sum = list_reader[0];
}

const MessageType SumRes::TypeInfo{1, "SumRes"};

using Sum = RequestResponse<SumReq, SumRes>;

struct EchoMessage {
  using Capnp = ::capnp::AnyPointer;
  static const MessageType TypeInfo;

  EchoMessage() {}  // Needed for serialization.
  EchoMessage(const std::string &data) : data(data) {}

  std::string data;
};

void Save(const EchoMessage &echo, ::capnp::AnyPointer::Builder *builder) {
  auto list_builder = builder->initAs<::capnp::List<::capnp::Text>>(1);
  list_builder.set(0, echo.data);
}

void Load(EchoMessage *echo, const ::capnp::AnyPointer::Reader &reader) {
  auto list_reader = reader.getAs<::capnp::List<::capnp::Text>>();
  echo->data = list_reader[0];
}

const MessageType EchoMessage::TypeInfo{2, "EchoMessage"};

using Echo = RequestResponse<EchoMessage, EchoMessage>;

TEST(Rpc, Call) {
  Server server({"127.0.0.1", 0});
  server.Register<Sum>([](const auto &req_reader, auto *res_builder) {
    SumReq req;
    Load(&req, req_reader);
    SumRes res(req.x + req.y);
    Save(res, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  Client client(server.endpoint());
  auto sum = client.Call<Sum>(10, 20);
  EXPECT_EQ(sum.sum, 30);

  server.Shutdown();
  server.AwaitShutdown();
}

TEST(Rpc, Abort) {
  Server server({"127.0.0.1", 0});
  server.Register<Sum>([](const auto &req_reader, auto *res_builder) {
    SumReq req;
    Load(&req, req_reader);
    std::this_thread::sleep_for(500ms);
    SumRes res(req.x + req.y);
    Save(res, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  Client client(server.endpoint());

  std::thread thread([&client]() {
    std::this_thread::sleep_for(100ms);
    LOG(INFO) << "Shutting down the connection!";
    client.Abort();
  });

  utils::Timer timer;
  EXPECT_THROW(client.Call<Sum>(10, 20),
               communication::rpc::RpcFailedException);
  EXPECT_LT(timer.Elapsed(), 200ms);

  thread.join();

  server.Shutdown();
  server.AwaitShutdown();
}

TEST(Rpc, ClientPool) {
  Server server({"127.0.0.1", 0});
  server.Register<Sum>([](const auto &req_reader, auto *res_builder) {
    SumReq req;
    Load(&req, req_reader);
    std::this_thread::sleep_for(100ms);
    SumRes res(req.x + req.y);
    Save(res, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  Client client(server.endpoint());

  /* these calls should take more than 400ms because we're using a regular
   * client */
  auto get_sum_client = [&client](int x, int y) {
    auto sum = client.Call<Sum>(x, y);
    EXPECT_EQ(sum.sum, x + y);
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
    EXPECT_EQ(sum.sum, x + y);
  };

  utils::Timer t2;
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back(get_sum, 2 * i, 2 * i + 1);
  }
  for (int i = 0; i < 4; ++i) {
    threads[i].join();
  }
  EXPECT_LE(t2.Elapsed(), 200ms);

  server.Shutdown();
  server.AwaitShutdown();
}

TEST(Rpc, LargeMessage) {
  Server server({"127.0.0.1", 0});
  server.Register<Echo>([](const auto &req_reader, auto *res_builder) {
    EchoMessage res;
    Load(&res, req_reader);
    Save(res, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  std::string testdata(100000, 'a');

  Client client(server.endpoint());
  auto echo = client.Call<Echo>(testdata);
  EXPECT_EQ(echo.data, testdata);

  server.Shutdown();
  server.AwaitShutdown();
}
