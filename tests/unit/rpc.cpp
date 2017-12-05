#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <thread>

#include "communication/messaging/distributed.hpp"
#include "communication/rpc/rpc.hpp"
#include "gtest/gtest.h"

using communication::messaging::System;
using communication::messaging::Message;
using namespace communication::rpc;
using namespace std::literals::chrono_literals;

struct SumReq : public Message {
  SumReq() {}  // cereal needs this
  SumReq(int x, int y) : x(x), y(y) {}
  int x;
  int y;

  template <class Archive>
  void serialize(Archive &ar) {
    ar(cereal::virtual_base_class<Message>(this), x, y);
  }
};
CEREAL_REGISTER_TYPE(SumReq);

struct SumRes : public Message {
  SumRes() {}  // cereal needs this
  SumRes(int sum) : sum(sum) {}
  int sum;

  template <class Archive>
  void serialize(Archive &ar) {
    ar(cereal::virtual_base_class<Message>(this), sum);
  }
};
CEREAL_REGISTER_TYPE(SumRes);
using Sum = RequestResponse<SumReq, SumRes>;

TEST(Rpc, Call) {
  System server_system("127.0.0.1", 10000);
  Server server(server_system, "main");
  server.Register<Sum>([](const SumReq &request) {
    return std::make_unique<SumRes>(request.x + request.y);
  });
  std::thread server_thread([&] { server.Start(); });
  std::this_thread::sleep_for(100ms);

  System client_system("127.0.0.1", 10001);
  Client client(client_system, "127.0.0.1", 10000, "main");
  auto sum = client.Call<Sum>(300ms, 10, 20);
  EXPECT_EQ(sum->sum, 30);

  server.Shutdown();
  server_thread.join();
  server_system.Shutdown();
  client_system.Shutdown();
}

TEST(Rpc, Timeout) {
  System server_system("127.0.0.1", 10000);
  Server server(server_system, "main");
  server.Register<Sum>([](const SumReq &request) {
    std::this_thread::sleep_for(300ms);
    return std::make_unique<SumRes>(request.x + request.y);
  });
  std::thread server_thread([&] { server.Start(); });
  std::this_thread::sleep_for(100ms);

  System client_system("127.0.0.1", 10001);
  Client client(client_system, "127.0.0.1", 10000, "main");
  auto sum = client.Call<Sum>(100ms, 10, 20);
  EXPECT_FALSE(sum);

  server.Shutdown();
  server_thread.join();
  server_system.Shutdown();
  client_system.Shutdown();
}
