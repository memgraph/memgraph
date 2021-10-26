// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <thread>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "rpc/client.hpp"
#include "rpc/client_pool.hpp"
#include "rpc/messages.hpp"
#include "rpc/server.hpp"
#include "utils/timer.hpp"

#include "rpc_messages.hpp"

using namespace rpc;
using namespace std::literals::chrono_literals;

namespace slk {
void Save(const SumReq &sum, Builder *builder) {
  Save(sum.x, builder);
  Save(sum.y, builder);
}

void Load(SumReq *sum, Reader *reader) {
  Load(&sum->x, reader);
  Load(&sum->y, reader);
}

void Save(const SumRes &res, Builder *builder) { Save(res.sum, builder); }

void Load(SumRes *res, Reader *reader) { Load(&res->sum, reader); }

void Save(const EchoMessage &echo, Builder *builder) { Save(echo.data, builder); }

void Load(EchoMessage *echo, Reader *reader) { Load(&echo->data, reader); }
}  // namespace slk

void SumReq::Load(SumReq *obj, slk::Reader *reader) { slk::Load(obj, reader); }
void SumReq::Save(const SumReq &obj, slk::Builder *builder) { slk::Save(obj, builder); }

void SumRes::Load(SumRes *obj, slk::Reader *reader) { slk::Load(obj, reader); }
void SumRes::Save(const SumRes &obj, slk::Builder *builder) { slk::Save(obj, builder); }

void EchoMessage::Load(EchoMessage *obj, slk::Reader *reader) { slk::Load(obj, reader); }
void EchoMessage::Save(const EchoMessage &obj, slk::Builder *builder) { slk::Save(obj, builder); }

TEST(Rpc, Call) {
  communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Sum>([](auto *req_reader, auto *res_builder) {
    SumReq req;
    slk::Load(&req, req_reader);
    SumRes res(req.x + req.y);
    slk::Save(res, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);
  auto sum = client.Call<Sum>(10, 20);
  EXPECT_EQ(sum.sum, 30);

  server.Shutdown();
  server.AwaitShutdown();
}

TEST(Rpc, Abort) {
  communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Sum>([](auto *req_reader, auto *res_builder) {
    SumReq req;
    slk::Load(&req, req_reader);
    std::this_thread::sleep_for(500ms);
    SumRes res(req.x + req.y);
    slk::Save(res, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);

  std::thread thread([&client]() {
    std::this_thread::sleep_for(100ms);
    spdlog::info("Shutting down the connection!");
    client.Abort();
  });

  utils::Timer timer;
  EXPECT_THROW(client.Call<Sum>(10, 20), RpcFailedException);
  EXPECT_LT(timer.Elapsed(), 200ms);

  thread.join();

  server.Shutdown();
  server.AwaitShutdown();
}

TEST(Rpc, ClientPool) {
  communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Sum>([](const auto &req_reader, auto *res_builder) {
    SumReq req;
    Load(&req, req_reader);
    std::this_thread::sleep_for(100ms);
    SumRes res(req.x + req.y);
    Save(res, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);

  // These calls should take more than 400ms because we're using a regular
  // client
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

  communication::ClientContext pool_context;
  ClientPool pool(server.endpoint(), &pool_context);

  // These calls shouldn't take much more that 100ms because they execute in
  // parallel
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
  communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Echo>([](auto *req_reader, auto *res_builder) {
    EchoMessage res;
    slk::Load(&res, req_reader);
    slk::Save(res, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  std::string testdata(100000, 'a');

  communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);
  auto echo = client.Call<Echo>(testdata);
  EXPECT_EQ(echo.data, testdata);

  server.Shutdown();
  server.AwaitShutdown();
}

TEST(Rpc, JumboMessage) {
  communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Echo>([](auto *req_reader, auto *res_builder) {
    EchoMessage res;
    slk::Load(&res, req_reader);
    slk::Save(res, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  // NOLINTNEXTLINE (bugprone-string-constructor)
  std::string testdata(10000000, 'a');

  communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);
  auto echo = client.Call<Echo>(testdata);
  EXPECT_EQ(echo.data, testdata);

  server.Shutdown();
  server.AwaitShutdown();
}

TEST(Rpc, Stream) {
  communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Echo>([](auto *req_reader, auto *res_builder) {
    EchoMessage req;
    slk::Load(&req, req_reader);
    std::string payload;
    slk::Load(&payload, req_reader);
    EchoMessage res(req.data + payload);
    slk::Save(res, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);
  auto stream = client.Stream<Echo>("hello");
  slk::Save("world", stream.GetBuilder());
  auto echo = stream.AwaitResponse();
  EXPECT_EQ(echo.data, "helloworld");

  server.Shutdown();
  server.AwaitShutdown();
}

TEST(Rpc, StreamLarge) {
  communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Echo>([](auto *req_reader, auto *res_builder) {
    EchoMessage req;
    slk::Load(&req, req_reader);
    std::string payload;
    slk::Load(&payload, req_reader);
    EchoMessage res(req.data + payload);
    slk::Save(res, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  std::string testdata1(50000, 'a');
  std::string testdata2(50000, 'b');

  communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);
  auto stream = client.Stream<Echo>(testdata1);
  slk::Save(testdata2, stream.GetBuilder());
  auto echo = stream.AwaitResponse();
  EXPECT_EQ(echo.data, testdata1 + testdata2);

  server.Shutdown();
  server.AwaitShutdown();
}

TEST(Rpc, StreamJumbo) {
  communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Echo>([](auto *req_reader, auto *res_builder) {
    EchoMessage req;
    slk::Load(&req, req_reader);
    std::string payload;
    slk::Load(&payload, req_reader);
    EchoMessage res(req.data + payload);
    slk::Save(res, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  // NOLINTNEXTLINE (bugprone-string-constructor)
  std::string testdata1(5000000, 'a');
  // NOLINTNEXTLINE (bugprone-string-constructor)
  std::string testdata2(5000000, 'b');

  communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);
  auto stream = client.Stream<Echo>(testdata1);
  slk::Save(testdata2, stream.GetBuilder());
  auto echo = stream.AwaitResponse();
  EXPECT_EQ(echo.data, testdata1 + testdata2);

  server.Shutdown();
  server.AwaitShutdown();
}
