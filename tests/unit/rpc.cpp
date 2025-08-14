// Copyright 2025 Memgraph Ltd.
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

#include "gtest/gtest.h"
#include "rpc_messages.hpp"

#include "rpc/client.hpp"
#include "rpc/client_pool.hpp"
#include "rpc/messages.hpp"
#include "rpc/server.hpp"
#include "rpc/utils.hpp"  // Needs to be included last so that SLK definitions are seen
#include "utils/on_scope_exit.hpp"
#include "utils/timer.hpp"

using namespace memgraph::rpc;
using namespace std::literals::chrono_literals;

TEST(Rpc, Call) {
  memgraph::communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  auto const on_exit = memgraph::utils::OnScopeExit{[&] {
    server.Shutdown();
    server.AwaitShutdown();
  }};
  server.Register<Sum>([](uint64_t const request_version, auto *req_reader, auto *res_builder) {
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
    auto const sum = std::accumulate(req.nums_.begin(), req.nums_.end(), 0);
    SumRes const res({sum});
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  memgraph::communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);
  auto sum = client.Call<SumV1>(10, 20);
  EXPECT_EQ(sum.sum, 30);
}

TEST(Rpc, Abort) {
  memgraph::communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Sum>([](uint64_t const request_version, auto *req_reader, auto *res_builder) {
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
    auto const sum = std::accumulate(req.nums_.begin(), req.nums_.end(), 0);
    std::this_thread::sleep_for(500ms);
    SumRes const res({sum});
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  memgraph::communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);

  std::thread thread([&client]() {
    std::this_thread::sleep_for(100ms);
    spdlog::info("Shutting down the connection!");
    client.Abort();
  });

  memgraph::utils::Timer const timer;
  EXPECT_THROW(client.Call<SumV1>(10, 20), RpcFailedException);
  EXPECT_LT(timer.Elapsed(), 200ms);

  thread.join();

  server.Shutdown();
  server.AwaitShutdown();
}

TEST(Rpc, ClientPool) {
  memgraph::communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Sum>([](uint64_t const request_version, const auto &req_reader, auto *res_builder) {
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
    auto const sum = std::accumulate(req.nums_.begin(), req.nums_.end(), 0);
    std::this_thread::sleep_for(100ms);
    SumRes const res({sum});
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  memgraph::communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);

  // These calls should take more than 400ms because we're using a regular
  // client
  auto get_sum_client = [&client](int x, int y) {
    auto sum = client.Call<SumV1>(x, y);
    EXPECT_EQ(sum.sum, x + y);
  };

  memgraph::utils::Timer const t1;
  std::vector<std::thread> threads;
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back(get_sum_client, 2 * i, 2 * i + 1);
  }
  for (int i = 0; i < 4; ++i) {
    threads[i].join();
  }
  threads.clear();

  EXPECT_GE(t1.Elapsed(), 400ms);

  memgraph::communication::ClientContext pool_context;
  ClientPool pool(server.endpoint(), &pool_context);

  // These calls shouldn't take much more that 100ms because they execute in
  // parallel
  auto get_sum = [&pool](int x, int y) {
    auto sum = pool.Call<SumV1>(x, y);
    EXPECT_EQ(sum.sum, x + y);
  };

  memgraph::utils::Timer const t2;
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
  memgraph::communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Echo>([](uint64_t const request_version, auto *req_reader, auto *res_builder) {
    EchoMessage req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
    memgraph::rpc::SendFinalResponse(req, request_version, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  std::string testdata(100000, 'a');

  memgraph::communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);
  auto echo = client.Call<Echo>(testdata);
  EXPECT_EQ(echo.data, testdata);

  server.Shutdown();
  server.AwaitShutdown();
}

TEST(Rpc, JumboMessage) {
  memgraph::communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Echo>([](uint64_t const request_version, auto *req_reader, auto *res_builder) {
    EchoMessage req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
    memgraph::rpc::SendFinalResponse(req, request_version, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  // NOLINTNEXTLINE (bugprone-string-constructor)
  std::string testdata(10000000, 'a');

  memgraph::communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);
  auto echo = client.Call<Echo>(testdata);
  EXPECT_EQ(echo.data, testdata);

  server.Shutdown();
  server.AwaitShutdown();
}

TEST(Rpc, Stream) {
  memgraph::communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Echo>([](uint64_t const request_version, auto *req_reader, auto *res_builder) {
    EchoMessage req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
    std::string payload;
    memgraph::slk::Load(&payload, req_reader);
    EchoMessage const res(req.data + payload);
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  memgraph::communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);
  auto stream = client.Stream<Echo>("hello");
  memgraph::slk::Save("world", stream.GetBuilder());
  auto echo = stream.SendAndWait();
  EXPECT_EQ(echo.data, "helloworld");

  server.Shutdown();
  server.AwaitShutdown();
}

TEST(Rpc, StreamLarge) {
  memgraph::communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Echo>([](uint64_t const request_version, auto *req_reader, auto *res_builder) {
    EchoMessage req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
    std::string payload;
    memgraph::slk::Load(&payload, req_reader);
    EchoMessage const res(req.data + payload);
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  std::string testdata1(50000, 'a');
  std::string testdata2(50000, 'b');

  memgraph::communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);
  auto stream = client.Stream<Echo>(testdata1);
  memgraph::slk::Save(testdata2, stream.GetBuilder());
  auto echo = stream.SendAndWait();
  EXPECT_EQ(echo.data, testdata1 + testdata2);

  server.Shutdown();
  server.AwaitShutdown();
}

TEST(Rpc, StreamJumbo) {
  memgraph::communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Echo>([](uint64_t const request_version, auto *req_reader, auto *res_builder) {
    EchoMessage req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
    std::string payload;
    memgraph::slk::Load(&payload, req_reader);
    EchoMessage const res(req.data + payload);
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  // NOLINTNEXTLINE (bugprone-string-constructor)
  std::string testdata1(5000000, 'a');
  // NOLINTNEXTLINE (bugprone-string-constructor)
  std::string testdata2(5000000, 'b');

  memgraph::communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);
  auto stream = client.Stream<Echo>(testdata1);
  memgraph::slk::Save(testdata2, stream.GetBuilder());
  auto echo = stream.SendAndWait();
  EXPECT_EQ(echo.data, testdata1 + testdata2);

  server.Shutdown();
  server.AwaitShutdown();
}
