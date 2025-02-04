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

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "rpc/client.hpp"
#include "rpc/server.hpp"

#include "rpc_messages.hpp"

using memgraph::communication::ClientContext;
using memgraph::communication::ServerContext;
using memgraph::io::network::Endpoint;
using memgraph::rpc::Client;
using memgraph::rpc::GenericRpcFailedException;
using memgraph::rpc::Server;
using memgraph::slk::Load;
using memgraph::slk::Save;

using namespace std::string_view_literals;
using namespace std::literals::chrono_literals;

namespace memgraph::slk {
void Save(const SumReq &sum, Builder *builder) {
  Save(sum.x, builder);
  Save(sum.y, builder);
}

void Load(SumReq *sum, Reader *reader) {
  Load(&sum->x, reader);
  Load(&sum->y, reader);
}

void Save(const SumRes &res, Builder *builder) {
  memgraph::slk::SerializeResHeader(res, builder);
  Save(res.sum, builder);
}

void Load(SumRes *res, Reader *reader) { Load(&res->sum, reader); }

void Save(const EchoMessage &echo, Builder *builder) { Save(echo.data, builder); }

void Load(EchoMessage *echo, Reader *reader) { Load(&echo->data, reader); }
}  // namespace memgraph::slk

void SumReq::Load(SumReq *obj, memgraph::slk::Reader *reader) { memgraph::slk::Load(obj, reader); }
void SumReq::Save(const SumReq &obj, memgraph::slk::Builder *builder) { memgraph::slk::Save(obj, builder); }

void SumRes::Load(SumRes *obj, memgraph::slk::Reader *reader) { memgraph::slk::Load(obj, reader); }
void SumRes::Save(const SumRes &obj, memgraph::slk::Builder *builder) { memgraph::slk::Save(obj, builder); }

void EchoMessage::Load(EchoMessage *obj, memgraph::slk::Reader *reader) { memgraph::slk::Load(obj, reader); }
void EchoMessage::Save(const EchoMessage &obj, memgraph::slk::Builder *builder) { memgraph::slk::Save(obj, builder); }

constexpr int port{8181};

// RPC client is setup with timeout but shouldn't be triggered.
TEST(RpcTimeout, TimeoutNoFailure) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Echo>([](auto *req_reader, auto *res_builder) {
    EchoMessage req;
    Load(&req, req_reader);

    EchoMessage res{"Sending reply"};
    memgraph::slk::SerializeResHeader(res, res_builder);
    Save(res, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("EchoMessage"sv, 2000)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<Echo>("Sending request");
  auto reply = stream.AwaitResponse();
  EXPECT_EQ(reply.data, "Sending reply");
}

// Simulate something long executing on server.
TEST(RpcTimeout, TimeoutExecutionBlocks) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Echo>([](auto *req_reader, auto *res_builder) {
    EchoMessage req;
    Load(&req, req_reader);

    std::this_thread::sleep_for(1100ms);
    EchoMessage res{"Sending reply"};
    memgraph::slk::SerializeResHeader(res, res_builder);
    Save(res, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("EchoMessage"sv, 1000)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<Echo>("Sending request");
  EXPECT_THROW(stream.AwaitResponse(), GenericRpcFailedException);
}

// Simulate server with one thread being busy processing other RPC message.
TEST(RpcTimeout, TimeoutServerBusy) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([](auto *req_reader, auto *res_builder) {
    spdlog::trace("Received sum request.");
    SumReq req;
    Load(&req, req_reader);
    std::this_thread::sleep_for(2500ms);
    SumRes res(req.x + req.y);
    Save(res, res_builder);
  });

  rpc_server.Register<Echo>([](auto *req_reader, auto *res_builder) {
    spdlog::trace("Received echo request");
    EchoMessage req;
    Load(&req, req_reader);

    EchoMessage res{"Sending reply"};
    memgraph::slk::SerializeResHeader(res, res_builder);
    Save(res, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("EchoMessage"sv, 1000)};
  ClientContext sum_client_context;
  Client sum_client{endpoint, &sum_client_context};

  ClientContext echo_client_context;
  Client echo_client{endpoint, &echo_client_context, rpc_timeouts};

  // Sum request won't timeout but Echo should timeout because server has only one
  // processing thread.
  auto sum_stream = sum_client.Stream<Sum>(10, 10);
  auto echo_stream = echo_client.Stream<Echo>("Sending request");
  // Don't block main test thread so echo_stream could timeout
  auto sum_thread_ = std::jthread([&sum_stream]() { sum_stream.AwaitResponse(); });
  // Wait so that server receives first SumReq and then EchoMessage
  std::this_thread::sleep_for(100ms);
  EXPECT_THROW(echo_stream.AwaitResponse(), GenericRpcFailedException);
}

TEST(RpcTimeout, SendingToWrongSocket) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Echo>([](auto *req_reader, auto *res_builder) {
    EchoMessage req;
    Load(&req, req_reader);

    std::this_thread::sleep_for(1100ms);
    EchoMessage res{"Sending reply"};
    memgraph::slk::SerializeResHeader(res, res_builder);
    Save(res, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("EchoMessage"sv, 1000)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<Echo>("Sending request");
  EXPECT_THROW(stream.AwaitResponse(), GenericRpcFailedException);
}
