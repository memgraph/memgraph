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
using memgraph::storage::replication::InProgressRes;

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
  Save(SumRes::kType.id, builder);
  Save(memgraph::rpc::current_version, builder);
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

constexpr int port{8182};

// TODO: multiple progresses
// TODO: timeout final
TEST(RpcInProgress, SingleProgress) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([](auto *req_reader, auto *res_builder) {
    spdlog::trace("Started executing sum callback");
    SumReq req;
    Load(&req, req_reader);

    spdlog::trace("Loaded sum req request");

    // Simulate work
    std::this_thread::sleep_for(100ms);
    InProgressRes progress1;
    Save(progress1, res_builder);
    res_builder->Finalize();
    spdlog::trace("Saved InProgressRes");

    // Simulate done
    std::this_thread::sleep_for(300ms);
    SumRes res{5};
    Save(res, res_builder);
    spdlog::trace("Saved SumRes response");
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, 2000)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<Sum>(2, 3);
  auto reply = stream.AwaitResponseWhileInProgress();
  EXPECT_EQ(reply.sum, 5);
}

TEST(RpcInProgress, Timeout) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([](auto *req_reader, auto *res_builder) {
    spdlog::trace("Started executing sum callback");
    SumReq req;
    Load(&req, req_reader);

    spdlog::trace("Loaded sum req request");

    // Simulate work
    std::this_thread::sleep_for(100ms);
    InProgressRes progress1;
    Save(progress1, res_builder);
    res_builder->Finalize();
    spdlog::trace("Saved InProgressRes");

    // Simulate done
    std::this_thread::sleep_for(300ms);
    SumRes res{5};
    Save(res, res_builder);
    spdlog::trace("Saved SumRes response");
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, 200)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<Sum>(2, 3);
  EXPECT_THROW(stream.AwaitResponseWhileInProgress(), GenericRpcFailedException);
}
