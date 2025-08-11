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

#include <storage/v2/replication/replication_client.hpp>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "rpc_messages.hpp"

#include "rpc/client.hpp"
#include "rpc/server.hpp"
#include "rpc/utils.hpp"  // Needs to be included last so that SLK definitions are seen

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

void Save(const SumRes &res, Builder *builder) { Save(res.sum, builder); }

void Load(SumRes *res, Reader *reader) { Load(&res->sum, reader); }

}  // namespace memgraph::slk

void SumReq::Load(SumReq *obj, memgraph::slk::Reader *reader) { memgraph::slk::Load(obj, reader); }
void SumReq::Save(const SumReq &obj, memgraph::slk::Builder *builder) { memgraph::slk::Save(obj, builder); }

void SumRes::Load(SumRes *obj, memgraph::slk::Reader *reader) { memgraph::slk::Load(obj, reader); }
void SumRes::Save(const SumRes &obj, memgraph::slk::Builder *builder) { memgraph::slk::Save(obj, builder); }

constexpr int port{8182};

TEST(RpcInProgress, SingleProgress) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([](uint64_t const request_version, auto *req_reader, auto *res_builder) {
    spdlog::trace("Started executing sum callback");
    SumReq req;
    Load(&req, req_reader);

    spdlog::trace("Loaded sum req request");

    // Simulate work
    std::this_thread::sleep_for(100ms);
    memgraph::rpc::SendInProgressMsg(res_builder);
    spdlog::trace("Saved InProgressRes");

    // Simulate done
    std::this_thread::sleep_for(300ms);
    SumRes res{5};
    memgraph::rpc::SendFinalResponse(res, res_builder);
    spdlog::trace("Saved SumRes response");
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, 2000)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<Sum>(2, 3);
  auto reply = stream.SendAndWaitProgress();
  EXPECT_EQ(reply.sum, 5);
}

// Each batch for itself shouldn't timeout
TEST(RpcInProgress, MultipleProgresses) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([](uint64_t const request_version, auto *req_reader, auto *res_builder) {
    spdlog::trace("Started executing sum callback");
    SumReq req;
    Load(&req, req_reader);

    spdlog::trace("Loaded sum req request");

    // Simulate work
    std::this_thread::sleep_for(100ms);
    memgraph::rpc::SendInProgressMsg(res_builder);
    spdlog::trace("Saved InProgressRes");

    // Simulate work
    std::this_thread::sleep_for(200ms);
    memgraph::rpc::SendInProgressMsg(res_builder);
    spdlog::trace("Saved InProgressRes");

    // Simulate work
    std::this_thread::sleep_for(250ms);
    memgraph::rpc::SendInProgressMsg(res_builder);
    spdlog::trace("Saved InProgressRes");

    // Simulate done
    std::this_thread::sleep_for(300ms);
    SumRes res{5};
    memgraph::rpc::SendFinalResponse(res, res_builder);
    spdlog::trace("Saved SumRes response");
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, 500)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<Sum>(2, 3);
  auto reply = stream.SendAndWaitProgress();
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

  rpc_server.Register<Sum>([](uint64_t const request_version, auto *req_reader, auto *res_builder) {
    spdlog::trace("Started executing sum callback");
    SumReq req;
    Load(&req, req_reader);

    spdlog::trace("Loaded sum req request");

    // Simulate work
    std::this_thread::sleep_for(100ms);
    memgraph::rpc::SendInProgressMsg(res_builder);
    spdlog::trace("Saved InProgressRes");

    // Simulate done
    std::this_thread::sleep_for(300ms);
    SumRes res{5};
    memgraph::rpc::SendFinalResponse(res, res_builder);
    spdlog::trace("Saved SumRes response");
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, 200)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<Sum>(2, 3);
  EXPECT_THROW(stream.SendAndWaitProgress(), GenericRpcFailedException);
}

TEST(RpcInProgress, NoTimeout) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([](uint64_t const request_version, auto *req_reader, auto *res_builder) {
    spdlog::trace("Started executing sum callback");
    SumReq req;
    Load(&req, req_reader);

    spdlog::trace("Loaded sum req request");
    SumRes res{5};
    memgraph::rpc::SendFinalResponse(res, res_builder);
    spdlog::trace("Saved SumRes response");
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, 200)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<Sum>(2, 3);
  EXPECT_NO_THROW(stream.SendAndWaitProgress());
}
