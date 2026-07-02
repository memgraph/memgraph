// Copyright 2026 Memgraph Ltd.
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

#include <atomic>
#include <chrono>
#include <thread>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "rpc_messages.hpp"

#include "rpc/client.hpp"
#include "rpc/file_replication_handler.hpp"
#include "rpc/server.hpp"
#include "rpc/utils.hpp"  // Needs to be included last so that SLK definitions are seen

using memgraph::communication::ClientContext;
using memgraph::communication::ServerContext;
using memgraph::io::network::Endpoint;
using memgraph::rpc::Client;
using memgraph::rpc::GenericRpcFailedException;
using memgraph::rpc::RpcTimeoutException;
using memgraph::rpc::Server;

using namespace std::string_view_literals;
using namespace std::literals::chrono_literals;

constexpr int port{8185};

TEST(RpcInProgress, SingleProgress) {
  Endpoint const endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                              uint64_t const request_version,
                              auto *req_reader,
                              auto *res_builder) {
    spdlog::trace("Started executing sum callback");
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

    spdlog::trace("Loaded sum req request");

    // Simulate work
    std::this_thread::sleep_for(100ms);
    memgraph::rpc::SendInProgressMsg(res_builder);
    spdlog::trace("Saved InProgressRes");

    // Simulate done
    std::this_thread::sleep_for(300ms);
    SumRes const res{5};
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
    spdlog::trace("Saved SumRes response");
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, 2000)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<SumV1>(2, 3);
  auto reply = stream.SendAndWaitProgress();
  EXPECT_EQ(reply.sum, 5);
}

// Each batch for itself shouldn't timeout
TEST(RpcInProgress, MultipleProgresses) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                              uint64_t const request_version,
                              auto *req_reader,
                              auto *res_builder) {
    spdlog::trace("Started executing sum callback");
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

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
    SumRes const res{5};
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
    spdlog::trace("Saved SumRes response");
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, 500)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<SumV1>(2, 3);
  auto reply = stream.SendAndWaitProgress();
  EXPECT_EQ(reply.sum, 5);
}

TEST(RpcInProgress, Timeout) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                              uint64_t const request_version,
                              auto *req_reader,
                              auto *res_builder) {
    spdlog::trace("Started executing sum callback");
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

    spdlog::trace("Loaded sum req request");

    // Simulate work
    std::this_thread::sleep_for(100ms);
    memgraph::rpc::SendInProgressMsg(res_builder);
    spdlog::trace("Saved InProgressRes");

    // Simulate done
    std::this_thread::sleep_for(300ms);
    SumRes const res{5};
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
    spdlog::trace("Saved SumRes response");
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, 200)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<SumV1>(2, 3);
  EXPECT_THROW(stream.SendAndWaitProgress(), RpcTimeoutException);
}

TEST(RpcInProgress, NoTimeout) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                              uint64_t const request_version,
                              auto *req_reader,
                              auto *res_builder) {
    spdlog::trace("Started executing sum callback");
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

    spdlog::trace("Loaded sum req request");
    SumRes const res{5};
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
    spdlog::trace("Saved SumRes response");
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, 200)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<SumV1>(2, 3);
  EXPECT_NO_THROW(stream.SendAndWaitProgress());
}

// Regression for the shutdown deadlock where ReplicationClient::Shutdown() could never break an in-flight recovery RPC.
// A replica that keeps streaming InProgressRes keeps the main's SendAndWaitProgress read alive indefinitely (well below
// the per-message timeout). Abort() must interrupt that wait promptly by shutting down the socket, regardless of the
// long configured RPC timeout.
TEST(RpcInProgress, AbortInterruptsInProgressWait) {
  Endpoint const endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  // Handler emulates a replica stuck loading a snapshot: it never sends the final response, only InProgressRes
  // heartbeats, until the client tears the connection down (at which point the write fails and we stop).
  rpc_server.Register<Sum>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                              uint64_t const request_version,
                              auto *req_reader,
                              auto *res_builder) {
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
    try {
      for (int i = 0; i < 600; ++i) {  // bounded so the worker can't run forever even if the client never aborts
        std::this_thread::sleep_for(50ms);
        memgraph::rpc::SendInProgressMsg(res_builder);
      }
    } catch (...) {
      // Client shut the socket down mid-stream; nothing left to do.
    }
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  // Generous per-message timeout: only Abort() can end the wait within the test window, not the timeout.
  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, 60'000)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  std::atomic<bool> threw{false};
  std::thread worker{[&] {
    try {
      auto stream = client.Stream<SumV1>(2, 3);
      stream.SendAndWaitProgress();
    } catch (const GenericRpcFailedException &) {
      threw.store(true);
    } catch (...) {
      // Any other failure leaves threw=false and fails the expectation below.
    }
  }};

  // Let the RPC get in-flight and receive a few InProgressRes heartbeats.
  std::this_thread::sleep_for(300ms);

  auto const before = std::chrono::steady_clock::now();
  client.Abort();
  worker.join();
  auto const elapsed = std::chrono::steady_clock::now() - before;

  EXPECT_TRUE(threw.load());
  // The wait must end because of Abort, not because the 60s timeout elapsed.
  EXPECT_LT(elapsed, 5s);
}

// Regression for the reconnect race: once Abort() has torn the client down during shutdown, no later Stream() (a queued
// recovery task, a heartbeat, or a commit) may revive the connection. The stream attempt must fail fast instead.
TEST(RpcInProgress, AbortPreventsReconnect) {
  Endpoint const endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                              uint64_t const request_version,
                              auto *req_reader,
                              auto *res_builder) {
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
    SumRes const res{5};
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  ClientContext client_context;
  Client client{endpoint, &client_context};

  // A healthy call works before abort.
  EXPECT_NO_THROW(client.Stream<SumV1>(2, 3).SendAndWaitProgress());

  client.Abort();

  // After abort the server is still up, but the client must refuse to open a new stream rather than reconnect.
  EXPECT_THROW(client.Stream<SumV1>(2, 3), GenericRpcFailedException);
}
