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
#include <condition_variable>
#include <mutex>
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

namespace {

// Bound to an ephemeral port so consecutive tests can't collide on a socket still in TIME_WAIT.
Endpoint const kAnyPort{"127.0.0.1", 0};

// Only stops a hang; reaching it means a step never happened, which fails the test's assertion.
constexpr auto kStepTimeout = 30s;

// A timeout no test in this file can cross: the property under test is the protocol, not the clock.
constexpr int kGenerousTimeoutMs = 10'000;

}  // namespace

// A single InProgressRes followed by the final response is consumed transparently.
TEST(RpcInProgress, SingleProgress) {
  ServerContext server_context;
  Server rpc_server{kAnyPort, &server_context, /* workers */ 1};
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
    memgraph::rpc::SendInProgressMsg(res_builder);
    SumRes const res{5};
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, kGenerousTimeoutMs)};
  ClientContext client_context;
  Client client{rpc_server.endpoint(), &client_context, rpc_timeouts};

  auto stream = client.Stream<SumV1>(2, 3);
  auto reply = stream.SendAndWaitProgress();
  EXPECT_EQ(reply.sum, 5);
}

// The per-message timeout restarts on every InProgressRes. The handler is silent for well over the timeout in total,
// so the call can only succeed if each heartbeat restarts the deadline. Each individual gap sits an order of magnitude
// below the timeout, and sleep_for never returns early, so the total is a guaranteed lower bound rather than a race.
TEST(RpcInProgress, MultipleProgresses) {
  static constexpr auto kHeartbeatGap = 50ms;
  static constexpr int kHeartbeats = 20;
  static constexpr int kTimeoutMs = 500;
  static constexpr auto kTotalServerTime = kHeartbeatGap * kHeartbeats;
  static_assert(kTotalServerTime >= 2 * std::chrono::milliseconds{kTimeoutMs});
  static_assert(10 * kHeartbeatGap <= std::chrono::milliseconds{kTimeoutMs});

  ServerContext server_context;
  Server rpc_server{kAnyPort, &server_context, /* workers */ 1};
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
    for (int i = 0; i < kHeartbeats; ++i) {
      std::this_thread::sleep_for(kHeartbeatGap);
      memgraph::rpc::SendInProgressMsg(res_builder);
    }
    SumRes const res{5};
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, kTimeoutMs)};
  ClientContext client_context;
  Client client{rpc_server.endpoint(), &client_context, rpc_timeouts};

  auto const start = std::chrono::steady_clock::now();
  auto stream = client.Stream<SumV1>(2, 3);
  auto reply = stream.SendAndWaitProgress();
  auto const elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_EQ(reply.sum, 5);
  // Documents that the call really outlived the timeout, which is what makes the success meaningful.
  EXPECT_GE(elapsed, kTotalServerTime);
}

// Once the handler goes quiet after an InProgressRes, the client times out. The handler is held on a condition variable
// the test only releases after the throw has been observed, so the client can't receive anything before the deadline
// no matter how the threads are scheduled.
TEST(RpcInProgress, Timeout) {
  std::mutex mutex;
  std::condition_variable cv;
  bool response_released = false;

  ServerContext server_context;
  Server rpc_server{kAnyPort, &server_context, /* workers */ 1};

  rpc_server.Register<Sum>(
      [&](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) {
        SumReq req;
        memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
        memgraph::rpc::SendInProgressMsg(res_builder);
        {
          std::unique_lock lock{mutex};
          cv.wait_for(lock, kStepTimeout, [&] { return response_released; });
        }
        try {
          SumRes const res{5};
          memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
        } catch (...) {
          // The client has already timed out and closed the socket.
        }
      });

  ASSERT_TRUE(rpc_server.Start());

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, 100)};
  ClientContext client_context;
  Client client{rpc_server.endpoint(), &client_context, rpc_timeouts};

  auto stream = client.Stream<SumV1>(2, 3);
  EXPECT_THROW(stream.SendAndWaitProgress(), RpcTimeoutException);

  {
    std::lock_guard const lock{mutex};
    response_released = true;
  }
  cv.notify_all();

  ASSERT_TRUE(rpc_server.Shutdown());
  rpc_server.AwaitShutdown();
}

// A handler that answers immediately never trips a configured timeout.
TEST(RpcInProgress, NoTimeout) {
  ServerContext server_context;
  Server rpc_server{kAnyPort, &server_context, /* workers */ 1};
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

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, kGenerousTimeoutMs)};
  ClientContext client_context;
  Client client{rpc_server.endpoint(), &client_context, rpc_timeouts};

  auto stream = client.Stream<SumV1>(2, 3);
  EXPECT_NO_THROW(stream.SendAndWaitProgress());
}

// Regression for the shutdown deadlock where ReplicationClient::Shutdown() could never break an in-flight recovery RPC.
// A replica that keeps streaming InProgressRes keeps the main's SendAndWaitProgress read alive indefinitely (well below
// the per-message timeout). Abort() must interrupt that wait promptly by shutting down the socket, regardless of the
// long configured RPC timeout.
TEST(RpcInProgress, AbortInterruptsInProgressWait) {
  // Loose enough to clear the delays a loaded runner produces by orders of magnitude, tight enough that an abort taking
  // seconds to land still fails.
  static constexpr auto kPromptly = 5s;

  std::mutex mutex;
  std::condition_variable cv;
  bool heartbeat_sent = false;

  ServerContext server_context;
  Server rpc_server{kAnyPort, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  // Handler emulates a replica stuck loading a snapshot: it never sends the final response, only InProgressRes
  // heartbeats, until the client tears the connection down (at which point the write fails and we stop).
  rpc_server.Register<Sum>(
      [&](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) {
        SumReq req;
        memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
        try {
          for (int i = 0; i < 600; ++i) {  // bounded so the worker can't run forever even if the client never aborts
            memgraph::rpc::SendInProgressMsg(res_builder);
            {
              std::lock_guard const lock{mutex};
              heartbeat_sent = true;
            }
            cv.notify_all();
            std::this_thread::sleep_for(50ms);
          }
        } catch (...) {
          // Client shut the socket down mid-stream; nothing left to do.
        }
      });

  ASSERT_TRUE(rpc_server.Start());

  // Generous per-message timeout: only Abort() can end the wait within the test window, not the timeout.
  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, 60'000)};
  ClientContext client_context;
  Client client{rpc_server.endpoint(), &client_context, rpc_timeouts};

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

  // The RPC is in flight and the client is inside the InProgressRes loop once the handler has sent a heartbeat.
  {
    std::unique_lock lock{mutex};
    cv.wait_for(lock, kStepTimeout, [&] { return heartbeat_sent; });
  }

  auto const before = std::chrono::steady_clock::now();
  client.Abort();
  worker.join();
  auto const elapsed = std::chrono::steady_clock::now() - before;

  EXPECT_TRUE(threw.load());
  // The wait must end because of Abort, not because the 60s timeout elapsed.
  EXPECT_LT(elapsed, kPromptly);
}

// Regression for the reconnect race: once Abort() has torn the client down during shutdown, no later Stream() (a queued
// recovery task, a heartbeat, or a commit) may revive the connection. The stream attempt must fail fast instead.
TEST(RpcInProgress, AbortPreventsReconnect) {
  ServerContext server_context;
  Server rpc_server{kAnyPort, &server_context, /* workers */ 1};
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

  ClientContext client_context;
  Client client{rpc_server.endpoint(), &client_context};

  // A healthy call works before abort.
  EXPECT_NO_THROW(client.Stream<SumV1>(2, 3).SendAndWaitProgress());

  client.Abort();

  // After abort the server is still up, but the client must refuse to open a new stream rather than reconnect.
  EXPECT_THROW(client.Stream<SumV1>(2, 3), GenericRpcFailedException);
}
