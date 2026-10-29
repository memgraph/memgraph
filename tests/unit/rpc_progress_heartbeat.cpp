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

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "gtest/gtest.h"

#include "rpc_messages.hpp"

#include "rpc/client.hpp"
#include "rpc/file_replication_handler.hpp"
#include "rpc/progress_heartbeat.hpp"
#include "rpc/server.hpp"
#include "rpc/utils.hpp"  // Needs to be included last so that SLK definitions are seen

using memgraph::communication::ClientContext;
using memgraph::communication::ServerContext;
using memgraph::io::network::Endpoint;
using memgraph::rpc::Client;
using memgraph::rpc::ProgressHeartbeat;
using memgraph::rpc::RpcTimeoutException;
using memgraph::rpc::Server;

using namespace std::string_view_literals;
using namespace std::literals::chrono_literals;

namespace {

constexpr int port{8195};

// The one wall-clock relation these tests rest on: a tick has to reach the peer inside its budget. The budget is an
// order of magnitude above the interval, so a tick that a loaded runner delays still lands before the peer gives up.
constexpr auto kHeartbeatInterval = 100ms;
constexpr int kClientTimeoutMs{1000};
constexpr auto kClientTimeout = std::chrono::milliseconds{kClientTimeoutMs};

// For a test whose property has nothing to do with the peer's budget: no load makes this fire, and a handler that
// never answers is reported by the ctest timeout on the target instead.
constexpr int kLongerThanAnyDelay{60'000};

// Only stops a hang; reaching it means a step never happened, which fails the test's assertion.
constexpr auto kStepTimeout = 30s;

}  // namespace

// A handler that keeps reporting progress can outlive the client's timeout: each heartbeat restarts the client's read
// wait, so total handler time is unbounded as long as work keeps happening.
TEST(ProgressHeartbeatTest, ProgressKeepsCallAliveBeyondTimeout) {
  Endpoint const endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*unused*/,
                              uint64_t const request_version,
                              auto *req_reader,
                              auto *res_builder) {
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

    ProgressHeartbeat heartbeat{res_builder, kHeartbeatInterval};
    // Records progress until the call has outlived the client's budget twice over. A deadline rather than a count of
    // sleeps: load stretches each sleep, and the property is how long the call lasted, not how many ticks it took.
    auto const deadline = std::chrono::steady_clock::now() + 2 * kClientTimeout;
    while (std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(kHeartbeatInterval);
      heartbeat.RecordProgress();
    }
    heartbeat.Stop();

    SumRes const res{5};
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, kClientTimeoutMs)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<SumV1>(2, 3);
  auto reply = stream.SendAndWaitProgress();
  EXPECT_EQ(reply.sum, 5);
}

// The property the whole design rests on: a handler that stalls without recording progress must NOT be kept alive.
// If this test starts passing without the timeout, the heartbeat has become an unconditional keepalive and a wedged
// replica would be indistinguishable from a busy one. The handler is held on a condition variable the test releases
// only after the throw has been observed, so the final response can't exist before the client gives up no matter how
// the threads are scheduled. The signal is declared before the server so the handler is joined before it is destroyed.
TEST(ProgressHeartbeatTest, StalledHandlerStillTimesOut) {
  Endpoint const endpoint{"localhost", port + 1};

  std::mutex mutex;
  std::condition_variable cv;
  bool response_released = false;

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([&](std::optional<memgraph::rpc::FileReplicationHandler> const & /*unused*/,
                               uint64_t const request_version,
                               auto *req_reader,
                               auto *res_builder) {
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

    ProgressHeartbeat heartbeat{res_builder, kHeartbeatInterval};
    // Heartbeat is running and ticking, but no work is ever recorded, so it must stay silent.
    {
      std::unique_lock lock{mutex};
      cv.wait_for(lock, kStepTimeout, [&] { return response_released; });
    }
    heartbeat.Stop();

    try {
      SumRes const res{5};
      memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
    } catch (std::exception const &) {
      // Expected: the client has already timed out and closed the socket.
    }
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, kClientTimeoutMs)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<SumV1>(2, 3);
  EXPECT_THROW(stream.SendAndWaitProgress(), RpcTimeoutException);

  {
    std::lock_guard const lock{mutex};
    response_released = true;
  }
  cv.notify_all();
}

// Progress that stops partway must also stop the heartbeat: the peer's timeout has to fire from the last tick, not be
// deferred forever by earlier work. As above, the handler is released only after the throw has been observed.
TEST(ProgressHeartbeatTest, ProgressStoppingMidCallTimesOut) {
  Endpoint const endpoint{"localhost", port + 2};

  std::mutex mutex;
  std::condition_variable cv;
  bool response_released = false;

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([&](std::optional<memgraph::rpc::FileReplicationHandler> const & /*unused*/,
                               uint64_t const request_version,
                               auto *req_reader,
                               auto *res_builder) {
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

    ProgressHeartbeat heartbeat{res_builder, kHeartbeatInterval};
    for (auto i = 0; i < 5; ++i) {
      std::this_thread::sleep_for(kHeartbeatInterval);
      heartbeat.RecordProgress();
    }
    // Work stops here; the remaining wait must not be covered by heartbeats.
    {
      std::unique_lock lock{mutex};
      cv.wait_for(lock, kStepTimeout, [&] { return response_released; });
    }
    heartbeat.Stop();

    try {
      SumRes const res{5};
      memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
    } catch (std::exception const &) {
      // Expected: the client has already timed out and closed the socket.
    }
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, kClientTimeoutMs)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<SumV1>(2, 3);
  EXPECT_THROW(stream.SendAndWaitProgress(), RpcTimeoutException);

  {
    std::lock_guard const lock{mutex};
    response_released = true;
  }
  cv.notify_all();
}

// Once the peer is gone the heartbeat latches PeerGone so long-running work can abandon early instead of finishing a
// job whose result can no longer be delivered. The handler records no progress until the test signals that the
// client's scope has closed, so the socket is already shut when the first tick goes out. Both signals are declared
// before the server so the handler is joined before they are destroyed.
TEST(ProgressHeartbeatTest, PeerGoneLatchesAfterClientDisconnects) {
  Endpoint const endpoint{"localhost", port + 3};

  std::mutex mutex;
  std::condition_variable cv;
  bool client_gone = false;
  bool handler_finished = false;

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([&](std::optional<memgraph::rpc::FileReplicationHandler> const & /*unused*/,
                               uint64_t const request_version,
                               auto *req_reader,
                               auto *res_builder) {
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

    ProgressHeartbeat heartbeat{res_builder, kHeartbeatInterval};
    // Stall with no progress until the client has given up and shut the socket. Recording progress before that would
    // (correctly) keep the call alive forever.
    {
      std::unique_lock lock{mutex};
      cv.wait_for(lock, kStepTimeout, [&] { return client_gone; });
    }

    // Now report progress. The first sends may still land in the kernel buffer, so keep going until a write
    // actually fails and PeerGone latches -- this is what a long index build would check to abandon its work.
    // Leaving the loop is the assertion: a heartbeat that never notices the peer leave keeps this handler, and
    // with it the server's shutdown, waiting, and the ctest timeout on the target reports that.
    while (!heartbeat.PeerGone()) {
      std::this_thread::sleep_for(kHeartbeatInterval / 2);
      heartbeat.RecordProgress();
    }
    heartbeat.Stop();
    {
      std::lock_guard const lock{mutex};
      handler_finished = true;
    }
    cv.notify_all();

    try {
      SumRes const res{5};
      memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
    } catch (std::exception const &) {
      // Expected: the peer is gone, so the final response cannot be delivered either.
    }
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  {
    auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, kClientTimeoutMs)};
    ClientContext client_context;
    Client client{endpoint, &client_context, rpc_timeouts};
    auto stream = client.Stream<SumV1>(2, 3);
    // The handler outlives this budget, and the client shuts the socket down on timeout.
    EXPECT_THROW(stream.SendAndWaitProgress(), RpcTimeoutException);
  }

  {
    std::lock_guard const lock{mutex};
    client_gone = true;
  }
  cv.notify_all();

  // PeerGone must latch because the client left, not because the server is shutting down underneath the handler.
  {
    std::unique_lock lock{mutex};
    EXPECT_TRUE(cv.wait_for(lock, kStepTimeout, [&] { return handler_finished; }));
  }
}

// Stop() must be idempotent and safe without any prior progress -- handlers call it on early-return paths where no
// work happened at all.
TEST(ProgressHeartbeatTest, StopIsIdempotentAndSafeWithoutProgress) {
  Endpoint const endpoint{"localhost", port + 4};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*unused*/,
                              uint64_t const request_version,
                              auto *req_reader,
                              auto *res_builder) {
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

    ProgressHeartbeat heartbeat;
    heartbeat.Start(res_builder, kHeartbeatInterval);
    heartbeat.Stop();
    heartbeat.Stop();
    // Recording after the heartbeat stopped must not resurrect it or leak progress into the next activation.
    heartbeat.RecordProgress();
    heartbeat.Start(res_builder, kHeartbeatInterval);
    std::this_thread::sleep_for(2 * kHeartbeatInterval);
    heartbeat.Stop();

    SumRes const res{5};
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, kLongerThanAnyDelay)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<SumV1>(2, 3);
  auto reply = stream.SendAndWaitProgress();
  EXPECT_EQ(reply.sum, 5);
}
