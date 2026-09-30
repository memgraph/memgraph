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

#include <atomic>
#include <chrono>
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
// Well under the client budgets below, so a working heartbeat gets several ticks in before the peer would give up.
constexpr auto kHeartbeatInterval = 100ms;

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
    // Three times the client's 500ms budget, reporting progress throughout.
    for (auto i = 0; i < 15; ++i) {
      std::this_thread::sleep_for(100ms);
      heartbeat.RecordProgress();
    }
    heartbeat.Stop();

    SumRes const res{5};
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
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

// The property the whole design rests on: a handler that stalls without recording progress must NOT be kept alive.
// If this test starts passing without the timeout, the heartbeat has become an unconditional keepalive and a wedged
// replica would be indistinguishable from a busy one.
TEST(ProgressHeartbeatTest, StalledHandlerStillTimesOut) {
  Endpoint const endpoint{"localhost", port + 1};

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
    // Heartbeat is running and ticking, but no work is ever recorded, so it must stay silent.
    std::this_thread::sleep_for(2s);
    heartbeat.Stop();

    SumRes const res{5};
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, 500)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<SumV1>(2, 3);
  EXPECT_THROW(stream.SendAndWaitProgress(), RpcTimeoutException);
}

// Progress that stops partway must also stop the heartbeat: the peer's timeout has to fire from the last tick, not be
// deferred forever by earlier work.
TEST(ProgressHeartbeatTest, ProgressStoppingMidCallTimesOut) {
  Endpoint const endpoint{"localhost", port + 2};

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
    for (auto i = 0; i < 5; ++i) {
      std::this_thread::sleep_for(100ms);
      heartbeat.RecordProgress();
    }
    // Work stops here; the remaining wait must not be covered by heartbeats.
    std::this_thread::sleep_for(2s);
    heartbeat.Stop();

    SumRes const res{5};
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, 500)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<SumV1>(2, 3);
  EXPECT_THROW(stream.SendAndWaitProgress(), RpcTimeoutException);
}

// Once the peer is gone the heartbeat latches PeerGone so long-running work can abandon early instead of finishing a
// job whose result can no longer be delivered.
TEST(ProgressHeartbeatTest, PeerGoneLatchesAfterClientDisconnects) {
  Endpoint const endpoint{"localhost", port + 3};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  std::atomic<bool> observed_peer_gone{false};
  std::atomic<bool> handler_finished{false};

  rpc_server.Register<Sum>(
      [&observed_peer_gone, &handler_finished](std::optional<memgraph::rpc::FileReplicationHandler> const & /*unused*/,
                                               uint64_t const request_version,
                                               auto *req_reader,
                                               auto *res_builder) {
        SumReq req;
        memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

        ProgressHeartbeat heartbeat{res_builder, kHeartbeatInterval};
        // Stall with no progress first, so the heartbeat stays silent and the client's budget expires -- that is what
        // makes it shut the socket. Recording progress here instead would (correctly) keep the call alive forever.
        std::this_thread::sleep_for(800ms);

        // Now report progress. The first sends may still land in the kernel buffer, so poll until the write actually
        // fails and PeerGone latches -- this is what a long index build would check to abandon its work.
        for (auto i = 0; i < 100 && !heartbeat.PeerGone(); ++i) {
          std::this_thread::sleep_for(50ms);
          heartbeat.RecordProgress();
        }
        observed_peer_gone.store(heartbeat.PeerGone());
        heartbeat.Stop();
        handler_finished.store(true);

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
    auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, 300)};
    ClientContext client_context;
    Client client{endpoint, &client_context, rpc_timeouts};
    auto stream = client.Stream<SumV1>(2, 3);
    // The handler outlives this budget, and the client shuts the socket down on timeout.
    EXPECT_THROW(stream.SendAndWaitProgress(), RpcTimeoutException);
  }

  // Give the handler room to notice the broken connection on its next tick.
  for (auto i = 0; i < 100 && !handler_finished.load(); ++i) {
    std::this_thread::sleep_for(50ms);
  }
  EXPECT_TRUE(handler_finished.load());
  EXPECT_TRUE(observed_peer_gone.load()) << "heartbeat did not notice the peer going away";
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

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SumReq"sv, 2000)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<SumV1>(2, 3);
  auto reply = stream.SendAndWaitProgress();
  EXPECT_EQ(reply.sum, 5);
}
