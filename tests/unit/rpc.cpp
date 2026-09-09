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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <latch>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "rpc_messages.hpp"

#include "rpc/client.hpp"
#include "rpc/client_pool.hpp"
#include "rpc/file_replication_handler.hpp"
#include "rpc/messages.hpp"
#include "rpc/server.hpp"
#include "rpc/utils.hpp"  // Needs to be included last so that SLK definitions are seen
#include "utils/on_scope_exit.hpp"

using namespace memgraph::rpc;
using namespace std::literals::chrono_literals;

TEST(Rpc, Call) {
  memgraph::communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  auto const on_exit = memgraph::utils::OnScopeExit{[&] {
    ASSERT_TRUE(server.Shutdown());
    server.AwaitShutdown();
  }};
  server.Register<Sum>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                          uint64_t const request_version,
                          auto *req_reader,
                          auto *res_builder) {
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
  // Two separate claims. The ordering, that the call ends because it was aborted and not because
  // the response arrived, is checked below without a clock, and it is the one that carries this
  // test: an abort that does nothing, or that only takes effect once the response is out, fails
  // there.
  //
  // Promptness is the second claim and is inherently temporal, so it keeps a bound. No absolute
  // bound survives arbitrary starvation, since a thread given a small enough share of a CPU misses
  // any deadline; this one is loose enough to clear the delays a loaded runner actually produces by
  // more than an order of magnitude, and tight enough that an abort taking seconds to land still
  // fails. If it ever does flake, delete it rather than widen it: widening only moves the
  // threshold, and the ordering check is what the test is for.
  static constexpr auto kPromptly = 10s;
  // Only stops a hang; reaching it means a step never happened, which fails the test below.
  static constexpr auto kStepTimeout = 30s;

  std::mutex mutex;
  std::condition_variable cv;
  // The server has the request, so the client has sent it and is waiting for the response. That is
  // the state the abort has to arrive in, and waiting for the server to say so is exact where
  // waiting for a duration is a guess.
  bool handler_entered = false;
  // Set by the test once it has judged the call, so the handler cannot respond until then.
  bool response_released = false;
  // Set immediately before the response goes out. A call that has already returned while this
  // reads false cannot have been ended by the response.
  std::atomic<bool> response_sent{false};

  memgraph::communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Sum>([&](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                           uint64_t const request_version,
                           auto *req_reader,
                           auto *res_builder) {
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
    auto const sum = std::accumulate(req.nums_.begin(), req.nums_.end(), 0);
    {
      std::lock_guard const lock{mutex};
      handler_entered = true;
    }
    cv.notify_all();
    {
      std::unique_lock lock{mutex};
      // Responding anyway on the timeout is what turns a client that never aborts into a failing
      // assertion below: the call returns a sum instead of throwing.
      cv.wait_for(lock, kStepTimeout, [&] { return response_released; });
    }
    response_sent.store(true);
    SumRes const res({sum});
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });
  ASSERT_TRUE(server.Start());

  memgraph::communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);

  std::thread thread([&]() {
    std::unique_lock lock{mutex};
    if (!cv.wait_for(lock, kStepTimeout, [&] { return handler_entered; })) return;
    lock.unlock();
    spdlog::info("Aborting the connection!");
    client.Abort();
  });

  auto const call_started = std::chrono::steady_clock::now();
  EXPECT_THROW(client.Call<SumV1>(10, 20), RpcFailedException);
  auto const call_took = std::chrono::steady_clock::now() - call_started;

  EXPECT_FALSE(response_sent.load())
      << "the call ended only once the server had responded, so the abort did not end it";
  EXPECT_LT(call_took, kPromptly) << "the abort ended the call, but not while anyone was waiting";

  {
    std::lock_guard const lock{mutex};
    response_released = true;
  }
  cv.notify_all();

  thread.join();
  client.Shutdown();

  ASSERT_TRUE(server.Shutdown());
  server.AwaitShutdown();
}

TEST(Rpc, ClientPool) {
  static constexpr int kConcurrentCalls = 4;
  // Only stops a hang; it measures nothing.
  static constexpr auto kRendezvousTimeout = 30s;

  // The counter and the peak would work as atomics. The condition variable is here because both
  // waits must give up after a while, and neither a wait on an atomic nor one on a latch takes a
  // deadline; without one a broken pool would hang the test instead of failing it.
  std::mutex rendezvous_mutex;
  std::condition_variable rendezvous_cv;
  bool rendezvous_armed = false;
  // Once one handler has timed out the calls are not going to gather, and letting the rest through
  // keeps a failing run to one timeout rather than one each.
  bool rendezvous_gave_up = false;
  int handlers_in_flight = 0;
  int peak_handlers_in_flight = 0;

  // Handing a request over is not the same as reaching the server, so the first handler waits for
  // every caller to hand over and then holds the server open a little longer to see whether a second
  // call arrives. A machine too slow to deliver one reports a client that runs calls one at a time,
  // which is what this check expects, so it can hide a bug but cannot invent one.
  std::latch callers_handed_over{kConcurrentCalls};
  static constexpr auto kControlWindow = 100ms;
  bool control_window_used = false;

  memgraph::communication::ServerContext server_context;
  // The worker count otherwise follows the machine, and a machine with fewer cores than this could
  // not run the calls together however well the pool behaved.
  Server server({"127.0.0.1", 0}, &server_context, kConcurrentCalls);
  server.Register<Sum>([&](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                           uint64_t const request_version,
                           const auto &req_reader,
                           auto *res_builder) {
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
    auto const sum = std::accumulate(req.nums_.begin(), req.nums_.end(), 0);

    {
      std::unique_lock lock(rendezvous_mutex);
      ++handlers_in_flight;
      peak_handlers_in_flight = std::max(peak_handlers_in_flight, handlers_in_flight);
      rendezvous_cv.notify_all();
      if (rendezvous_armed && !rendezvous_gave_up) {
        // Waiting on the peak rather than the live count: it only rises, so it still reads as
        // reached for handlers that wake after the first has left.
        const bool gathered = rendezvous_cv.wait_for(lock, kRendezvousTimeout, [&] {
          return peak_handlers_in_flight >= kConcurrentCalls || rendezvous_gave_up;
        });
        if (!gathered) {
          rendezvous_gave_up = true;
          rendezvous_cv.notify_all();
        }
      } else if (!rendezvous_armed && !control_window_used) {
        control_window_used = true;
        lock.unlock();
        callers_handed_over.wait();
        lock.lock();
        rendezvous_cv.wait_for(lock, kControlWindow, [&] { return handlers_in_flight >= 2; });
      }
      --handlers_in_flight;
    }

    SumRes const res({sum});
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });
  // No wait needed here: the socket is bound and accepting from construction.
  ASSERT_TRUE(server.Start());

  auto peak_and_reset = [&] {
    std::lock_guard const lock(rendezvous_mutex);
    return std::exchange(peak_handlers_in_flight, 0);
  };

  memgraph::communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);

  auto get_sum_client = [&client, &callers_handed_over](int x, int y) {
    callers_handed_over.count_down();
    auto sum = client.Call<SumV1>(x, y);
    EXPECT_EQ(sum.sum, x + y);
  };

  {
    std::vector<std::jthread> threads;
    threads.reserve(kConcurrentCalls);
    for (int i = 0; i < kConcurrentCalls; ++i) {
      threads.emplace_back(get_sum_client, 2 * i, 2 * i + 1);
    }
  }

  // One client is one connection, so its callers queue up.
  EXPECT_EQ(peak_and_reset(), 1) << "a single client let calls into the server at the same time";

  {
    std::lock_guard const lock(rendezvous_mutex);
    rendezvous_armed = true;
  }

  memgraph::communication::ClientContext pool_context;
  ClientPool pool(server.endpoint(), &pool_context);

  auto get_sum = [&pool](int x, int y) {
    auto sum = pool.Call<SumV1>(x, y);
    EXPECT_EQ(sum.sum, x + y);
  };

  {
    std::vector<std::jthread> threads;
    threads.reserve(kConcurrentCalls);
    for (int i = 0; i < kConcurrentCalls; ++i) {
      threads.emplace_back(get_sum, 2 * i, 2 * i + 1);
    }
  }

  EXPECT_EQ(peak_and_reset(), kConcurrentCalls)
      << "pooled calls did not overlap in the server; the pool serialised them";

  ASSERT_TRUE(server.Shutdown());
  server.AwaitShutdown();
}

TEST(Rpc, LargeMessage) {
  memgraph::communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Echo>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                           uint64_t const request_version,
                           auto *req_reader,
                           auto *res_builder) {
    EchoMessage req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
    memgraph::rpc::SendFinalResponse(req, request_version, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  std::string testdata(100'000, 'a');

  memgraph::communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);
  auto echo = client.Call<Echo>(testdata);
  EXPECT_EQ(echo.data, testdata);

  ASSERT_TRUE(server.Shutdown());
  server.AwaitShutdown();
}

TEST(Rpc, JumboMessage) {
  memgraph::communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Echo>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                           uint64_t const request_version,
                           auto *req_reader,
                           auto *res_builder) {
    EchoMessage req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
    memgraph::rpc::SendFinalResponse(req, request_version, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  // NOLINTNEXTLINE (bugprone-string-constructor)
  std::string testdata(10'000'000, 'a');

  memgraph::communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);
  auto echo = client.Call<Echo>(testdata);
  EXPECT_EQ(echo.data, testdata);

  ASSERT_TRUE(server.Shutdown());
  server.AwaitShutdown();
}

TEST(Rpc, Stream) {
  memgraph::communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Echo>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                           uint64_t const request_version,
                           auto *req_reader,
                           auto *res_builder) {
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

  ASSERT_TRUE(server.Shutdown());
  server.AwaitShutdown();
}

TEST(Rpc, StreamLarge) {
  memgraph::communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Echo>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                           uint64_t const request_version,
                           auto *req_reader,
                           auto *res_builder) {
    EchoMessage req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
    std::string payload;
    memgraph::slk::Load(&payload, req_reader);
    EchoMessage const res(req.data + payload);
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });
  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(100ms);

  std::string testdata1(50'000, 'a');
  std::string testdata2(50'000, 'b');

  memgraph::communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);
  auto stream = client.Stream<Echo>(testdata1);
  memgraph::slk::Save(testdata2, stream.GetBuilder());
  auto echo = stream.SendAndWait();
  EXPECT_EQ(echo.data, testdata1 + testdata2);

  ASSERT_TRUE(server.Shutdown());
  server.AwaitShutdown();
}

TEST(Rpc, StreamJumbo) {
  memgraph::communication::ServerContext server_context;
  Server server({"127.0.0.1", 0}, &server_context);
  server.Register<Echo>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                           uint64_t const request_version,
                           auto *req_reader,
                           auto *res_builder) {
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
  std::string testdata1(5'000'000, 'a');
  // NOLINTNEXTLINE (bugprone-string-constructor)
  std::string testdata2(5'000'000, 'b');

  memgraph::communication::ClientContext client_context;
  Client client(server.endpoint(), &client_context);
  auto stream = client.Stream<Echo>(testdata1);
  memgraph::slk::Save(testdata2, stream.GetBuilder());
  auto echo = stream.SendAndWait();
  EXPECT_EQ(echo.data, testdata1 + testdata2);

  ASSERT_TRUE(server.Shutdown());
  server.AwaitShutdown();
}
