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
#include <filesystem>
#include <future>
#include <optional>
#include <thread>

#include <gtest/gtest.h>

#include "auth/auth.hpp"
#include "communication/context.hpp"
#include "dbms/dbms_handler.hpp"
#include "parameters/parameters.hpp"
#include "replication/config.hpp"
#include "replication/replication_client.hpp"
#include "replication_coordination_glue/handler.hpp"
#include "replication_handler/replication_handler.hpp"
#include "rpc/server.hpp"
#include "storage/v2/config.hpp"
#include "system/system.hpp"
#include "utils/uuid.hpp"

using namespace std::chrono_literals;
namespace fs = std::filesystem;
using memgraph::replication::ReplicationClient;

// The replica checker of a BEHIND client waits for the system transaction lock inside SystemRestore, while another
// thread that already holds a system transaction unregisters the replica and so joins that checker. Shutdown must
// return, leaving the client BEHIND to be retried, instead of waiting on a lock its caller holds.
TEST(ReplicationCheckerShutdown, ShutdownReturnsWhileCheckerWaitsForSystemTransaction) {
  static constexpr auto kBound = 10s;

  auto const dir = fs::temp_directory_path() / "MG_test_unit_replication_checker_shutdown";
  fs::remove_all(dir);
  fs::create_directories(dir);

  memgraph::storage::Config conf;
  memgraph::storage::UpdatePaths(conf, dir);
  conf.durability.snapshot_wal_mode =
      memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;

  memgraph::auth::SynchedAuth auth{dir / "auth", memgraph::auth::Auth::Config{}};
  memgraph::system::System system;
  memgraph::parameters::Parameters parameters{dir};
  memgraph::dbms::DbmsHandler dbms{conf};

  std::atomic<int> heartbeats{0};
  memgraph::communication::ServerContext server_context;
  memgraph::rpc::Server server({"127.0.0.1", 0}, &server_context);
  server.Register<memgraph::replication_coordination_glue::FrequentHeartbeatRpc>(
      [&heartbeats](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                    uint64_t const request_version,
                    auto *req_reader,
                    auto *res_builder) {
        heartbeats.fetch_add(1, std::memory_order_acq_rel);
        memgraph::replication_coordination_glue::FrequentHeartbeatHandler(request_version, req_reader, res_builder);
      });
  ASSERT_TRUE(server.Start());

  {
    ReplicationClient client{memgraph::replication::ReplicationClientConfig{
        .name = "REPLICA",
        .mode = memgraph::replication_coordination_glue::ReplicationMode::SYNC,
        .repl_server_endpoint = server.endpoint(),
        .replica_check_frequency = 1s,
    }};

    auto txn = system.TryCreateTransaction();
    ASSERT_TRUE(txn.has_value());

    memgraph::replication::StartReplicaClient(client,
                                              system,
                                              dbms,
                                              memgraph::utils::UUID{}
#ifdef MG_ENTERPRISE
                                              ,
                                              auth
#endif
                                              ,
                                              parameters);

    // The client starts BEHIND, so the first heartbeat enters SystemRestore<true>, which sets RECOVERY before it asks
    // for the system lock. An unbounded wait leaves the checker in RECOVERY while the transaction is held. A bounded
    // wait may already have rolled back to BEHIND, in which case a second heartbeat shows the checker is past its first
    // tick.
    auto const checker_ready = [&] {
      auto const deadline = std::chrono::steady_clock::now() + kBound;
      while (std::chrono::steady_clock::now() < deadline) {
        auto const beats = heartbeats.load(std::memory_order_acquire);
        auto const state = client.state_.WithLock([](auto const &s) { return s; });
        if (beats >= 1 && (state == ReplicationClient::State::RECOVERY || beats >= 2)) {
          return true;
        }
        std::this_thread::sleep_for(1ms);
      }
      return false;
    }();

    auto shutdown = std::async(std::launch::async, [&client] { client.Shutdown(); });
    auto const returned = shutdown.wait_for(kBound) == std::future_status::ready;
    auto const state = client.state_.WithLock([](auto const &s) { return s; });

    // Release the lock so a blocked checker can finish and the future's destructor cannot hang the test.
    txn.reset();
    shutdown.get();

    EXPECT_TRUE(checker_ready) << "checker never reached SystemRestore";
    EXPECT_TRUE(returned) << "Shutdown did not return while a system transaction was held";
    EXPECT_EQ(state, ReplicationClient::State::BEHIND);
  }

  ASSERT_TRUE(server.Shutdown());
  server.AwaitShutdown();
  fs::remove_all(dir);
}
