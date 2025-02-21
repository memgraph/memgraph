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

#include "dbms/inmemory/replication_handlers.hpp"
#include "storage/v2/inmemory/replication/recovery.cpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"

#include "rpc/client.hpp"
#include "rpc/server.hpp"
#include "rpc/utils.hpp"  // Needs to be included last so that SLK definitions are seen

using memgraph::communication::ClientContext;
using memgraph::communication::ServerContext;
using memgraph::dbms::InMemoryReplicationHandlers;
using memgraph::io::network::Endpoint;
using memgraph::rpc::Client;
using memgraph::rpc::GenericRpcFailedException;
using memgraph::rpc::Server;
using memgraph::slk::Load;
using memgraph::slk::Save;
using memgraph::storage::Config;
using memgraph::storage::Delta;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::ReplicaStream;
using memgraph::storage::Storage;
using memgraph::storage::replication::AppendDeltasReq;
using memgraph::storage::replication::AppendDeltasRes;
using memgraph::storage::replication::AppendDeltasRpc;
using memgraph::storage::replication::Decoder;
using memgraph::utils::UUID;

using namespace std::string_view_literals;
using namespace std::literals::chrono_literals;

constexpr int port{8183};

class ReplicationRpcProgressTest : public ::testing::Test {
 public:
  std::filesystem::path main_directory{std::filesystem::temp_directory_path() /
                                       "MG_test_unit_replication_rpc_progress_main"};

  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

  void Clear() const {
    if (std::filesystem::exists(main_directory)) {
      std::filesystem::remove_all(main_directory);
    }
  }

  Config main_conf = [&] {
    Config config{
        .durability =
            {
                .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            },
        .salient.items = {.properties_on_edges = false},
    };
    UpdatePaths(config, main_directory);
    return config;
  }();

  InMemoryStorage main_storage{main_conf};
};

// Timeout immediately
TEST_F(ReplicationRpcProgressTest, AppendDeltasNoTimeout) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<memgraph::storage::replication::AppendDeltasRpc>([](auto *req_reader, auto *res_builder) {
    AppendDeltasReq req;
    Load(&req, req_reader);
    // Epoch id needs to be read
    Decoder decoder(req_reader);
    auto maybe_epoch_id = decoder.ReadString();

    // Simulate done
    AppendDeltasRes res{true};
    memgraph::rpc::SendFinalResponse(res, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("AppendDeltasReq"sv, 100)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  ReplicaStream stream{&main_storage, client, 1, UUID{}};
  EXPECT_NO_THROW(stream.Finalize());
}

// Timeout immediately
TEST_F(ReplicationRpcProgressTest, AppendDeltasTimeout) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<memgraph::storage::replication::AppendDeltasRpc>([](auto *req_reader, auto *res_builder) {
    AppendDeltasReq req;
    Load(&req, req_reader);
    Decoder decoder(req_reader);
    auto maybe_epoch_id = decoder.ReadString();

    // Simulate done
    std::this_thread::sleep_for(150ms);
    AppendDeltasRes res{true};
    memgraph::rpc::SendFinalResponse(res, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("AppendDeltasReq"sv, 100)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  ReplicaStream stream{&main_storage, client, 1, UUID{}};
  EXPECT_THROW(stream.Finalize(), GenericRpcFailedException);
}

// First send progress, then timeout
TEST_F(ReplicationRpcProgressTest, AppendDeltasProgressTimeout) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<AppendDeltasRpc>([](auto *req_reader, auto *res_builder) {
    AppendDeltasReq req;
    Load(&req, req_reader);
    Decoder decoder(req_reader);
    auto maybe_epoch_id = decoder.ReadString();

    std::this_thread::sleep_for(100ms);
    memgraph::rpc::SendInProgressMsg(res_builder);
    std::this_thread::sleep_for(200ms);
    memgraph::rpc::SendInProgressMsg(res_builder);
    std::this_thread::sleep_for(100ms);
    AppendDeltasRes res{true};
    memgraph::rpc::SendFinalResponse(res, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("AppendDeltasReq"sv, 150)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  ReplicaStream stream{&main_storage, client, 1, UUID{}};

  EXPECT_THROW(stream.Finalize(), GenericRpcFailedException);
}

// First send progress, then timeout
TEST_F(ReplicationRpcProgressTest, CurrentWalNoTimeout) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<memgraph::storage::replication::CurrentWalRpc>([](auto *req_reader, auto *res_builder) {
    memgraph::storage::replication::CurrentWalReq req;
    Load(&req, req_reader);

    memgraph::storage::replication::CurrentWalRes res{true};
    memgraph::rpc::SendFinalResponse(res, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("CurrentWalReq"sv, 150)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = memgraph::storage::InMemoryCurrentWalHandler{UUID{}, &main_storage, client};

  EXPECT_NO_THROW(stream.Finalize());
}

// First send progress, then timeout
TEST_F(ReplicationRpcProgressTest, CurrentWalProgressTimeout) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<memgraph::storage::replication::CurrentWalRpc>([](auto *req_reader, auto *res_builder) {
    memgraph::storage::replication::CurrentWalReq req;
    Load(&req, req_reader);

    std::this_thread::sleep_for(100ms);
    memgraph::rpc::SendInProgressMsg(res_builder);
    std::this_thread::sleep_for(200ms);
    memgraph::rpc::SendInProgressMsg(res_builder);
    std::this_thread::sleep_for(100ms);
    memgraph::storage::replication::CurrentWalRes res{true};
    memgraph::rpc::SendFinalResponse(res, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("CurrentWalReq"sv, 150)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = memgraph::storage::InMemoryCurrentWalHandler{UUID{}, &main_storage, client};

  EXPECT_THROW(stream.Finalize(), GenericRpcFailedException);
}

// First send progress, then timeout
TEST_F(ReplicationRpcProgressTest, WalFilesNoTimeout) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<memgraph::storage::replication::WalFilesRpc>([](auto *req_reader, auto *res_builder) {
    memgraph::storage::replication::WalFilesReq req;
    Load(&req, req_reader);

    memgraph::storage::replication::WalFilesRes res{true};
    memgraph::rpc::SendFinalResponse(res, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("WalFilesReq"sv, 150)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<memgraph::storage::replication::WalFilesRpc>(UUID{}, UUID{}, 1);
  EXPECT_NO_THROW(stream.AwaitResponseWhileInProgress());
}

// First send progress, then timeout
TEST_F(ReplicationRpcProgressTest, WalFilesProgressTimeout) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<memgraph::storage::replication::WalFilesRpc>([](auto *req_reader, auto *res_builder) {
    memgraph::storage::replication::WalFilesReq req;
    Load(&req, req_reader);

    std::this_thread::sleep_for(100ms);
    memgraph::rpc::SendInProgressMsg(res_builder);
    std::this_thread::sleep_for(200ms);
    memgraph::rpc::SendInProgressMsg(res_builder);
    std::this_thread::sleep_for(100ms);
    memgraph::storage::replication::WalFilesRes res{true};
    memgraph::rpc::SendFinalResponse(res, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("WalFilesReq"sv, 150)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<memgraph::storage::replication::WalFilesRpc>(UUID{}, UUID{}, 1);
  EXPECT_THROW(stream.AwaitResponseWhileInProgress(), GenericRpcFailedException);
}

// Timeout immediately
TEST_F(ReplicationRpcProgressTest, TestTTT) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<memgraph::storage::replication::CurrentWalRpc>([](auto *req_reader, auto *res_builder) {
    memgraph::storage::replication::CurrentWalReq req;
    Load(&req, req_reader);
    std::this_thread::sleep_for(150ms);
    memgraph::storage::replication::CurrentWalRes res{true};
    std::this_thread::sleep_for(500ms);
    memgraph::rpc::SendFinalResponse(res, res_builder);
  });

  rpc_server.Register<memgraph::storage::replication::WalFilesRpc>([](auto *req_reader, auto *res_builder) {
    memgraph::storage::replication::WalFilesReq req;
    Load(&req, req_reader);
    std::this_thread::sleep_for(1s);
    memgraph::storage::replication::WalFilesRes res{true};
    memgraph::rpc::SendFinalResponse(res, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("CurrentWalReq"sv, 100)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  {
    auto stream = memgraph::storage::InMemoryCurrentWalHandler{UUID{}, &main_storage, client};
    EXPECT_THROW(stream.Finalize(), GenericRpcFailedException);
  }

  {
    auto wal_files_stream = client.Stream<memgraph::storage::replication::WalFilesRpc>(UUID{}, UUID{}, 1);
    EXPECT_NO_THROW(wal_files_stream.AwaitResponseWhileInProgress());
  }
}
