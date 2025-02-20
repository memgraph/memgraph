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

#include <optional>

#include "rpc/client.hpp"
#include "rpc/server.hpp"
#include "rpc/utils.hpp"  // Needs to be included last so that SLK definitions are seen
#include "storage/v2/indices/indices_utils.hpp"
#include "storage/v2/inmemory/edge_type_index.hpp"
#include "storage/v2/inmemory/edge_type_property_index.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/observer.hpp"

using memgraph::communication::ClientContext;
using memgraph::communication::ServerContext;
using memgraph::io::network::Endpoint;
using memgraph::rpc::Client;
using memgraph::rpc::GenericRpcFailedException;
using memgraph::rpc::Server;
using memgraph::slk::Load;
using memgraph::slk::Save;
using memgraph::storage::Config;
using memgraph::storage::Edge;
using memgraph::storage::EdgeRef;
using memgraph::storage::EdgeTypeId;
using memgraph::storage::Gid;
using memgraph::storage::InMemoryEdgeTypeIndex;
using memgraph::storage::InMemoryEdgeTypePropertyIndex;
using memgraph::storage::InMemoryLabelIndex;
using memgraph::storage::InMemoryLabelPropertyIndex;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::LabelId;
using memgraph::storage::PropertyId;
using memgraph::storage::SnapshotObserverInfo;
using memgraph::storage::Vertex;
using memgraph::storage::durability::ParallelizedSchemaCreationInfo;
using memgraph::storage::replication::SnapshotReq;
using memgraph::storage::replication::SnapshotRes;
using memgraph::storage::replication::SnapshotRpc;
using memgraph::utils::Observer;
using memgraph::utils::SkipList;
using memgraph::utils::UUID;

using namespace std::string_view_literals;
using namespace std::literals::chrono_literals;

class SnapshotRpcProgressTest : public ::testing::Test {
 public:
  std::filesystem::path main_directory{std::filesystem::temp_directory_path() /
                                       "MG_test_unit_snapshot_rpc_progress_main"};
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

  InMemoryStorage storage{main_conf};
};

class MockedSnapshotObserver final : public Observer<void> {
 public:
  MOCK_METHOD(void, Update, (), (override));
};

constexpr int port{8184};

TEST_F(SnapshotRpcProgressTest, TestLabelIndexSingleThreadedNoVertices) {
  InMemoryLabelIndex label_idx;

  auto label = LabelId::FromUint(1);
  auto vertices = SkipList<Vertex>();
  std::optional<ParallelizedSchemaCreationInfo> par_schema_info = std::nullopt;
  auto mocked_observer = std::make_shared<MockedSnapshotObserver>();
  SnapshotObserverInfo snapshot_info{.observer = mocked_observer, .item_batch_size = 3};

  EXPECT_CALL(*mocked_observer, Update()).Times(0);
  ASSERT_TRUE(label_idx.CreateIndex(label, vertices.access(), par_schema_info, snapshot_info));
}

TEST_F(SnapshotRpcProgressTest, TestLabelIndexSingleThreadedVertices) {
  InMemoryLabelIndex label_idx;

  auto label = LabelId::FromUint(1);
  auto vertices = SkipList<Vertex>();
  {
    auto acc = vertices.access();
    for (uint32_t i = 1; i <= 5; i++) {
      auto [_, inserted] = acc.insert(Vertex{Gid::FromUint(i), nullptr});
      ASSERT_TRUE(inserted);
    }
  }

  std::optional<ParallelizedSchemaCreationInfo> par_schema_info = std::nullopt;

  auto mocked_observer = std::make_shared<MockedSnapshotObserver>();
  SnapshotObserverInfo snapshot_info{.observer = mocked_observer, .item_batch_size = 2};
  EXPECT_CALL(*mocked_observer, Update()).Times(2);
  ASSERT_TRUE(label_idx.CreateIndex(label, vertices.access(), par_schema_info, snapshot_info));
}

TEST_F(SnapshotRpcProgressTest, TestLabelIndexMultiThreadedVertices) {
  InMemoryLabelIndex label_idx;

  auto label = LabelId::FromUint(1);
  auto vertices = SkipList<Vertex>();
  {
    auto acc = vertices.access();
    for (uint32_t i = 1; i <= 5; i++) {
      auto [_, inserted] = acc.insert(Vertex{Gid::FromUint(i), nullptr});
      ASSERT_TRUE(inserted);
    }
  }

  auto par_schema_info = ParallelizedSchemaCreationInfo{
      .vertex_recovery_info = std::vector<std::pair<Gid, uint64_t>>{{Gid::FromUint(1), 2}, {Gid::FromUint(3), 3}},
      .thread_count = 2};

  auto mocked_observer = std::make_shared<MockedSnapshotObserver>();
  SnapshotObserverInfo snapshot_info{.observer = mocked_observer, .item_batch_size = 2};
  EXPECT_CALL(*mocked_observer, Update()).Times(2);
  ASSERT_TRUE(label_idx.CreateIndex(label, vertices.access(), par_schema_info, snapshot_info));
}

TEST_F(SnapshotRpcProgressTest, TestLabelPropertyIndexSingleThreadedNoVertices) {
  InMemoryLabelPropertyIndex label_prop_idx;

  auto label = LabelId::FromUint(1);
  auto prop = PropertyId::FromUint(1);
  auto vertices = SkipList<Vertex>();
  std::optional<ParallelizedSchemaCreationInfo> par_schema_info = std::nullopt;
  auto mocked_observer = std::make_shared<MockedSnapshotObserver>();
  SnapshotObserverInfo snapshot_info{.observer = mocked_observer, .item_batch_size = 3};

  EXPECT_CALL(*mocked_observer, Update()).Times(0);
  ASSERT_TRUE(label_prop_idx.CreateIndex(label, prop, vertices.access(), par_schema_info, snapshot_info));
}

TEST_F(SnapshotRpcProgressTest, TestLabelPropertyIndexSingleThreadedVertices) {
  InMemoryLabelPropertyIndex label_prop_idx;

  auto label = LabelId::FromUint(1);
  auto prop = PropertyId::FromUint(1);
  auto vertices = SkipList<Vertex>();
  {
    auto acc = vertices.access();
    for (uint32_t i = 1; i <= 5; i++) {
      auto [_, inserted] = acc.insert(Vertex{Gid::FromUint(i), nullptr});
      ASSERT_TRUE(inserted);
    }
  }

  std::optional<ParallelizedSchemaCreationInfo> par_schema_info = std::nullopt;

  auto mocked_observer = std::make_shared<MockedSnapshotObserver>();
  SnapshotObserverInfo snapshot_info{.observer = mocked_observer, .item_batch_size = 2};
  EXPECT_CALL(*mocked_observer, Update()).Times(2);
  ASSERT_TRUE(label_prop_idx.CreateIndex(label, prop, vertices.access(), par_schema_info, snapshot_info));
}

TEST_F(SnapshotRpcProgressTest, TestLabelPropertyIndexMultiThreadedVertices) {
  InMemoryLabelPropertyIndex label_prop_idx;

  auto label = LabelId::FromUint(1);
  auto prop = PropertyId::FromUint(1);
  auto vertices = SkipList<Vertex>();
  {
    auto acc = vertices.access();
    for (uint32_t i = 1; i <= 5; i++) {
      auto [_, inserted] = acc.insert(Vertex{Gid::FromUint(i), nullptr});
      ASSERT_TRUE(inserted);
    }
  }

  auto par_schema_info = ParallelizedSchemaCreationInfo{
      .vertex_recovery_info = std::vector<std::pair<Gid, uint64_t>>{{Gid::FromUint(1), 2}, {Gid::FromUint(3), 3}},
      .thread_count = 2};

  auto mocked_observer = std::make_shared<MockedSnapshotObserver>();
  SnapshotObserverInfo snapshot_info{.observer = mocked_observer, .item_batch_size = 2};
  EXPECT_CALL(*mocked_observer, Update()).Times(2);
  ASSERT_TRUE(label_prop_idx.CreateIndex(label, prop, vertices.access(), par_schema_info, snapshot_info));
}

TEST_F(SnapshotRpcProgressTest, SnapshotRpcNoTimeout) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<SnapshotRpc>([](auto *req_reader, auto *res_builder) {
    SnapshotReq req;
    Load(&req, req_reader);
    SnapshotRes res{true};
    memgraph::rpc::SendFinalResponse(res, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SnapshotReq"sv, 150)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<SnapshotRpc>(UUID{}, UUID{});
  EXPECT_NO_THROW(stream.AwaitResponseWhileInProgress());
}

TEST_F(SnapshotRpcProgressTest, SnapshotRpcProgress) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<SnapshotRpc>([](auto *req_reader, auto *res_builder) {
    SnapshotReq req;
    Load(&req, req_reader);
    std::this_thread::sleep_for(10ms);
    memgraph::rpc::SendInProgressMsg(res_builder);
    std::this_thread::sleep_for(10ms);
    memgraph::rpc::SendInProgressMsg(res_builder);
    std::this_thread::sleep_for(10ms);
    SnapshotRes res{true};
    memgraph::rpc::SendFinalResponse(res, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SnapshotReq"sv, 150)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<SnapshotRpc>(UUID{}, UUID{});
  EXPECT_NO_THROW(stream.AwaitResponseWhileInProgress());
}

TEST_F(SnapshotRpcProgressTest, SnapshotRpcTimeout) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<SnapshotRpc>([](auto *req_reader, auto *res_builder) {
    SnapshotReq req;
    Load(&req, req_reader);
    std::this_thread::sleep_for(75ms);
    memgraph::rpc::SendInProgressMsg(res_builder);
    std::this_thread::sleep_for(10ms);
    SnapshotRes res{true};
    memgraph::rpc::SendFinalResponse(res, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("SnapshotReq"sv, 25)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<SnapshotRpc>(UUID{}, UUID{});
  EXPECT_THROW(stream.AwaitResponseWhileInProgress(), GenericRpcFailedException);
}

TEST_F(SnapshotRpcProgressTest, TestEdgeTypeIndexSingleThreadedNoVertices) {
  InMemoryEdgeTypeIndex etype_idx;

  auto etype = EdgeTypeId::FromUint(1);
  auto vertices = SkipList<Vertex>();
  auto mocked_observer = std::make_shared<MockedSnapshotObserver>();
  SnapshotObserverInfo snapshot_info{.observer = mocked_observer, .item_batch_size = 3};

  EXPECT_CALL(*mocked_observer, Update()).Times(0);
  ASSERT_TRUE(etype_idx.CreateIndex(etype, vertices.access(), snapshot_info));
}

TEST_F(SnapshotRpcProgressTest, TestEdgeTypeIndexSingleThreadedVerticesEdges) {
  InMemoryEdgeTypeIndex etype_idx;

  auto etype = EdgeTypeId::FromUint(1);
  auto vertices = SkipList<Vertex>();
  {
    auto acc = vertices.access();
    for (uint32_t i = 1; i <= 11; i++) {
      auto vertex = Vertex{Gid::FromUint(i), nullptr};
      EdgeRef edge_ref(Gid::FromUint(1));
      vertex.out_edges.emplace_back(etype, &vertex, edge_ref);
      auto [_, inserted] = acc.insert(std::move(vertex));
      ASSERT_TRUE(inserted);
    }
  }
  auto mocked_observer = std::make_shared<MockedSnapshotObserver>();
  SnapshotObserverInfo snapshot_info{.observer = mocked_observer, .item_batch_size = 3};

  EXPECT_CALL(*mocked_observer, Update()).Times(3);
  ASSERT_TRUE(etype_idx.CreateIndex(etype, vertices.access(), snapshot_info));
}

TEST_F(SnapshotRpcProgressTest, TestEdgeTypePropertyIndexSingleThreadedNoVertices) {
  InMemoryEdgeTypePropertyIndex etype_idx;

  auto etype = EdgeTypeId::FromUint(1);
  auto prop = PropertyId::FromUint(1);
  auto vertices = SkipList<Vertex>();
  auto mocked_observer = std::make_shared<MockedSnapshotObserver>();
  SnapshotObserverInfo snapshot_info{.observer = mocked_observer, .item_batch_size = 3};

  EXPECT_CALL(*mocked_observer, Update()).Times(0);
  ASSERT_TRUE(etype_idx.CreateIndex(etype, prop, vertices.access(), snapshot_info));
}

TEST_F(SnapshotRpcProgressTest, TestEdgeTypePropertyIndexSingleThreadedVerticesEdges) {
  InMemoryEdgeTypePropertyIndex etype_idx;

  auto etype = EdgeTypeId::FromUint(1);
  auto prop = PropertyId::FromUint(1);
  auto vertices = SkipList<Vertex>();
  auto edges = SkipList<Edge>();
  {
    auto acc = vertices.access();
    auto edge_acc = edges.access();
    for (uint32_t i = 1; i <= 7; i++) {
      auto vertex = Vertex{Gid::FromUint(i), nullptr};
      auto [edge, inserted] = edge_acc.insert(Edge{Gid::FromUint(i), nullptr});
      ASSERT_TRUE(inserted);
      auto edge_ref = EdgeRef{&*edge};
      vertex.out_edges.emplace_back(etype, &vertex, edge_ref);
      auto [_, ver_inserted] = acc.insert(std::move(vertex));
      ASSERT_TRUE(ver_inserted);
    }
  }
  auto mocked_observer = std::make_shared<MockedSnapshotObserver>();
  SnapshotObserverInfo snapshot_info{.observer = mocked_observer, .item_batch_size = 3};

  EXPECT_CALL(*mocked_observer, Update()).Times(2);
  ASSERT_TRUE(etype_idx.CreateIndex(etype, prop, vertices.access(), snapshot_info));
}
