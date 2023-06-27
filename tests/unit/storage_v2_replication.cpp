// Copyright 2023 Memgraph Ltd.
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
#include <memory>
#include <thread>

#include <fmt/format.h>
#include <gmock/gmock-generated-matchers.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <storage/v2/inmemory/storage.hpp>
#include <storage/v2/property_value.hpp>
#include <storage/v2/replication/enums.hpp>
#include "storage/v2/replication/config.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/view.hpp"

using testing::UnorderedElementsAre;

class ReplicationTest : public ::testing::Test {
 protected:
  std::filesystem::path storage_directory{std::filesystem::temp_directory_path() /
                                          "MG_test_unit_storage_v2_replication"};
  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

  memgraph::storage::Config configuration{
      .items = {.properties_on_edges = true},
      .durability = {
          .storage_directory = storage_directory,
          .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
      }};

  const std::string local_host = ("127.0.0.1");
  const std::array<uint16_t, 2> ports{10000, 20000};
  const std::array<std::string, 2> replicas = {"REPLICA1", "REPLICA2"};

 private:
  void Clear() {
    if (!std::filesystem::exists(storage_directory)) return;
    std::filesystem::remove_all(storage_directory);
  }
};

TEST_F(ReplicationTest, BasicSynchronousReplicationTest) {
  std::unique_ptr<memgraph::storage::Storage> main_store =
      std::make_unique<memgraph::storage::InMemoryStorage>(configuration);
  std::unique_ptr<memgraph::storage::Storage> replica_store =
      std::make_unique<memgraph::storage::InMemoryStorage>(configuration);

  auto *main_mem_store = static_cast<memgraph::storage::InMemoryStorage *>(main_store.get());
  auto *replica_mem_store = static_cast<memgraph::storage::InMemoryStorage *>(replica_store.get());

  replica_mem_store->SetReplicaRole(memgraph::io::network::Endpoint{local_host, ports[0]},
                                    memgraph::storage::replication::ReplicationServerConfig{});

  ASSERT_FALSE(main_mem_store
                   ->RegisterReplica("REPLICA", memgraph::io::network::Endpoint{local_host, ports[0]},
                                     memgraph::storage::replication::ReplicationMode::SYNC,
                                     memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                     memgraph::storage::replication::ReplicationClientConfig{})
                   .HasError());

  // vertex create
  // vertex add label
  // vertex set property
  const auto *vertex_label = "vertex_label";
  const auto *vertex_property = "vertex_property";
  const auto *vertex_property_value = "vertex_property_value";
  std::optional<memgraph::storage::Gid> vertex_gid;
  {
    auto acc = main_store->Access();
    auto v = acc->CreateVertex();
    vertex_gid.emplace(v.Gid());
    ASSERT_TRUE(v.AddLabel(main_store->NameToLabel(vertex_label)).HasValue());
    ASSERT_TRUE(v.SetProperty(main_store->NameToProperty(vertex_property),
                              memgraph::storage::PropertyValue(vertex_property_value))
                    .HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }

  {
    auto acc = replica_store->Access();
    const auto v = acc->FindVertex(*vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v);
    const auto labels = v->Labels(memgraph::storage::View::OLD);
    ASSERT_TRUE(labels.HasValue());
    ASSERT_EQ(labels->size(), 1);
    ASSERT_THAT(*labels, UnorderedElementsAre(replica_store->NameToLabel(vertex_label)));
    const auto properties = v->Properties(memgraph::storage::View::OLD);
    ASSERT_TRUE(properties.HasValue());
    ASSERT_EQ(properties->size(), 1);
    ASSERT_THAT(*properties,
                UnorderedElementsAre(std::make_pair(replica_store->NameToProperty(vertex_property),
                                                    memgraph::storage::PropertyValue(vertex_property_value))));

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // vertex remove label
  {
    auto acc = main_store->Access();
    auto v = acc->FindVertex(*vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v);
    ASSERT_TRUE(v->RemoveLabel(main_store->NameToLabel(vertex_label)).HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }

  {
    auto acc = replica_store->Access();
    const auto v = acc->FindVertex(*vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v);
    const auto labels = v->Labels(memgraph::storage::View::OLD);
    ASSERT_TRUE(labels.HasValue());
    ASSERT_EQ(labels->size(), 0);
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // vertex delete
  {
    auto acc = main_store->Access();
    auto v = acc->FindVertex(*vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v);
    ASSERT_TRUE(acc->DeleteVertex(&*v).HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }

  {
    auto acc = replica_store->Access();
    const auto v = acc->FindVertex(*vertex_gid, memgraph::storage::View::OLD);
    ASSERT_FALSE(v);
    vertex_gid.reset();
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // edge create
  // edge set property
  const auto *edge_type = "edge_type";
  const auto *edge_property = "edge_property";
  const auto *edge_property_value = "edge_property_value";
  std::optional<memgraph::storage::Gid> edge_gid;
  {
    auto acc = main_store->Access();
    auto v = acc->CreateVertex();
    vertex_gid.emplace(v.Gid());
    auto edgeRes = acc->CreateEdge(&v, &v, main_store->NameToEdgeType(edge_type));
    ASSERT_TRUE(edgeRes.HasValue());
    auto edge = edgeRes.GetValue();
    ASSERT_TRUE(edge.SetProperty(main_store->NameToProperty(edge_property),
                                 memgraph::storage::PropertyValue(edge_property_value))
                    .HasValue());
    edge_gid.emplace(edge.Gid());
    ASSERT_FALSE(acc->Commit().HasError());
  }

  const auto find_edge = [&](const auto &edges,
                             const memgraph::storage::Gid edge_gid) -> std::optional<memgraph::storage::EdgeAccessor> {
    for (const auto &edge : edges) {
      if (edge.Gid() == edge_gid) {
        return edge;
      }
    }
    return std::nullopt;
  };

  {
    auto acc = replica_store->Access();
    const auto v = acc->FindVertex(*vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v);
    const auto out_edges = v->OutEdges(memgraph::storage::View::OLD);
    ASSERT_TRUE(out_edges.HasValue());
    const auto edge = find_edge(*out_edges, *edge_gid);
    ASSERT_EQ(edge->EdgeType(), replica_store->NameToEdgeType(edge_type));
    const auto properties = edge->Properties(memgraph::storage::View::OLD);
    ASSERT_TRUE(properties.HasValue());
    ASSERT_EQ(properties->size(), 1);
    ASSERT_THAT(*properties,
                UnorderedElementsAre(std::make_pair(replica_store->NameToProperty(edge_property),
                                                    memgraph::storage::PropertyValue(edge_property_value))));
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // delete edge
  {
    auto acc = main_store->Access();
    auto v = acc->FindVertex(*vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v);
    auto out_edges = v->OutEdges(memgraph::storage::View::OLD);
    auto edge = find_edge(*out_edges, *edge_gid);
    ASSERT_TRUE(edge);
    ASSERT_TRUE(acc->DeleteEdge(&*edge).HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }

  {
    auto acc = replica_store->Access();
    const auto v = acc->FindVertex(*vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v);
    const auto out_edges = v->OutEdges(memgraph::storage::View::OLD);
    ASSERT_TRUE(out_edges.HasValue());
    ASSERT_FALSE(find_edge(*out_edges, *edge_gid));
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // label index create
  // label property index create
  // existence constraint create
  // unique constriant create
  const auto *label = "label";
  const auto *property = "property";
  const auto *property_extra = "property_extra";
  {
    ASSERT_FALSE(main_store->CreateIndex(main_store->NameToLabel(label)).HasError());
    ASSERT_FALSE(
        main_store->CreateIndex(main_store->NameToLabel(label), main_store->NameToProperty(property)).HasError());
    ASSERT_FALSE(
        main_store->CreateExistenceConstraint(main_store->NameToLabel(label), main_store->NameToProperty(property), {})
            .HasError());
    ASSERT_FALSE(main_store
                     ->CreateUniqueConstraint(
                         main_store->NameToLabel(label),
                         {main_store->NameToProperty(property), main_store->NameToProperty(property_extra)}, {})
                     .HasError());
  }

  {
    const auto indices = replica_store->ListAllIndices();
    ASSERT_THAT(indices.label, UnorderedElementsAre(replica_store->NameToLabel(label)));
    ASSERT_THAT(indices.label_property, UnorderedElementsAre(std::make_pair(replica_store->NameToLabel(label),
                                                                            replica_store->NameToProperty(property))));

    const auto constraints = replica_store->ListAllConstraints();
    ASSERT_THAT(constraints.existence, UnorderedElementsAre(std::make_pair(replica_store->NameToLabel(label),
                                                                           replica_store->NameToProperty(property))));
    ASSERT_THAT(constraints.unique,
                UnorderedElementsAre(std::make_pair(
                    replica_store->NameToLabel(label),
                    std::set{replica_store->NameToProperty(property), replica_store->NameToProperty(property_extra)})));
  }

  // label index drop
  // label property index drop
  // existence constraint drop
  // unique constriant drop
  {
    ASSERT_FALSE(main_store->DropIndex(main_store->NameToLabel(label)).HasError());
    ASSERT_FALSE(
        main_store->DropIndex(main_store->NameToLabel(label), main_store->NameToProperty(property)).HasError());
    ASSERT_FALSE(
        main_store->DropExistenceConstraint(main_store->NameToLabel(label), main_store->NameToProperty(property), {})
            .HasError());
    ASSERT_EQ(main_store
                  ->DropUniqueConstraint(
                      main_store->NameToLabel(label),
                      {main_store->NameToProperty(property), main_store->NameToProperty(property_extra)}, {})
                  .GetValue(),
              memgraph::storage::UniqueConstraints::DeletionStatus::SUCCESS);
  }

  {
    const auto indices = replica_store->ListAllIndices();
    ASSERT_EQ(indices.label.size(), 0);
    ASSERT_EQ(indices.label_property.size(), 0);

    const auto constraints = replica_store->ListAllConstraints();
    ASSERT_EQ(constraints.existence.size(), 0);
    ASSERT_EQ(constraints.unique.size(), 0);
  }
}

TEST_F(ReplicationTest, MultipleSynchronousReplicationTest) {
  std::unique_ptr<memgraph::storage::Storage> main_store{new memgraph::storage::InMemoryStorage(
      {.durability = {
           .storage_directory = storage_directory,
           .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
       }})};
  auto *main_mem_store = static_cast<memgraph::storage::InMemoryStorage *>(main_store.get());

  std::unique_ptr<memgraph::storage::Storage> replica_store1{new memgraph::storage::InMemoryStorage(
      {.durability = {
           .storage_directory = storage_directory,
           .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
       }})};
  auto *replica_mem_store1 = static_cast<memgraph::storage::InMemoryStorage *>(replica_store1.get());

  replica_mem_store1->SetReplicaRole(memgraph::io::network::Endpoint{local_host, ports[0]},
                                     memgraph::storage::replication::ReplicationServerConfig{});

  std::unique_ptr<memgraph::storage::Storage> replica_store2{new memgraph::storage::InMemoryStorage(
      {.durability = {
           .storage_directory = storage_directory,
           .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
       }})};
  auto *replica_mem_store2 = static_cast<memgraph::storage::InMemoryStorage *>(replica_store2.get());
  replica_mem_store2->SetReplicaRole(memgraph::io::network::Endpoint{local_host, ports[1]},
                                     memgraph::storage::replication::ReplicationServerConfig{});

  ASSERT_FALSE(main_mem_store
                   ->RegisterReplica(replicas[0], memgraph::io::network::Endpoint{local_host, ports[0]},
                                     memgraph::storage::replication::ReplicationMode::SYNC,
                                     memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                     memgraph::storage::replication::ReplicationClientConfig{})
                   .HasError());
  ASSERT_FALSE(main_mem_store
                   ->RegisterReplica(replicas[1], memgraph::io::network::Endpoint{local_host, ports[1]},
                                     memgraph::storage::replication::ReplicationMode::SYNC,
                                     memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                     memgraph::storage::replication::ReplicationClientConfig{})
                   .HasError());

  const auto *vertex_label = "label";
  const auto *vertex_property = "property";
  const auto *vertex_property_value = "property_value";
  std::optional<memgraph::storage::Gid> vertex_gid;
  {
    auto acc = main_store->Access();
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.AddLabel(main_store->NameToLabel(vertex_label)).HasValue());
    ASSERT_TRUE(v.SetProperty(main_store->NameToProperty(vertex_property),
                              memgraph::storage::PropertyValue(vertex_property_value))
                    .HasValue());
    vertex_gid.emplace(v.Gid());
    ASSERT_FALSE(acc->Commit().HasError());
  }

  const auto check_replica = [&](memgraph::storage::Storage *replica_store) {
    auto acc = replica_store->Access();
    const auto v = acc->FindVertex(*vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v);
    const auto labels = v->Labels(memgraph::storage::View::OLD);
    ASSERT_TRUE(labels.HasValue());
    ASSERT_THAT(*labels, UnorderedElementsAre(replica_store->NameToLabel(vertex_label)));
    ASSERT_FALSE(acc->Commit().HasError());
  };

  check_replica(replica_store1.get());
  check_replica(replica_store2.get());

  main_mem_store->UnregisterReplica(replicas[1]);
  {
    auto acc = main_store->Access();
    auto v = acc->CreateVertex();
    vertex_gid.emplace(v.Gid());
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // REPLICA1 should contain the new vertex
  {
    auto acc = replica_store1->Access();
    const auto v = acc->FindVertex(*vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v);
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // REPLICA2 should not contain the new vertex
  {
    auto acc = replica_store2->Access();
    const auto v = acc->FindVertex(*vertex_gid, memgraph::storage::View::OLD);
    ASSERT_FALSE(v);
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

TEST_F(ReplicationTest, RecoveryProcess) {
  std::vector<memgraph::storage::Gid> vertex_gids;
  // Force the creation of snapshot
  {
    std::unique_ptr<memgraph::storage::Storage> main_store{new memgraph::storage::InMemoryStorage(
        {.durability = {
             .storage_directory = storage_directory,
             .recover_on_startup = true,
             .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
             .snapshot_on_exit = true,
         }})};
    {
      auto acc = main_store->Access();
      // Create the vertex before registering a replica
      auto v = acc->CreateVertex();
      vertex_gids.emplace_back(v.Gid());
      ASSERT_FALSE(acc->Commit().HasError());
    }
  }

  {
    // Create second WAL
    std::unique_ptr<memgraph::storage::Storage> main_store{new memgraph::storage::InMemoryStorage(
        {.durability = {.storage_directory = storage_directory,
                        .recover_on_startup = true,
                        .snapshot_wal_mode =
                            memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL}})};
    // Create vertices in 2 different transactions
    {
      auto acc = main_store->Access();
      auto v = acc->CreateVertex();
      vertex_gids.emplace_back(v.Gid());
      ASSERT_FALSE(acc->Commit().HasError());
    }
    {
      auto acc = main_store->Access();
      auto v = acc->CreateVertex();
      vertex_gids.emplace_back(v.Gid());
      ASSERT_FALSE(acc->Commit().HasError());
    }
  }

  std::unique_ptr<memgraph::storage::Storage> main_store{new memgraph::storage::InMemoryStorage(
      {.durability = {
           .storage_directory = storage_directory,
           .recover_on_startup = true,
           .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
       }})};
  auto *main_mem_store = static_cast<memgraph::storage::InMemoryStorage *>(main_store.get());

  static constexpr const auto *property_name = "property_name";
  static constexpr const auto property_value = 1;
  {
    // Force the creation of current WAL file
    auto acc = main_store->Access();
    for (const auto &vertex_gid : vertex_gids) {
      auto v = acc->FindVertex(vertex_gid, memgraph::storage::View::OLD);
      ASSERT_TRUE(v);
      ASSERT_TRUE(
          v->SetProperty(main_store->NameToProperty(property_name), memgraph::storage::PropertyValue(property_value))
              .HasValue());
    }
    ASSERT_FALSE(acc->Commit().HasError());
  }

  std::filesystem::path replica_storage_directory{std::filesystem::temp_directory_path() /
                                                  "MG_test_unit_storage_v2_replication_replica"};
  memgraph::utils::OnScopeExit replica_directory_cleaner(
      [&]() { std::filesystem::remove_all(replica_storage_directory); });

  static constexpr const auto *vertex_label = "vertex_label";
  {
    std::unique_ptr<memgraph::storage::Storage> replica_store{new memgraph::storage::InMemoryStorage(
        {.durability = {.storage_directory = replica_storage_directory,
                        .snapshot_wal_mode =
                            memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL}})};
    auto *replica_mem_store = static_cast<memgraph::storage::InMemoryStorage *>(replica_store.get());

    replica_mem_store->SetReplicaRole(memgraph::io::network::Endpoint{local_host, ports[0]},
                                      memgraph::storage::replication::ReplicationServerConfig{});

    ASSERT_FALSE(main_mem_store
                     ->RegisterReplica(replicas[0], memgraph::io::network::Endpoint{local_host, ports[0]},
                                       memgraph::storage::replication::ReplicationMode::SYNC,
                                       memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                       memgraph::storage::replication::ReplicationClientConfig{})
                     .HasError());

    ASSERT_EQ(main_mem_store->GetReplicaState(replicas[0]), memgraph::storage::replication::ReplicaState::RECOVERY);

    while (main_mem_store->GetReplicaState(replicas[0]) != memgraph::storage::replication::ReplicaState::READY) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    {
      auto acc = main_store->Access();
      for (const auto &vertex_gid : vertex_gids) {
        auto v = acc->FindVertex(vertex_gid, memgraph::storage::View::OLD);
        ASSERT_TRUE(v);
        ASSERT_TRUE(v->AddLabel(main_store->NameToLabel(vertex_label)).HasValue());
      }
      ASSERT_FALSE(acc->Commit().HasError());
    }
    {
      auto acc = replica_store->Access();
      for (const auto &vertex_gid : vertex_gids) {
        auto v = acc->FindVertex(vertex_gid, memgraph::storage::View::OLD);
        ASSERT_TRUE(v);
        const auto labels = v->Labels(memgraph::storage::View::OLD);
        ASSERT_TRUE(labels.HasValue());
        ASSERT_THAT(*labels, UnorderedElementsAre(replica_store->NameToLabel(vertex_label)));
        const auto properties = v->Properties(memgraph::storage::View::OLD);
        ASSERT_TRUE(properties.HasValue());
        ASSERT_THAT(*properties,
                    UnorderedElementsAre(std::make_pair(replica_store->NameToProperty(property_name),
                                                        memgraph::storage::PropertyValue(property_value))));
      }
      ASSERT_FALSE(acc->Commit().HasError());
    }
  }
  {
    std::unique_ptr<memgraph::storage::Storage> replica_store{new memgraph::storage::InMemoryStorage(
        {.durability = {.storage_directory = replica_storage_directory,
                        .recover_on_startup = true,
                        .snapshot_wal_mode =
                            memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL}})};
    {
      auto acc = replica_store->Access();
      for (const auto &vertex_gid : vertex_gids) {
        auto v = acc->FindVertex(vertex_gid, memgraph::storage::View::OLD);
        ASSERT_TRUE(v);
        const auto labels = v->Labels(memgraph::storage::View::OLD);
        ASSERT_TRUE(labels.HasValue());
        ASSERT_THAT(*labels, UnorderedElementsAre(replica_store->NameToLabel(vertex_label)));
        const auto properties = v->Properties(memgraph::storage::View::OLD);
        ASSERT_TRUE(properties.HasValue());
        ASSERT_THAT(*properties,
                    UnorderedElementsAre(std::make_pair(replica_store->NameToProperty(property_name),
                                                        memgraph::storage::PropertyValue(property_value))));
      }
      ASSERT_FALSE(acc->Commit().HasError());
    }
  }
}

TEST_F(ReplicationTest, BasicAsynchronousReplicationTest) {
  std::unique_ptr<memgraph::storage::Storage> main_store{new memgraph::storage::InMemoryStorage(configuration)};
  auto *main_mem_store = static_cast<memgraph::storage::InMemoryStorage *>(main_store.get());

  std::unique_ptr<memgraph::storage::Storage> replica_store_async{
      new memgraph::storage::InMemoryStorage(configuration)};

  auto *replica_mem_store_async = static_cast<memgraph::storage::InMemoryStorage *>(replica_store_async.get());

  replica_mem_store_async->SetReplicaRole(memgraph::io::network::Endpoint{local_host, ports[1]},
                                          memgraph::storage::replication::ReplicationServerConfig{});

  ASSERT_FALSE(main_mem_store
                   ->RegisterReplica("REPLICA_ASYNC", memgraph::io::network::Endpoint{local_host, ports[1]},
                                     memgraph::storage::replication::ReplicationMode::ASYNC,
                                     memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                     memgraph::storage::replication::ReplicationClientConfig{})
                   .HasError());

  static constexpr size_t vertices_create_num = 10;
  std::vector<memgraph::storage::Gid> created_vertices;
  for (size_t i = 0; i < vertices_create_num; ++i) {
    auto acc = main_store->Access();
    auto v = acc->CreateVertex();
    created_vertices.push_back(v.Gid());
    ASSERT_FALSE(acc->Commit().HasError());

    if (i == 0) {
      ASSERT_EQ(main_mem_store->GetReplicaState("REPLICA_ASYNC"),
                memgraph::storage::replication::ReplicaState::REPLICATING);
    } else {
      ASSERT_EQ(main_mem_store->GetReplicaState("REPLICA_ASYNC"),
                memgraph::storage::replication::ReplicaState::RECOVERY);
    }
  }

  while (main_mem_store->GetReplicaState("REPLICA_ASYNC") != memgraph::storage::replication::ReplicaState::READY) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(std::all_of(created_vertices.begin(), created_vertices.end(), [&](const auto vertex_gid) {
    auto acc = replica_store_async->Access();
    auto v = acc->FindVertex(vertex_gid, memgraph::storage::View::OLD);
    const bool exists = v.has_value();
    EXPECT_FALSE(acc->Commit().HasError());
    return exists;
  }));
}

TEST_F(ReplicationTest, EpochTest) {
  std::unique_ptr<memgraph::storage::Storage> main_store{new memgraph::storage::InMemoryStorage(configuration)};
  std::unique_ptr<memgraph::storage::Storage> replica_store1{new memgraph::storage::InMemoryStorage(configuration)};
  auto *main_mem_store = static_cast<memgraph::storage::InMemoryStorage *>(main_store.get());
  auto *replica_mem_store1 = static_cast<memgraph::storage::InMemoryStorage *>(replica_store1.get());

  replica_mem_store1->SetReplicaRole(memgraph::io::network::Endpoint{local_host, ports[0]},
                                     memgraph::storage::replication::ReplicationServerConfig{});

  std::unique_ptr<memgraph::storage::Storage> replica_store2{new memgraph::storage::InMemoryStorage(configuration)};
  auto *replica_mem_store2 = static_cast<memgraph::storage::InMemoryStorage *>(replica_store2.get());

  replica_mem_store2->SetReplicaRole(memgraph::io::network::Endpoint{local_host, 10001},
                                     memgraph::storage::replication::ReplicationServerConfig{});

  ASSERT_FALSE(main_mem_store
                   ->RegisterReplica(replicas[0], memgraph::io::network::Endpoint{local_host, ports[0]},
                                     memgraph::storage::replication::ReplicationMode::SYNC,
                                     memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                     memgraph::storage::replication::ReplicationClientConfig{})
                   .HasError());

  ASSERT_FALSE(main_mem_store
                   ->RegisterReplica(replicas[1], memgraph::io::network::Endpoint{local_host, 10001},
                                     memgraph::storage::replication::ReplicationMode::SYNC,
                                     memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                     memgraph::storage::replication::ReplicationClientConfig{})
                   .HasError());

  std::optional<memgraph::storage::Gid> vertex_gid;
  {
    auto acc = main_store->Access();
    const auto v = acc->CreateVertex();
    vertex_gid.emplace(v.Gid());
    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = replica_store1->Access();
    const auto v = acc->FindVertex(*vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v);
    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = replica_store2->Access();
    const auto v = acc->FindVertex(*vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v);
    ASSERT_FALSE(acc->Commit().HasError());
  }

  main_mem_store->UnregisterReplica(replicas[0]);
  main_mem_store->UnregisterReplica(replicas[1]);

  replica_mem_store1->SetMainReplicationRole();
  ASSERT_FALSE(replica_mem_store1
                   ->RegisterReplica(replicas[1], memgraph::io::network::Endpoint{local_host, 10001},
                                     memgraph::storage::replication::ReplicationMode::SYNC,
                                     memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                     memgraph::storage::replication::ReplicationClientConfig{})

                   .HasError());

  {
    auto acc = main_store->Access();
    acc->CreateVertex();
    ASSERT_FALSE(acc->Commit().HasError());
  }
  {
    auto acc = replica_store1->Access();
    auto v = acc->CreateVertex();
    vertex_gid.emplace(v.Gid());
    ASSERT_FALSE(acc->Commit().HasError());
  }
  // Replica1 should forward it's vertex to Replica2
  {
    auto acc = replica_store2->Access();
    const auto v = acc->FindVertex(*vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v);
    ASSERT_FALSE(acc->Commit().HasError());
  }

  replica_mem_store1->SetReplicaRole(memgraph::io::network::Endpoint{local_host, ports[0]},
                                     memgraph::storage::replication::ReplicationServerConfig{});
  ASSERT_TRUE(main_mem_store
                  ->RegisterReplica(replicas[0], memgraph::io::network::Endpoint{local_host, ports[0]},
                                    memgraph::storage::replication::ReplicationMode::SYNC,
                                    memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                    memgraph::storage::replication::ReplicationClientConfig{})

                  .HasError());

  {
    auto acc = main_store->Access();
    const auto v = acc->CreateVertex();
    vertex_gid.emplace(v.Gid());
    ASSERT_FALSE(acc->Commit().HasError());
  }
  // Replica1 is not compatible with the main so it shouldn't contain
  // it's newest vertex
  {
    auto acc = replica_store1->Access();
    const auto v = acc->FindVertex(*vertex_gid, memgraph::storage::View::OLD);
    ASSERT_FALSE(v);
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

TEST_F(ReplicationTest, ReplicationInformation) {
  std::unique_ptr<memgraph::storage::Storage> main_store{new memgraph::storage::InMemoryStorage(configuration)};
  std::unique_ptr<memgraph::storage::Storage> replica_store1{new memgraph::storage::InMemoryStorage(configuration)};
  auto *main_mem_store = static_cast<memgraph::storage::InMemoryStorage *>(main_store.get());
  auto *replica_mem_store1 = static_cast<memgraph::storage::InMemoryStorage *>(replica_store1.get());

  const memgraph::io::network::Endpoint replica1_endpoint{local_host, 10001};
  replica_mem_store1->SetReplicaRole(replica1_endpoint, memgraph::storage::replication::ReplicationServerConfig{});

  const memgraph::io::network::Endpoint replica2_endpoint{local_host, 10002};
  std::unique_ptr<memgraph::storage::Storage> replica_store2{new memgraph::storage::InMemoryStorage(configuration)};
  auto *replica_mem_store2 = static_cast<memgraph::storage::InMemoryStorage *>(replica_store2.get());
  replica_mem_store2->SetReplicaRole(replica2_endpoint, memgraph::storage::replication::ReplicationServerConfig{});

  const std::string replica1_name{replicas[0]};
  ASSERT_FALSE(main_mem_store
                   ->RegisterReplica(replica1_name, replica1_endpoint,
                                     memgraph::storage::replication::ReplicationMode::SYNC,
                                     memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                     memgraph::storage::replication::ReplicationClientConfig{})

                   .HasError());

  const std::string replica2_name{replicas[1]};
  ASSERT_FALSE(main_mem_store
                   ->RegisterReplica(replica2_name, replica2_endpoint,
                                     memgraph::storage::replication::ReplicationMode::ASYNC,
                                     memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                     memgraph::storage::replication::ReplicationClientConfig{})

                   .HasError());

  ASSERT_EQ(main_mem_store->GetReplicationRole(), memgraph::storage::replication::ReplicationRole::MAIN);
  ASSERT_EQ(replica_mem_store1->GetReplicationRole(), memgraph::storage::replication::ReplicationRole::REPLICA);
  ASSERT_EQ(replica_mem_store2->GetReplicationRole(), memgraph::storage::replication::ReplicationRole::REPLICA);

  const auto replicas_info = main_mem_store->ReplicasInfo();
  ASSERT_EQ(replicas_info.size(), 2);

  const auto &first_info = replicas_info[0];
  ASSERT_EQ(first_info.name, replica1_name);
  ASSERT_EQ(first_info.mode, memgraph::storage::replication::ReplicationMode::SYNC);
  ASSERT_EQ(first_info.endpoint, replica1_endpoint);
  ASSERT_EQ(first_info.state, memgraph::storage::replication::ReplicaState::READY);

  const auto &second_info = replicas_info[1];
  ASSERT_EQ(second_info.name, replica2_name);
  ASSERT_EQ(second_info.mode, memgraph::storage::replication::ReplicationMode::ASYNC);
  ASSERT_EQ(second_info.endpoint, replica2_endpoint);
  ASSERT_EQ(second_info.state, memgraph::storage::replication::ReplicaState::READY);
}

TEST_F(ReplicationTest, ReplicationReplicaWithExistingName) {
  std::unique_ptr<memgraph::storage::Storage> main_store{new memgraph::storage::InMemoryStorage(configuration)};
  std::unique_ptr<memgraph::storage::Storage> replica_store1{new memgraph::storage::InMemoryStorage(configuration)};
  auto *main_mem_store = static_cast<memgraph::storage::InMemoryStorage *>(main_store.get());
  auto *replica_mem_store1 = static_cast<memgraph::storage::InMemoryStorage *>(replica_store1.get());

  const memgraph::io::network::Endpoint replica1_endpoint{local_host, 10001};
  replica_mem_store1->SetReplicaRole(replica1_endpoint, memgraph::storage::replication::ReplicationServerConfig{});

  const memgraph::io::network::Endpoint replica2_endpoint{local_host, 10002};
  std::unique_ptr<memgraph::storage::Storage> replica_store2{new memgraph::storage::InMemoryStorage(configuration)};
  auto *replica_mem_store2 = static_cast<memgraph::storage::InMemoryStorage *>(replica_store2.get());
  replica_mem_store2->SetReplicaRole(replica2_endpoint, memgraph::storage::replication::ReplicationServerConfig{});

  const std::string replica1_name{replicas[0]};
  ASSERT_FALSE(main_mem_store
                   ->RegisterReplica(replica1_name, replica1_endpoint,
                                     memgraph::storage::replication::ReplicationMode::SYNC,
                                     memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                     memgraph::storage::replication::ReplicationClientConfig{})
                   .HasError());

  const std::string replica2_name{replicas[0]};
  ASSERT_TRUE(main_mem_store
                  ->RegisterReplica(replica2_name, replica2_endpoint,
                                    memgraph::storage::replication::ReplicationMode::ASYNC,
                                    memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                    memgraph::storage::replication::ReplicationClientConfig{})
                  .GetError() == memgraph::storage::InMemoryStorage::RegisterReplicaError::NAME_EXISTS);
}

TEST_F(ReplicationTest, ReplicationReplicaWithExistingEndPoint) {
  std::unique_ptr<memgraph::storage::Storage> main_store{new memgraph::storage::InMemoryStorage(configuration)};
  std::unique_ptr<memgraph::storage::Storage> replica_store1{new memgraph::storage::InMemoryStorage(configuration)};
  auto *main_mem_store = static_cast<memgraph::storage::InMemoryStorage *>(main_store.get());
  auto *replica_mem_store1 = static_cast<memgraph::storage::InMemoryStorage *>(replica_store1.get());

  const memgraph::io::network::Endpoint replica1_endpoint{local_host, 10001};
  replica_mem_store1->SetReplicaRole(replica1_endpoint, memgraph::storage::replication::ReplicationServerConfig{});

  std::unique_ptr<memgraph::storage::Storage> replica_store2{new memgraph::storage::InMemoryStorage(configuration)};
  auto *replica_mem_store2 = static_cast<memgraph::storage::InMemoryStorage *>(replica_store2.get());

  const memgraph::io::network::Endpoint replica2_endpoint{local_host, 10001};
  replica_mem_store2->SetReplicaRole(replica2_endpoint, memgraph::storage::replication::ReplicationServerConfig{});

  const std::string replica1_name{replicas[0]};
  ASSERT_FALSE(main_mem_store
                   ->RegisterReplica(replica1_name, replica1_endpoint,
                                     memgraph::storage::replication::ReplicationMode::SYNC,
                                     memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                     memgraph::storage::replication::ReplicationClientConfig{})
                   .HasError());

  const std::string replica2_name{replicas[1]};
  ASSERT_TRUE(main_mem_store
                  ->RegisterReplica(replica2_name, replica2_endpoint,
                                    memgraph::storage::replication::ReplicationMode::ASYNC,
                                    memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                    memgraph::storage::replication::ReplicationClientConfig{})
                  .GetError() == memgraph::storage::InMemoryStorage::RegisterReplicaError::END_POINT_EXISTS);
}

TEST_F(ReplicationTest, RestoringReplicationAtStartupAftgerDroppingReplica) {
  auto main_config = configuration;
  main_config.durability.restore_replication_state_on_startup = true;
  std::unique_ptr<memgraph::storage::Storage> main_store{new memgraph::storage::InMemoryStorage(main_config)};
  std::unique_ptr<memgraph::storage::Storage> replica_store1{new memgraph::storage::InMemoryStorage(configuration)};
  auto *main_mem_store = static_cast<memgraph::storage::InMemoryStorage *>(main_store.get());
  auto *replica_mem_store1 = static_cast<memgraph::storage::InMemoryStorage *>(replica_store1.get());

  replica_mem_store1->SetReplicaRole(memgraph::io::network::Endpoint{local_host, ports[0]},
                                     memgraph::storage::replication::ReplicationServerConfig{});

  std::unique_ptr<memgraph::storage::Storage> replica_store2{new memgraph::storage::InMemoryStorage(configuration)};
  auto *replica_mem_store2 = static_cast<memgraph::storage::InMemoryStorage *>(replica_store2.get());
  replica_mem_store2->SetReplicaRole(memgraph::io::network::Endpoint{local_host, ports[1]},
                                     memgraph::storage::replication::ReplicationServerConfig{});

  auto res = main_mem_store->RegisterReplica(replicas[0], memgraph::io::network::Endpoint{local_host, ports[0]},
                                             memgraph::storage::replication::ReplicationMode::SYNC,
                                             memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                             memgraph::storage::replication::ReplicationClientConfig{});
  ASSERT_FALSE(res.HasError());
  res = main_mem_store->RegisterReplica(replicas[1], memgraph::io::network::Endpoint{local_host, ports[1]},
                                        memgraph::storage::replication::ReplicationMode::SYNC,
                                        memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                        memgraph::storage::replication::ReplicationClientConfig{});
  ASSERT_FALSE(res.HasError());

  auto replica_infos = main_mem_store->ReplicasInfo();

  ASSERT_EQ(replica_infos.size(), 2);
  ASSERT_EQ(replica_infos[0].name, replicas[0]);
  ASSERT_EQ(replica_infos[0].endpoint.address, local_host);
  ASSERT_EQ(replica_infos[0].endpoint.port, ports[0]);
  ASSERT_EQ(replica_infos[1].name, replicas[1]);
  ASSERT_EQ(replica_infos[1].endpoint.address, local_host);
  ASSERT_EQ(replica_infos[1].endpoint.port, ports[1]);

  main_store.reset();

  std::unique_ptr<memgraph::storage::Storage> other_main_store{new memgraph::storage::InMemoryStorage(main_config)};
  auto *other_main_mem_store = static_cast<memgraph::storage::InMemoryStorage *>(other_main_store.get());

  replica_infos = other_main_mem_store->ReplicasInfo();
  ASSERT_EQ(replica_infos.size(), 2);
  ASSERT_EQ(replica_infos[0].name, replicas[0]);
  ASSERT_EQ(replica_infos[0].endpoint.address, local_host);
  ASSERT_EQ(replica_infos[0].endpoint.port, ports[0]);
  ASSERT_EQ(replica_infos[1].name, replicas[1]);
  ASSERT_EQ(replica_infos[1].endpoint.address, local_host);
  ASSERT_EQ(replica_infos[1].endpoint.port, ports[1]);
}

TEST_F(ReplicationTest, RestoringReplicationAtStartup) {
  auto main_config = configuration;
  main_config.durability.restore_replication_state_on_startup = true;

  std::unique_ptr<memgraph::storage::Storage> main_store{new memgraph::storage::InMemoryStorage(main_config)};
  std::unique_ptr<memgraph::storage::Storage> replica_store1{new memgraph::storage::InMemoryStorage(configuration)};
  auto *main_mem_store = static_cast<memgraph::storage::InMemoryStorage *>(main_store.get());
  auto *replica_mem_store1 = static_cast<memgraph::storage::InMemoryStorage *>(replica_store1.get());

  replica_mem_store1->SetReplicaRole(memgraph::io::network::Endpoint{local_host, ports[0]},
                                     memgraph::storage::replication::ReplicationServerConfig{});

  std::unique_ptr<memgraph::storage::Storage> replica_store2{new memgraph::storage::InMemoryStorage(configuration)};
  auto *replica_mem_store2 = static_cast<memgraph::storage::InMemoryStorage *>(replica_store2.get());

  replica_mem_store2->SetReplicaRole(memgraph::io::network::Endpoint{local_host, ports[1]},
                                     memgraph::storage::replication::ReplicationServerConfig{});

  auto res = main_mem_store->RegisterReplica(replicas[0], memgraph::io::network::Endpoint{local_host, ports[0]},
                                             memgraph::storage::replication::ReplicationMode::SYNC,
                                             memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                             memgraph::storage::replication::ReplicationClientConfig{});
  ASSERT_FALSE(res.HasError());
  res = main_mem_store->RegisterReplica(replicas[1], memgraph::io::network::Endpoint{local_host, ports[1]},
                                        memgraph::storage::replication::ReplicationMode::SYNC,
                                        memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                        memgraph::storage::replication::ReplicationClientConfig{});
  ASSERT_FALSE(res.HasError());

  auto replica_infos = main_mem_store->ReplicasInfo();

  ASSERT_EQ(replica_infos.size(), 2);
  ASSERT_EQ(replica_infos[0].name, replicas[0]);
  ASSERT_EQ(replica_infos[0].endpoint.address, local_host);
  ASSERT_EQ(replica_infos[0].endpoint.port, ports[0]);
  ASSERT_EQ(replica_infos[1].name, replicas[1]);
  ASSERT_EQ(replica_infos[1].endpoint.address, local_host);
  ASSERT_EQ(replica_infos[1].endpoint.port, ports[1]);

  const auto unregister_res = main_mem_store->UnregisterReplica(replicas[0]);
  ASSERT_TRUE(unregister_res);

  replica_infos = main_mem_store->ReplicasInfo();
  ASSERT_EQ(replica_infos.size(), 1);
  ASSERT_EQ(replica_infos[0].name, replicas[1]);
  ASSERT_EQ(replica_infos[0].endpoint.address, local_host);
  ASSERT_EQ(replica_infos[0].endpoint.port, ports[1]);

  main_store.reset();

  std::unique_ptr<memgraph::storage::Storage> other_main_store{new memgraph::storage::InMemoryStorage(main_config)};
  auto *other_main_mem_store = static_cast<memgraph::storage::InMemoryStorage *>(other_main_store.get());
  replica_infos = other_main_mem_store->ReplicasInfo();
  ASSERT_EQ(replica_infos.size(), 1);
  ASSERT_EQ(replica_infos[0].name, replicas[1]);
  ASSERT_EQ(replica_infos[0].endpoint.address, local_host);
  ASSERT_EQ(replica_infos[0].endpoint.port, ports[1]);
}

TEST_F(ReplicationTest, AddingInvalidReplica) {
  std::unique_ptr<memgraph::storage::Storage> main_store{new memgraph::storage::InMemoryStorage(configuration)};
  auto *main_mem_store = static_cast<memgraph::storage::InMemoryStorage *>(main_store.get());

  ASSERT_TRUE(main_mem_store
                  ->RegisterReplica("REPLICA", memgraph::io::network::Endpoint{local_host, ports[0]},
                                    memgraph::storage::replication::ReplicationMode::SYNC,
                                    memgraph::storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                                    memgraph::storage::replication::ReplicationClientConfig{})
                  .GetError() == memgraph::storage::InMemoryStorage::RegisterReplicaError::CONNECTION_FAILED);
}
