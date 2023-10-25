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
#include "dbms/database.hpp"
#include "dbms/replication_handler.hpp"
#include "replication/config.hpp"
#include "replication/state.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/view.hpp"

using testing::UnorderedElementsAre;

using memgraph::dbms::RegisterReplicaError;
using memgraph::dbms::RegistrationMode;
using memgraph::dbms::ReplicationHandler;
using memgraph::dbms::UnregisterReplicaResult;
using memgraph::replication::ReplicationClientConfig;
using memgraph::replication::ReplicationMode;
using memgraph::replication::ReplicationRole;
using memgraph::replication::ReplicationServerConfig;
using memgraph::storage::Config;
using memgraph::storage::EdgeAccessor;
using memgraph::storage::Gid;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::PropertyValue;
using memgraph::storage::Storage;
using memgraph::storage::View;
using memgraph::storage::replication::ReplicaState;

class ReplicationTest : public ::testing::Test {
 protected:
  std::filesystem::path storage_directory{std::filesystem::temp_directory_path() /
                                          "MG_test_unit_storage_v2_replication"};
  std::filesystem::path repl_storage_directory{std::filesystem::temp_directory_path() /
                                               "MG_test_unit_storage_v2_replication_repl"};
  std::filesystem::path repl2_storage_directory{std::filesystem::temp_directory_path() /
                                                "MG_test_unit_storage_v2_replication_repl2"};
  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

  Config main_conf{.items = {.properties_on_edges = true},
                   .durability = {
                       .storage_directory = storage_directory,
                       .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                   }};
  Config repl_conf{.items = {.properties_on_edges = true},
                   .durability = {
                       .storage_directory = repl_storage_directory,
                       .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                   }};
  Config repl2_conf{.items = {.properties_on_edges = true},
                    .durability = {
                        .storage_directory = repl2_storage_directory,
                        .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                    }};

  const std::string local_host = ("127.0.0.1");
  const std::array<uint16_t, 2> ports{10000, 20000};
  const std::array<std::string, 2> replicas = {"REPLICA1", "REPLICA2"};

 private:
  void Clear() {
    if (std::filesystem::exists(storage_directory)) std::filesystem::remove_all(storage_directory);
    if (std::filesystem::exists(repl_storage_directory)) std::filesystem::remove_all(repl_storage_directory);
    if (std::filesystem::exists(repl2_storage_directory)) std::filesystem::remove_all(repl2_storage_directory);
  }
};

TEST_F(ReplicationTest, BasicSynchronousReplicationTest) {
  memgraph::replication::ReplicationState main_repl_state(ReplicationStateHelper(main_conf));
  memgraph::dbms::Database main_db(main_conf, main_repl_state);
  memgraph::replication::ReplicationState replica_repl_state(ReplicationStateHelper(repl_conf));
  memgraph::dbms::Database replica_db(repl_conf, replica_repl_state);
  replica_store_handler.SetReplicationRoleReplica(ReplicationServerConfig{
                   .HasError());

                   // vertex create
                   // vertex add label
                   // vertex set property
                   const auto *vertex_label = "vertex_label";
                   const auto *vertex_property = "vertex_property";
                   const auto *vertex_property_value = "vertex_property_value";
                   std::optional<Gid> vertex_gid;
                   {
                     auto acc = main_db.Access();
                     auto v = acc->CreateVertex();
                     vertex_gid.emplace(v.Gid());
                     ASSERT_TRUE(v.AddLabel(main_db.storage()->NameToLabel(vertex_label)).HasValue());
                     ASSERT_TRUE(v.SetProperty(main_db.storage()->NameToProperty(vertex_property),
                                               PropertyValue(vertex_property_value))
                                     .HasValue());
                     ASSERT_FALSE(acc->Commit().HasError());
                   }

                   {
                     auto acc = replica_db.Access();
                     const auto v = acc->FindVertex(*vertex_gid, View::OLD);
                     ASSERT_TRUE(v);
                     const auto labels = v->Labels(View::OLD);
                     ASSERT_TRUE(labels.HasValue());
                     ASSERT_EQ(labels->size(), 1);
                     ASSERT_THAT(*labels, UnorderedElementsAre(replica_db.storage()->NameToLabel(vertex_label)));
                     const auto properties = v->Properties(View::OLD);
                     ASSERT_TRUE(properties.HasValue());
                     ASSERT_EQ(properties->size(), 1);
                     ASSERT_THAT(*properties, UnorderedElementsAre(
                                                  std::make_pair(replica_db.storage()->NameToProperty(vertex_property),
                                                                 PropertyValue(vertex_property_value))));

                     ASSERT_FALSE(acc->Commit().HasError());
                   }

                   // vertex remove label
                   {
                     auto acc = main_db.Access();
                     auto v = acc->FindVertex(*vertex_gid, View::OLD);
                     ASSERT_TRUE(v);
                     ASSERT_TRUE(v->RemoveLabel(main_db.storage()->NameToLabel(vertex_label)).HasValue());
                     ASSERT_FALSE(acc->Commit().HasError());
                   }

                   {
                     auto acc = replica_db.Access();
                     const auto v = acc->FindVertex(*vertex_gid, View::OLD);
                     ASSERT_TRUE(v);
                     const auto labels = v->Labels(View::OLD);
                     ASSERT_TRUE(labels.HasValue());
                     ASSERT_EQ(labels->size(), 0);
                     ASSERT_FALSE(acc->Commit().HasError());
                   }

                   // vertex delete
                   {
                     auto acc = main_db.Access();
                     auto v = acc->FindVertex(*vertex_gid, View::OLD);
                     ASSERT_TRUE(v);
                     ASSERT_TRUE(acc->DeleteVertex(&*v).HasValue());
                     ASSERT_FALSE(acc->Commit().HasError());
                   }

                   {
                     auto acc = replica_db.Access();
                     const auto v = acc->FindVertex(*vertex_gid, View::OLD);
                     ASSERT_FALSE(v);
                     vertex_gid.reset();
                     ASSERT_FALSE(acc->Commit().HasError());
                   }

                   // edge create
                   // edge set property
                   const auto *edge_type = "edge_type";
                   const auto *edge_property = "edge_property";
                   const auto *edge_property_value = "edge_property_value";
                   std::optional<Gid> edge_gid;
                   {
                     auto acc = main_db.Access();
                     auto v = acc->CreateVertex();
                     vertex_gid.emplace(v.Gid());
                     auto edgeRes = acc->CreateEdge(&v, &v, main_db.storage()->NameToEdgeType(edge_type));
                     ASSERT_TRUE(edgeRes.HasValue());
                     auto edge = edgeRes.GetValue();
                     ASSERT_TRUE(edge.SetProperty(main_db.storage()->NameToProperty(edge_property),
                                                  PropertyValue(edge_property_value))
                                     .HasValue());
                     edge_gid.emplace(edge.Gid());
                     ASSERT_FALSE(acc->Commit().HasError());
                   }

                   const auto find_edge = [&](const auto &edges, const Gid edge_gid) -> std::optional<EdgeAccessor> {
                     for (const auto &edge : edges) {
                       if (edge.Gid() == edge_gid) {
                         return edge;
                       }
                     }
                     return std::nullopt;
                   };

                   {
                     auto acc = replica_db.Access();
                     const auto v = acc->FindVertex(*vertex_gid, View::OLD);
                     ASSERT_TRUE(v);
                     const auto out_edges = v->OutEdges(View::OLD);
                     ASSERT_TRUE(out_edges.HasValue());
                     const auto edge = find_edge(out_edges->edges, *edge_gid);
                     ASSERT_EQ(edge->EdgeType(), replica_db.storage()->NameToEdgeType(edge_type));
                     const auto properties = edge->Properties(View::OLD);
                     ASSERT_TRUE(properties.HasValue());
                     ASSERT_EQ(properties->size(), 1);
                     ASSERT_THAT(*properties, UnorderedElementsAre(
                                                  std::make_pair(replica_db.storage()->NameToProperty(edge_property),
                                                                 PropertyValue(edge_property_value))));
                     ASSERT_FALSE(acc->Commit().HasError());
                   }

                   // delete edge
                   {
                     auto acc = main_db.Access();
                     auto v = acc->FindVertex(*vertex_gid, View::OLD);
                     ASSERT_TRUE(v);
                     auto out_edges = v->OutEdges(View::OLD);
                     auto edge = find_edge(out_edges->edges, *edge_gid);
                     ASSERT_TRUE(edge);
                     ASSERT_TRUE(acc->DeleteEdge(&*edge).HasValue());
                     ASSERT_FALSE(acc->Commit().HasError());
                   }

                   {
                     auto acc = replica_db.Access();
                     const auto v = acc->FindVertex(*vertex_gid, View::OLD);
                     ASSERT_TRUE(v);
                     const auto out_edges = v->OutEdges(View::OLD);
                     ASSERT_TRUE(out_edges.HasValue());
                     ASSERT_FALSE(find_edge(out_edges->edges, *edge_gid));
                     ASSERT_FALSE(acc->Commit().HasError());
                   }

                   // label index create
                   // label property index create
                   // existence constraint create
                   // unique constriant create
                   const auto *label = "label";
                   const auto *property = "property";
                   const auto *property_extra = "property_extra";
                   const memgraph::storage::LabelIndexStats l_stats{12, 34};
                   const memgraph::storage::LabelPropertyIndexStats lp_stats{98, 76, 5.4, 3.2, 1.0};

                   {
                     auto unique_acc = main_db.UniqueAccess();
                     ASSERT_FALSE(unique_acc->CreateIndex(main_db.storage()->NameToLabel(label)).HasError());
                     ASSERT_FALSE(unique_acc->Commit().HasError());
                   }
                   {
                     auto unique_acc = main_db.UniqueAccess();
                     unique_acc->SetIndexStats(main_db.storage()->NameToLabel(label), l_stats);
                     ASSERT_FALSE(unique_acc->Commit().HasError());
                   }
                   {
                     auto unique_acc = main_db.UniqueAccess();
                     ASSERT_FALSE(unique_acc
                                      ->CreateIndex(main_db.storage()->NameToLabel(label),
                                                    main_db.storage()->NameToProperty(property))
                                      .HasError());
                     ASSERT_FALSE(unique_acc->Commit().HasError());
                   }
                   {
                     auto unique_acc = main_db.UniqueAccess();
                     unique_acc->SetIndexStats(main_db.storage()->NameToLabel(label),
                                               main_db.storage()->NameToProperty(property), lp_stats);
                     ASSERT_FALSE(unique_acc->Commit().HasError());
                   }
                   {
                     auto unique_acc = main_db.UniqueAccess();
                     ASSERT_FALSE(unique_acc
                                      ->CreateExistenceConstraint(main_db.storage()->NameToLabel(label),
                                                                  main_db.storage()->NameToProperty(property))
                                      .HasError());
                     ASSERT_FALSE(unique_acc->Commit().HasError());
                   }
                   {
                     auto unique_acc = main_db.UniqueAccess();
                     ASSERT_FALSE(unique_acc
                                      ->CreateUniqueConstraint(main_db.storage()->NameToLabel(label),
                                                               {main_db.storage()->NameToProperty(property),
                                                                main_db.storage()->NameToProperty(property_extra)})
                                      .HasError());
                     ASSERT_FALSE(unique_acc->Commit().HasError());
                   }

                   {
                     const auto indices = replica_db.Access()->ListAllIndices();
                     ASSERT_THAT(indices.label, UnorderedElementsAre(replica_db.storage()->NameToLabel(label)));
                     ASSERT_THAT(indices.label_property,
                                 UnorderedElementsAre(std::make_pair(replica_db.storage()->NameToLabel(label),
                                                                     replica_db.storage()->NameToProperty(property))));
                     const auto &l_stats_rep =
                         replica_db.Access()->GetIndexStats(replica_db.storage()->NameToLabel(label));
                     ASSERT_TRUE(l_stats_rep);
                     ASSERT_EQ(l_stats_rep->count, l_stats.count);
                     ASSERT_EQ(l_stats_rep->avg_degree, l_stats.avg_degree);
                     const auto &lp_stats_rep = replica_db.Access()->GetIndexStats(
                         replica_db.storage()->NameToLabel(label), replica_db.storage()->NameToProperty(property));
                     ASSERT_TRUE(lp_stats_rep);
                     ASSERT_EQ(lp_stats_rep->count, lp_stats.count);
                     ASSERT_EQ(lp_stats_rep->distinct_values_count, lp_stats.distinct_values_count);
                     ASSERT_EQ(lp_stats_rep->statistic, lp_stats.statistic);
                     ASSERT_EQ(lp_stats_rep->avg_group_size, lp_stats.avg_group_size);
                     ASSERT_EQ(lp_stats_rep->avg_degree, lp_stats.avg_degree);
                     const auto constraints = replica_db.Access()->ListAllConstraints();
                     ASSERT_THAT(constraints.existence,
                                 UnorderedElementsAre(std::make_pair(replica_db.storage()->NameToLabel(label),
                                                                     replica_db.storage()->NameToProperty(property))));
                     ASSERT_THAT(constraints.unique,
                                 UnorderedElementsAre(
                                     std::make_pair(replica_db.storage()->NameToLabel(label),
                                                    std::set{replica_db.storage()->NameToProperty(property),
                                                             replica_db.storage()->NameToProperty(property_extra)})));
                   }

                   // label index drop
                   // label property index drop
                   // existence constraint drop
                   // unique constriant drop
                   {
                     auto unique_acc = main_db.UniqueAccess();
                     unique_acc->DeleteLabelIndexStats(main_db.storage()->NameToLabel(label));
                     ASSERT_FALSE(unique_acc->Commit().HasError());
                   }
                   {
                     auto unique_acc = main_db.UniqueAccess();
                     ASSERT_FALSE(unique_acc->DropIndex(main_db.storage()->NameToLabel(label)).HasError());
                     ASSERT_FALSE(unique_acc->Commit().HasError());
                   }
                   {
                     auto unique_acc = main_db.UniqueAccess();
                     unique_acc->DeleteLabelPropertyIndexStats(main_db.storage()->NameToLabel(label));
                     ASSERT_FALSE(unique_acc->Commit().HasError());
                   }
                   {
                     auto unique_acc = main_db.UniqueAccess();
                     ASSERT_FALSE(unique_acc
                                      ->DropIndex(main_db.storage()->NameToLabel(label),
                                                  main_db.storage()->NameToProperty(property))
                                      .HasError());
                     ASSERT_FALSE(unique_acc->Commit().HasError());
                   }
                   {
                     auto unique_acc = main_db.UniqueAccess();
                     ASSERT_FALSE(unique_acc
                                      ->DropExistenceConstraint(main_db.storage()->NameToLabel(label),
                                                                main_db.storage()->NameToProperty(property))
                                      .HasError());
                     ASSERT_FALSE(unique_acc->Commit().HasError());
                   }
                   {
                     auto unique_acc = main_db.UniqueAccess();
                     ASSERT_EQ(unique_acc->DropUniqueConstraint(main_db.storage()->NameToLabel(label),
                                                                {main_db.storage()->NameToProperty(property),
                                                                 main_db.storage()->NameToProperty(property_extra)}),
                               memgraph::storage::UniqueConstraints::DeletionStatus::SUCCESS);
                     ASSERT_FALSE(unique_acc->Commit().HasError());
                   }

                   {
                     const auto indices = replica_db.Access()->ListAllIndices();
                     ASSERT_EQ(indices.label.size(), 0);
                     ASSERT_EQ(indices.label_property.size(), 0);

                     const auto &l_stats_rep =
                         replica_db.Access()->GetIndexStats(replica_db.storage()->NameToLabel(label));
                     ASSERT_FALSE(l_stats_rep);
                     const auto &lp_stats_rep = replica_db.Access()->GetIndexStats(
                         replica_db.storage()->NameToLabel(label), replica_db.storage()->NameToProperty(property));
                     ASSERT_FALSE(lp_stats_rep);

                     const auto constraints = replica_db.Access()->ListAllConstraints();
                     ASSERT_EQ(constraints.existence.size(), 0);
                     ASSERT_EQ(constraints.unique.size(), 0);
                   }
}

TEST_F(ReplicationTest, MultipleSynchronousReplicationTest) {
    memgraph::replication::ReplicationState main_repl_state(ReplicationStateHelper(main_conf));
    memgraph::dbms::Database main_db(main_conf, main_repl_state);
    memgraph::replication::ReplicationState replica1_repl_state(ReplicationStateHelper(repl_conf));
    memgraph::dbms::Database replica1_db(repl_conf, replica1_repl_state);
    memgraph::replication::ReplicationState replica2_repl_state(ReplicationStateHelper(repl2_conf));
    memgraph::dbms::Database replica2_db(repl2_conf, replica2_repl_state);

    auto replica1_store_handler = ReplicationHandler{replica1_repl_state, *replica1_db.storage()};
    replica1_store_handler.SetReplicationRoleReplica(ReplicationServerConfig{
        .ip_address = local_host,
        .port = ports[0],
    });
    auto replica2_store_handler = ReplicationHandler{replica2_repl_state, *replica2_db.storage()};
    replica2_store_handler.SetReplicationRoleReplica(ReplicationServerConfig{
        .ip_address = local_host,
        .port = ports[1],
    });

    auto main_store_handler = ReplicationHandler{main_repl_state, *main_db.storage()};
    ASSERT_FALSE(main_store_handler
                     .RegisterReplica(ReplicationClientConfig{
                         .name = replicas[0],
                         .mode = ReplicationMode::SYNC,
                         .ip_address = local_host,
                         .port = ports[0],
                     })
                     .HasError());
    ASSERT_FALSE(main_store_handler
                     .RegisterReplica(ReplicationClientConfig{
                         .name = replicas[1],
                         .mode = ReplicationMode::SYNC,
                         .ip_address = local_host,
                         .port = ports[1],
                     })
                     .HasError());

    const auto *vertex_label = "label";
    const auto *vertex_property = "property";
    const auto *vertex_property_value = "property_value";
    std::optional<Gid> vertex_gid;
    {
      auto acc = main_db.Access();
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.AddLabel(main_db.storage()->NameToLabel(vertex_label)).HasValue());
      ASSERT_TRUE(
          v.SetProperty(main_db.storage()->NameToProperty(vertex_property), PropertyValue(vertex_property_value))
              .HasValue());
      vertex_gid.emplace(v.Gid());
      ASSERT_FALSE(acc->Commit().HasError());
    }

    const auto check_replica = [&](Storage *replica_store) {
      auto acc = replica_store->Access();
      const auto v = acc->FindVertex(*vertex_gid, View::OLD);
      ASSERT_TRUE(v);
      const auto labels = v->Labels(View::OLD);
      ASSERT_TRUE(labels.HasValue());
      ASSERT_THAT(*labels, UnorderedElementsAre(replica_store->NameToLabel(vertex_label)));
      ASSERT_FALSE(acc->Commit().HasError());
    };

    check_replica(replica1_db.storage());
    check_replica(replica2_db.storage());

    auto handler = ReplicationHandler{main_repl_state, *main_db.storage()};
    handler.UnregisterReplica(replicas[1]);
    {
      auto acc = main_db.Access();
      auto v = acc->CreateVertex();
      vertex_gid.emplace(v.Gid());
      ASSERT_FALSE(acc->Commit().HasError());
    }

    // REPLICA1 should contain the new vertex
    {
      auto acc = replica1_db.storage()->Access();
      const auto v = acc->FindVertex(*vertex_gid, View::OLD);
      ASSERT_TRUE(v);
      ASSERT_FALSE(acc->Commit().HasError());
    }

    // REPLICA2 should not contain the new vertex
    {
      auto acc = replica2_db.storage()->Access();
      const auto v = acc->FindVertex(*vertex_gid, View::OLD);
      ASSERT_FALSE(v);
      ASSERT_FALSE(acc->Commit().HasError());
    }
}

TEST_F(ReplicationTest, RecoveryProcess) {
    std::vector<Gid> vertex_gids;
    // Force the creation of snapshot
    {
      memgraph::storage::Config conf{
          .durability = {
              .storage_directory = storage_directory,
              .recover_on_startup = true,
              .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
              .snapshot_on_exit = true,
          }};
      memgraph::replication::ReplicationState main_repl_state(ReplicationStateHelper(conf));
      memgraph::dbms::Database main_db(conf, main_repl_state);

      {
        auto acc = main_db.Access();
        // Create the vertex before registering a replica
        auto v = acc->CreateVertex();
        vertex_gids.emplace_back(v.Gid());
        ASSERT_FALSE(acc->Commit().HasError());
      }
    }

    {
      // Create second WAL
      memgraph::storage::Config conf{
          .durability = {.storage_directory = storage_directory,
                         .recover_on_startup = true,
                         .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL}};
      memgraph::replication::ReplicationState main_repl_state(ReplicationStateHelper(conf));
      memgraph::dbms::Database main_db(conf, main_repl_state);
      // Create vertices in 2 different transactions
      {
        auto acc = main_db.Access();
        auto v = acc->CreateVertex();
        vertex_gids.emplace_back(v.Gid());
        ASSERT_FALSE(acc->Commit().HasError());
      }
      {
        auto acc = main_db.Access();
        auto v = acc->CreateVertex();
        vertex_gids.emplace_back(v.Gid());
        ASSERT_FALSE(acc->Commit().HasError());
      }
    }

    memgraph::storage::Config conf{
        .durability = {
            .storage_directory = storage_directory,
            .recover_on_startup = true,
            .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
        }};
    memgraph::replication::ReplicationState main_repl_state(ReplicationStateHelper(conf));
    memgraph::dbms::Database main_db(conf, main_repl_state);

    static constexpr const auto *property_name = "property_name";
    static constexpr const auto property_value = 1;
    {
      // Force the creation of current WAL file
      auto acc = main_db.Access();
      for (const auto &vertex_gid : vertex_gids) {
        auto v = acc->FindVertex(vertex_gid, View::OLD);
        ASSERT_TRUE(v);
        ASSERT_TRUE(
            v->SetProperty(main_db.storage()->NameToProperty(property_name), PropertyValue(property_value)).HasValue());
      }
      ASSERT_FALSE(acc->Commit().HasError());
    }

    static constexpr const auto *vertex_label = "vertex_label";
    {
      memgraph::replication::ReplicationState replica_repl_state(ReplicationStateHelper(repl_conf));
      memgraph::dbms::Database replica_db(repl_conf, replica_repl_state);
      auto replica_store_handler = ReplicationHandler{replica_repl_state, *replica_db.storage()};

      replica_store_handler.SetReplicationRoleReplica(ReplicationServerConfig{
          .ip_address = local_host,
          .port = ports[0],
      });
      auto main_store_handler = ReplicationHandler{main_repl_state, *main_db.storage()};
      ASSERT_FALSE(main_store_handler
                       .RegisterReplica(ReplicationClientConfig{
                           .name = replicas[0],
                           .mode = ReplicationMode::SYNC,
                           .ip_address = local_host,
                           .port = ports[0],
                       })
                       .HasError());

      ASSERT_EQ(main_db.storage()->GetReplicaState(replicas[0]), ReplicaState::RECOVERY);

      while (main_db.storage()->GetReplicaState(replicas[0]) != ReplicaState::READY) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }

      {
        auto acc = main_db.Access();
        for (const auto &vertex_gid : vertex_gids) {
          auto v = acc->FindVertex(vertex_gid, View::OLD);
          ASSERT_TRUE(v);
          ASSERT_TRUE(v->AddLabel(main_db.storage()->NameToLabel(vertex_label)).HasValue());
        }
        ASSERT_FALSE(acc->Commit().HasError());
      }
      {
        auto acc = replica_db.Access();
        for (const auto &vertex_gid : vertex_gids) {
          auto v = acc->FindVertex(vertex_gid, View::OLD);
          ASSERT_TRUE(v);
          const auto labels = v->Labels(View::OLD);
          ASSERT_TRUE(labels.HasValue());
          ASSERT_THAT(*labels, UnorderedElementsAre(replica_db.storage()->NameToLabel(vertex_label)));
          const auto properties = v->Properties(View::OLD);
          ASSERT_TRUE(properties.HasValue());
          ASSERT_THAT(*properties,
                      UnorderedElementsAre(std::make_pair(replica_db.storage()->NameToProperty(property_name),
                                                          PropertyValue(property_value))));
        }
        ASSERT_FALSE(acc->Commit().HasError());
      }
    }
    {
      memgraph::storage::Config repl_conf{
          .durability = {.storage_directory = repl_storage_directory,
                         .recover_on_startup = true,
                         .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL}};
      memgraph::replication::ReplicationState replica_repl_state(ReplicationStateHelper(repl_conf));
      memgraph::dbms::Database replica_db(repl_conf, replica_repl_state);
      {
        auto acc = replica_db.Access();
        for (const auto &vertex_gid : vertex_gids) {
          auto v = acc->FindVertex(vertex_gid, View::OLD);
          ASSERT_TRUE(v);
          const auto labels = v->Labels(View::OLD);
          ASSERT_TRUE(labels.HasValue());
          ASSERT_THAT(*labels, UnorderedElementsAre(replica_db.storage()->NameToLabel(vertex_label)));
          const auto properties = v->Properties(View::OLD);
          ASSERT_TRUE(properties.HasValue());
          ASSERT_THAT(*properties,
                      UnorderedElementsAre(std::make_pair(replica_db.storage()->NameToProperty(property_name),
                                                          PropertyValue(property_value))));
        }
        ASSERT_FALSE(acc->Commit().HasError());
      }
    }
}

TEST_F(ReplicationTest, BasicAsynchronousReplicationTest) {
    memgraph::replication::ReplicationState main_repl_state(ReplicationStateHelper(main_conf));
    memgraph::dbms::Database main_db(main_conf, main_repl_state);
    memgraph::replication::ReplicationState replica_async_repl_state(ReplicationStateHelper(repl_conf));
    memgraph::dbms::Database replica_async_db(repl_conf, replica_async_repl_state);

    auto replica_store_handler = ReplicationHandler{replica_async_repl_state, *replica_async_db.storage()};
    replica_store_handler.SetReplicationRoleReplica(ReplicationServerConfig{
        .ip_address = local_host,
        .port = ports[1],
    });

    auto main_store_handler = ReplicationHandler{main_repl_state, *main_db.storage()};
    ASSERT_FALSE(main_store_handler
                     .RegisterReplica(ReplicationClientConfig{
                         .name = "REPLICA_ASYNC",
                         .mode = ReplicationMode::ASYNC,
                         .ip_address = local_host,
                         .port = ports[1],
                     })
                     .HasError());

    static constexpr size_t vertices_create_num = 10;
    std::vector<Gid> created_vertices;
    for (size_t i = 0; i < vertices_create_num; ++i) {
      auto acc = main_db.Access();
      auto v = acc->CreateVertex();
      created_vertices.push_back(v.Gid());
      ASSERT_FALSE(acc->Commit().HasError());

      if (i == 0) {
        ASSERT_EQ(main_db.storage()->GetReplicaState("REPLICA_ASYNC"), ReplicaState::REPLICATING);
      } else {
        ASSERT_EQ(main_db.storage()->GetReplicaState("REPLICA_ASYNC"), ReplicaState::RECOVERY);
      }
    }

    while (main_db.storage()->GetReplicaState("REPLICA_ASYNC") != ReplicaState::READY) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_TRUE(std::all_of(created_vertices.begin(), created_vertices.end(), [&](const auto vertex_gid) {
      auto acc = replica_async_db.storage()->Access();
      auto v = acc->FindVertex(vertex_gid, View::OLD);
      const bool exists = v.has_value();
      EXPECT_FALSE(acc->Commit().HasError());
      return exists;
    }));
}

TEST_F(ReplicationTest, EpochTest) {
    memgraph::replication::ReplicationState main_repl_state(ReplicationStateHelper(main_conf));
    memgraph::dbms::Database main_db(main_conf, main_repl_state);
    memgraph::replication::ReplicationState replica1_repl_state(ReplicationStateHelper(repl_conf));
    memgraph::dbms::Database replica1_db(repl_conf, replica1_repl_state);

    auto replica1_store_handler = ReplicationHandler{replica1_repl_state, *replica1_db.storage()};
    replica1_store_handler.SetReplicationRoleReplica(ReplicationServerConfig{
        .ip_address = local_host,
        .port = ports[0],
    });

    memgraph::replication::ReplicationState replica2_repl_state(ReplicationStateHelper(repl2_conf));
    memgraph::dbms::Database replica2_db(repl2_conf, replica2_repl_state);

    auto replica2_store_handler = ReplicationHandler{replica2_repl_state, *replica2_db.storage()};
    replica2_store_handler.SetReplicationRoleReplica(ReplicationServerConfig{
        .ip_address = local_host,
        .port = 10001,
    });

    auto main_store_handler = ReplicationHandler{main_repl_state, *main_db.storage()};
    ASSERT_FALSE(main_store_handler
                     .RegisterReplica(ReplicationClientConfig{
                         .name = replicas[0],
                         .mode = ReplicationMode::SYNC,
                         .ip_address = local_host,
                         .port = ports[0],
                     })
                     .HasError());

    ASSERT_FALSE(main_store_handler
                     .RegisterReplica(ReplicationClientConfig{
                         .name = replicas[1],
                         .mode = ReplicationMode::SYNC,
                         .ip_address = local_host,
                         .port = 10001,
                     })
                     .HasError());

    std::optional<Gid> vertex_gid;
    {
      auto acc = main_db.Access();
      const auto v = acc->CreateVertex();
      vertex_gid.emplace(v.Gid());
      ASSERT_FALSE(acc->Commit().HasError());
    }
    {
      auto acc = replica1_db.storage()->Access();
      const auto v = acc->FindVertex(*vertex_gid, View::OLD);
      ASSERT_TRUE(v);
      ASSERT_FALSE(acc->Commit().HasError());
    }
    {
      auto acc = replica2_db.storage()->Access();
      const auto v = acc->FindVertex(*vertex_gid, View::OLD);
      ASSERT_TRUE(v);
      ASSERT_FALSE(acc->Commit().HasError());
    }

    main_store_handler.UnregisterReplica(replicas[0]);
    main_store_handler.UnregisterReplica(replicas[1]);

    ASSERT_TRUE(replica1_store_handler.SetReplicationRoleMain());

    ASSERT_FALSE(replica1_store_handler
                     .RegisterReplica(ReplicationClientConfig{
                         .name = replicas[1],
                         .mode = ReplicationMode::SYNC,
                         .ip_address = local_host,
                         .port = 10001,
                     })

                     .HasError());

    {
      auto acc = main_db.Access();
      acc->CreateVertex();
      ASSERT_FALSE(acc->Commit().HasError());
    }
    {
      auto acc = replica1_db.storage()->Access();
      auto v = acc->CreateVertex();
      vertex_gid.emplace(v.Gid());
      ASSERT_FALSE(acc->Commit().HasError());
    }
    // Replica1 should forward it's vertex to Replica2
    {
      auto acc = replica2_db.storage()->Access();
      const auto v = acc->FindVertex(*vertex_gid, View::OLD);
      ASSERT_TRUE(v);
      ASSERT_FALSE(acc->Commit().HasError());
    }

    replica1_store_handler.SetReplicationRoleReplica(ReplicationServerConfig{
        .ip_address = local_host,
        .port = ports[0],
    });
    ASSERT_TRUE(main_store_handler
                    .RegisterReplica(ReplicationClientConfig{
                        .name = replicas[0],
                        .mode = ReplicationMode::SYNC,
                        .ip_address = local_host,
                        .port = ports[0],
                    })

                    .HasError());

    {
      auto acc = main_db.Access();
      const auto v = acc->CreateVertex();
      vertex_gid.emplace(v.Gid());
      ASSERT_FALSE(acc->Commit().HasError());
    }
    // Replica1 is not compatible with the main so it shouldn't contain
    // it's newest vertex
    {
      auto acc = replica1_db.storage()->Access();
      const auto v = acc->FindVertex(*vertex_gid, View::OLD);
      ASSERT_FALSE(v);
      ASSERT_FALSE(acc->Commit().HasError());
    }
}

TEST_F(ReplicationTest, ReplicationInformation) {
    memgraph::replication::ReplicationState main_repl_state(ReplicationStateHelper(main_conf));
    memgraph::dbms::Database main_db(main_conf, main_repl_state);
    memgraph::replication::ReplicationState replica1_repl_state(ReplicationStateHelper(repl_conf));
    memgraph::dbms::Database replica1_db(repl_conf, replica1_repl_state);

    uint16_t replica1_port = 10001;
    auto replica1_store_handler = ReplicationHandler{replica1_repl_state, *replica1_db.storage()};
    replica1_store_handler.SetReplicationRoleReplica(ReplicationServerConfig{
        .ip_address = local_host,
        .port = replica1_port,
    });

    uint16_t replica2_port = 10002;
    memgraph::replication::ReplicationState replica2_repl_state(ReplicationStateHelper(repl2_conf));
    memgraph::dbms::Database replica2_db(repl2_conf, replica2_repl_state);
    auto replica2_store_handler = ReplicationHandler{replica2_repl_state, *replica2_db.storage()};
    replica2_store_handler.SetReplicationRoleReplica(ReplicationServerConfig{
        .ip_address = local_host,
        .port = replica2_port,
    });

    auto main_store_handler = ReplicationHandler{main_repl_state, *main_db.storage()};
    ASSERT_FALSE(main_store_handler
                     .RegisterReplica(ReplicationClientConfig{
                         .name = replicas[0],
                         .mode = ReplicationMode::SYNC,
                         .ip_address = local_host,
                         .port = replica1_port,
                     })

                     .HasError());

    ASSERT_FALSE(main_store_handler
                     .RegisterReplica(ReplicationClientConfig{
                         .name = replicas[1],
                         .mode = ReplicationMode::ASYNC,
                         .ip_address = local_host,
                         .port = replica2_port,
                     })

                     .HasError());

    ASSERT_TRUE(main_repl_state.IsMain());
    ASSERT_TRUE(replica1_repl_state.IsReplica());
    ASSERT_TRUE(replica2_repl_state.IsReplica());

    const auto replicas_info = main_db.storage()->ReplicasInfo();
    ASSERT_EQ(replicas_info.size(), 2);

    const auto &first_info = replicas_info[0];
    ASSERT_EQ(first_info.name, replicas[0]);
    ASSERT_EQ(first_info.mode, ReplicationMode::SYNC);
    ASSERT_EQ(first_info.endpoint, (memgraph::io::network::Endpoint{local_host, replica1_port}));
    ASSERT_EQ(first_info.state, ReplicaState::READY);

    const auto &second_info = replicas_info[1];
    ASSERT_EQ(second_info.name, replicas[1]);
    ASSERT_EQ(second_info.mode, ReplicationMode::ASYNC);
    ASSERT_EQ(second_info.endpoint, (memgraph::io::network::Endpoint{local_host, replica2_port}));
    ASSERT_EQ(second_info.state, ReplicaState::READY);
}

TEST_F(ReplicationTest, ReplicationReplicaWithExistingName) {
    memgraph::replication::ReplicationState main_repl_state(ReplicationStateHelper(main_conf));
    memgraph::dbms::Database main_db(main_conf, main_repl_state);
    memgraph::replication::ReplicationState replica1_repl_state(ReplicationStateHelper(repl_conf));
    memgraph::dbms::Database replica1_db(repl_conf, replica1_repl_state);

    uint16_t replica1_port = 10001;
    auto replica1_store_handler = ReplicationHandler{replica1_repl_state, *replica1_db.storage()};
    replica1_store_handler.SetReplicationRoleReplica(ReplicationServerConfig{
        .ip_address = local_host,
        .port = replica1_port,
    });

    uint16_t replica2_port = 10002;
    memgraph::replication::ReplicationState replica2_repl_state(ReplicationStateHelper(repl2_conf));
    memgraph::dbms::Database replica2_db(repl2_conf, replica2_repl_state);
    auto replica2_store_handler = ReplicationHandler{replica2_repl_state, *replica2_db.storage()};
    replica2_store_handler.SetReplicationRoleReplica(ReplicationServerConfig{
        .ip_address = local_host,
        .port = replica2_port,
    });
    auto main_store_handler = ReplicationHandler{main_repl_state, *main_db.storage()};
    ASSERT_FALSE(main_store_handler
                     .RegisterReplica(ReplicationClientConfig{
                         .name = replicas[0],
                         .mode = ReplicationMode::SYNC,
                         .ip_address = local_host,
                         .port = replica1_port,
                     })
                     .HasError());

    ASSERT_TRUE(main_store_handler
                    .RegisterReplica(ReplicationClientConfig{
                        .name = replicas[0],
                        .mode = ReplicationMode::ASYNC,
                        .ip_address = local_host,
                        .port = replica2_port,
                    })
                    .GetError() == RegisterReplicaError::NAME_EXISTS);
}

TEST_F(ReplicationTest, ReplicationReplicaWithExistingEndPoint) {
    uint16_t common_port = 10001;

    memgraph::replication::ReplicationState main_repl_state(ReplicationStateHelper(main_conf));
    memgraph::dbms::Database main_db(main_conf, main_repl_state);
    memgraph::replication::ReplicationState replica1_repl_state(ReplicationStateHelper(repl_conf));
    memgraph::dbms::Database replica1_db(repl_conf, replica1_repl_state);
    auto replica1_store_handler = ReplicationHandler{replica1_repl_state, *replica1_db.storage()};
    replica1_store_handler.SetReplicationRoleReplica(ReplicationServerConfig{
        .ip_address = local_host,
        .port = common_port,
    });

    memgraph::replication::ReplicationState replica2_repl_state(ReplicationStateHelper(repl2_conf));
    memgraph::dbms::Database replica2_db(repl2_conf, replica2_repl_state);
    auto replica2_store_handler = ReplicationHandler{replica2_repl_state, *replica2_db.storage()};
    replica2_store_handler.SetReplicationRoleReplica(ReplicationServerConfig{
        .ip_address = local_host,
        .port = common_port,
    });

    auto main_store_handler = ReplicationHandler{main_repl_state, *main_db.storage()};
    ASSERT_FALSE(main_store_handler
                     .RegisterReplica(ReplicationClientConfig{
                         .name = replicas[0],
                         .mode = ReplicationMode::SYNC,
                         .ip_address = local_host,
                         .port = common_port,
                     })
                     .HasError());

    ASSERT_TRUE(main_store_handler
                    .RegisterReplica(ReplicationClientConfig{
                        .name = replicas[1],
                        .mode = ReplicationMode::ASYNC,
                        .ip_address = local_host,
                        .port = common_port,
                    })
                    .GetError() == RegisterReplicaError::END_POINT_EXISTS);
}

TEST_F(ReplicationTest, RestoringReplicationAtStartupAfterDroppingReplica) {
    auto main_config = main_conf;
    auto replica1_config = main_conf;
    auto replica2_config = main_conf;
    main_config.durability.restore_replication_state_on_startup = true;

    std::filesystem::path replica1_storage_directory{std::filesystem::temp_directory_path() / "replica1"};
    std::filesystem::path replica2_storage_directory{std::filesystem::temp_directory_path() / "replica2"};
    memgraph::utils::OnScopeExit replica1_directory_cleaner(
        [&]() { std::filesystem::remove_all(replica1_storage_directory); });
    memgraph::utils::OnScopeExit replica2_directory_cleaner(
        [&]() { std::filesystem::remove_all(replica2_storage_directory); });

    replica1_config.durability.storage_directory = replica1_storage_directory;
    replica2_config.durability.storage_directory = replica2_storage_directory;

    std::optional<memgraph::replication::ReplicationState> main_repl_state(ReplicationStateHelper(main_config));
    std::optional<memgraph::dbms::Database> main_db(std::in_place, main_config, *main_repl_state);
    memgraph::replication::ReplicationState replica1_repl_state(ReplicationStateHelper(replica1_config));
    memgraph::dbms::Database replica1_db(replica1_config, replica1_repl_state);

    auto replica1_store_handler = ReplicationHandler{replica1_repl_state, *replica1_db.storage()};
    replica1_store_handler.SetReplicationRoleReplica(ReplicationServerConfig{
        .ip_address = local_host,
        .port = ports[0],
    });

    memgraph::replication::ReplicationState replica2_repl_state(ReplicationStateHelper(replica2_config));
    memgraph::dbms::Database replica2_db(replica2_config, replica2_repl_state);
    auto replica2_store_handler = ReplicationHandler{replica2_repl_state, *replica2_db.storage()};
    replica2_store_handler.SetReplicationRoleReplica(ReplicationServerConfig{
        .ip_address = local_host,
        .port = ports[1],
    });

    auto main_store_handler = ReplicationHandler{*main_repl_state, *main_db->storage()};
    auto res = main_store_handler.RegisterReplica(ReplicationClientConfig{
        .name = replicas[0],
        .mode = ReplicationMode::SYNC,
        .ip_address = local_host,
        .port = ports[0],
    });
    ASSERT_FALSE(res.HasError());
    res = main_store_handler.RegisterReplica(ReplicationClientConfig{
        .name = replicas[1],
        .mode = ReplicationMode::SYNC,
        .ip_address = local_host,
        .port = ports[1],
    });
    ASSERT_FALSE(res.HasError());

    auto replica_infos = main_db->storage()->ReplicasInfo();

    ASSERT_EQ(replica_infos.size(), 2);
    ASSERT_EQ(replica_infos[0].name, replicas[0]);
    ASSERT_EQ(replica_infos[0].endpoint.address, local_host);
    ASSERT_EQ(replica_infos[0].endpoint.port, ports[0]);
    ASSERT_EQ(replica_infos[1].name, replicas[1]);
    ASSERT_EQ(replica_infos[1].endpoint.address, local_host);
    ASSERT_EQ(replica_infos[1].endpoint.port, ports[1]);

    main_repl_state.reset();
    main_db.reset();

    std::optional<memgraph::replication::ReplicationState> other_main_repl_state(ReplicationStateHelper(main_config));
    std::optional<memgraph::dbms::Database> other_main_db{std::in_place, main_config, *other_main_repl_state};

    replica_infos = other_main_db->storage()->ReplicasInfo();
    ASSERT_EQ(replica_infos.size(), 2);
    ASSERT_EQ(replica_infos[0].name, replicas[0]);
    ASSERT_EQ(replica_infos[0].endpoint.address, local_host);
    ASSERT_EQ(replica_infos[0].endpoint.port, ports[0]);
    ASSERT_EQ(replica_infos[1].name, replicas[1]);
    ASSERT_EQ(replica_infos[1].endpoint.address, local_host);
    ASSERT_EQ(replica_infos[1].endpoint.port, ports[1]);
}

TEST_F(ReplicationTest, RestoringReplicationAtStartup) {
    auto main_config = main_conf;
    main_config.durability.restore_replication_state_on_startup = true;

    std::optional<memgraph::replication::ReplicationState> main_repl_state(ReplicationStateHelper(main_config));
    std::optional<memgraph::dbms::Database> main_db(std::in_place, main_config, *main_repl_state);
    memgraph::replication::ReplicationState replica1_repl_state(ReplicationStateHelper(repl_conf));
    memgraph::dbms::Database replica1_db(repl_conf, replica1_repl_state);

    auto replica1_store_handler = ReplicationHandler{replica1_repl_state, *replica1_db.storage()};
    replica1_store_handler.SetReplicationRoleReplica(ReplicationServerConfig{
        .ip_address = local_host,
        .port = ports[0],
    });

    memgraph::replication::ReplicationState replica2_repl_state(ReplicationStateHelper(repl2_conf));
    memgraph::dbms::Database replica2_db(repl2_conf, replica2_repl_state);

    auto replica2_store_handler = ReplicationHandler{replica2_repl_state, *replica2_db.storage()};
    replica2_store_handler.SetReplicationRoleReplica(ReplicationServerConfig{
        .ip_address = local_host,
        .port = ports[1],
    });
    auto main_store_handler = ReplicationHandler{*main_repl_state, *main_db->storage()};
    auto res = main_store_handler.RegisterReplica(ReplicationClientConfig{
        .name = replicas[0],
        .mode = ReplicationMode::SYNC,
        .ip_address = local_host,
        .port = ports[0],
    });
    ASSERT_FALSE(res.HasError());
    res = main_store_handler.RegisterReplica(ReplicationClientConfig{
        .name = replicas[1],
        .mode = ReplicationMode::SYNC,
        .ip_address = local_host,
        .port = ports[1],
    });
    ASSERT_FALSE(res.HasError());

    auto replica_infos = main_db->storage()->ReplicasInfo();

    ASSERT_EQ(replica_infos.size(), 2);
    ASSERT_EQ(replica_infos[0].name, replicas[0]);
    ASSERT_EQ(replica_infos[0].endpoint.address, local_host);
    ASSERT_EQ(replica_infos[0].endpoint.port, ports[0]);
    ASSERT_EQ(replica_infos[1].name, replicas[1]);
    ASSERT_EQ(replica_infos[1].endpoint.address, local_host);
    ASSERT_EQ(replica_infos[1].endpoint.port, ports[1]);

    auto handler = ReplicationHandler{*main_repl_state, *main_db->storage()};
    const auto unregister_res = handler.UnregisterReplica(replicas[0]);
    ASSERT_EQ(unregister_res, UnregisterReplicaResult::SUCCESS);

    replica_infos = main_db->storage()->ReplicasInfo();
    ASSERT_EQ(replica_infos.size(), 1);
    ASSERT_EQ(replica_infos[0].name, replicas[1]);
    ASSERT_EQ(replica_infos[0].endpoint.address, local_host);
    ASSERT_EQ(replica_infos[0].endpoint.port, ports[1]);

    main_repl_state.reset();
    main_db.reset();

    std::optional<memgraph::replication::ReplicationState> other_main_repl_state(ReplicationStateHelper(main_config));
    std::optional<memgraph::dbms::Database> other_main_db{std::in_place, main_config, *other_main_repl_state};

    replica_infos = other_main_db->storage()->ReplicasInfo();
    ASSERT_EQ(replica_infos.size(), 1);
    ASSERT_EQ(replica_infos[0].name, replicas[1]);
    ASSERT_EQ(replica_infos[0].endpoint.address, local_host);
    ASSERT_EQ(replica_infos[0].endpoint.port, ports[1]);
}

TEST_F(ReplicationTest, AddingInvalidReplica) {
    memgraph::replication::ReplicationState main_repl_state(ReplicationStateHelper(main_conf));
    memgraph::dbms::Database main_db(main_conf, main_repl_state);

    auto main_store_handler = ReplicationHandler{main_repl_state, *main_db.storage()};
    ASSERT_TRUE(main_store_handler
                    .RegisterReplica(ReplicationClientConfig{
                        .name = "REPLICA",
                        .mode = ReplicationMode::SYNC,
                        .ip_address = local_host,
                        .port = ports[0],
                    })
                    .GetError() == RegisterReplicaError::CONNECTION_FAILED);
}
