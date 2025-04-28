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

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <thread>

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <storage/v2/inmemory/storage.hpp>
#include <storage/v2/property_value.hpp>
#include <storage/v2/replication/enums.hpp>
#include "auth/auth.hpp"
#include "dbms/database.hpp"
#include "dbms/dbms_handler.hpp"
#include "query/interpreter_context.hpp"
#include "replication/config.hpp"
#include "replication/state.hpp"
#include "replication_handler/replication_handler.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/replication/recovery.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/view.hpp"
#include "tests/unit/storage_test_utils.hpp"

using testing::UnorderedElementsAre;

using memgraph::io::network::Endpoint;
using memgraph::query::RegisterReplicaError;
using memgraph::query::UnregisterReplicaResult;
using memgraph::replication::ReplicationClientConfig;
using memgraph::replication::ReplicationHandler;
using memgraph::replication::ReplicationServerConfig;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::replication_coordination_glue::ReplicationRole;
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

  Config main_conf = [&] {
    Config config{
        .durability =
            {
                .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            },
        .salient.items = {.properties_on_edges = true},
    };
    UpdatePaths(config, storage_directory);
    return config;
  }();
  Config repl_conf = [&] {
    Config config{
        .durability =
            {
                .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            },
        .salient.items = {.properties_on_edges = true},
    };
    UpdatePaths(config, repl_storage_directory);
    return config;
  }();
  Config repl2_conf = [&] {
    Config config{
        .durability =
            {
                .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            },
        .salient.items = {.properties_on_edges = true},
    };
    UpdatePaths(config, repl2_storage_directory);
    return config;
  }();

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

struct MinMemgraph {
  MinMemgraph(const memgraph::storage::Config &conf)
      : auth{conf.durability.storage_directory / "auth", memgraph::auth::Auth::Config{/* default */}},
        repl_state{ReplicationStateRootPath(conf)},
        dbms{conf, repl_state
#ifdef MG_ENTERPRISE
             ,
             auth, true
#endif
        },
        db_acc{dbms.Get()},
        db{*db_acc.get()},
        repl_handler(repl_state, dbms
#ifdef MG_ENTERPRISE
                     ,
                     system_, auth
#endif
        ) {
  }
  memgraph::auth::SynchedAuth auth;
  memgraph::system::System system_;
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state;
  memgraph::dbms::DbmsHandler dbms;
  memgraph::dbms::DatabaseAccess db_acc;
  memgraph::dbms::Database &db;
  ReplicationHandler repl_handler;
};

TEST_F(ReplicationTest, BasicSynchronousReplicationTest) {
  MinMemgraph main(main_conf);
  MinMemgraph replica(repl_conf);

  auto replica_store_handler = replica.repl_handler;
  replica_store_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{.repl_server = Endpoint(local_host, ports[0])}, std::nullopt);

  const auto &reg = main.repl_handler.TryRegisterReplica(ReplicationClientConfig{
      .name = "REPLICA",
      .mode = ReplicationMode::SYNC,
      .repl_server_endpoint = Endpoint(local_host, ports[0]),
  });
  ASSERT_FALSE(reg.HasError()) << (int)reg.GetError();

  // vertex create
  // vertex add label
  // vertex set property
  const auto *vertex_label = "vertex_label";
  const auto *vertex_property = "vertex_property";
  const auto *vertex_property_value = "vertex_property_value";
  std::optional<Gid> vertex_gid;
  {
    auto acc = main.db.Access();
    auto v = acc->CreateVertex();
    vertex_gid.emplace(v.Gid());
    ASSERT_TRUE(v.AddLabel(main.db.storage()->NameToLabel(vertex_label)).HasValue());
    ASSERT_TRUE(v.SetProperty(main.db.storage()->NameToProperty(vertex_property), PropertyValue(vertex_property_value))
                    .HasValue());
    ASSERT_FALSE(acc->Commit({}, main.db_acc).HasError());
  }

  {
    auto acc = replica.db.Access();
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    const auto labels = v->Labels(View::OLD);
    ASSERT_TRUE(labels.HasValue());
    ASSERT_EQ(labels->size(), 1);
    ASSERT_THAT(*labels, UnorderedElementsAre(replica.db.storage()->NameToLabel(vertex_label)));
    const auto properties = v->Properties(View::OLD);
    ASSERT_TRUE(properties.HasValue());
    ASSERT_EQ(properties->size(), 1);
    ASSERT_THAT(*properties, UnorderedElementsAre(std::make_pair(replica.db.storage()->NameToProperty(vertex_property),
                                                                 PropertyValue(vertex_property_value))));

    ASSERT_FALSE(acc->Commit().HasError());
  }

  // vertex remove label
  {
    auto acc = main.db.Access();
    auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    ASSERT_TRUE(v->RemoveLabel(main.db.storage()->NameToLabel(vertex_label)).HasValue());
    ASSERT_FALSE(acc->Commit({}, main.db_acc).HasError());
  }

  {
    auto acc = replica.db.Access();
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    const auto labels = v->Labels(View::OLD);
    ASSERT_TRUE(labels.HasValue());
    ASSERT_EQ(labels->size(), 0);
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // vertex delete
  {
    auto acc = main.db.Access();
    auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    ASSERT_TRUE(acc->DeleteVertex(&*v).HasValue());
    ASSERT_FALSE(acc->Commit({}, main.db_acc).HasError());
  }

  {
    auto acc = replica.db.Access();
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
    auto acc = main.db.Access();
    auto v = acc->CreateVertex();
    vertex_gid.emplace(v.Gid());
    auto edgeRes = acc->CreateEdge(&v, &v, main.db.storage()->NameToEdgeType(edge_type));
    ASSERT_TRUE(edgeRes.HasValue());
    auto edge = edgeRes.GetValue();
    ASSERT_TRUE(edge.SetProperty(main.db.storage()->NameToProperty(edge_property), PropertyValue(edge_property_value))
                    .HasValue());
    edge_gid.emplace(edge.Gid());
    ASSERT_FALSE(acc->Commit({}, main.db_acc).HasError());
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
    auto acc = replica.db.Access();
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    const auto out_edges = v->OutEdges(View::OLD);
    ASSERT_TRUE(out_edges.HasValue());
    const auto edge = find_edge(out_edges->edges, *edge_gid);
    ASSERT_EQ(edge->EdgeType(), replica.db.storage()->NameToEdgeType(edge_type));
    const auto properties = edge->Properties(View::OLD);
    ASSERT_TRUE(properties.HasValue());
    ASSERT_EQ(properties->size(), 1);
    ASSERT_THAT(*properties, UnorderedElementsAre(std::make_pair(replica.db.storage()->NameToProperty(edge_property),
                                                                 PropertyValue(edge_property_value))));
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // delete edge
  {
    auto acc = main.db.Access();
    auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    auto out_edges = v->OutEdges(View::OLD);
    auto edge = find_edge(out_edges->edges, *edge_gid);
    ASSERT_TRUE(edge);
    ASSERT_TRUE(acc->DeleteEdge(&*edge).HasValue());
    ASSERT_FALSE(acc->Commit({}, main.db_acc).HasError());
  }

  {
    auto acc = replica.db.Access();
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
    auto unique_acc = main.db.UniqueAccess();
    ASSERT_FALSE(unique_acc->CreateIndex(main.db.storage()->NameToLabel(label)).HasError());
    ASSERT_FALSE(unique_acc->Commit({}, main.db_acc).HasError());
  }
  {
    auto unique_acc = main.db.UniqueAccess();
    unique_acc->SetIndexStats(main.db.storage()->NameToLabel(label), l_stats);
    ASSERT_FALSE(unique_acc->Commit({}, main.db_acc).HasError());
  }
  {
    auto unique_acc = main.db.ReadOnlyAccess();
    ASSERT_FALSE(
        unique_acc->CreateIndex(main.db.storage()->NameToLabel(label), {main.db.storage()->NameToProperty(property)})
            .HasError());
    ASSERT_FALSE(unique_acc->Commit({}, main.db_acc).HasError());
  }
  {
    auto unique_acc = main.db.ReadOnlyAccess();
    ASSERT_FALSE(
        unique_acc
            ->CreateIndex(main.db.storage()->NameToLabel(label), {main.db.storage()->NameToProperty(property),
                                                                  main.db.storage()->NameToProperty(property_extra)})
            .HasError());
    ASSERT_FALSE(unique_acc->Commit({}, main.db_acc).HasError());
  }
  {
    auto unique_acc = main.db.UniqueAccess();
    unique_acc->SetIndexStats(main.db.storage()->NameToLabel(label),
                              std::array{main.db.storage()->NameToProperty(property)}, lp_stats);
    ASSERT_FALSE(unique_acc->Commit({}, main.db_acc).HasError());
  }
  {
    auto unique_acc = main.db.UniqueAccess();
    unique_acc->SetIndexStats(
        main.db.storage()->NameToLabel(label),
        std::array{main.db.storage()->NameToProperty(property), main.db.storage()->NameToProperty(property_extra)},
        lp_stats);
    ASSERT_FALSE(unique_acc->Commit({}, main.db_acc).HasError());
  }
  {
    auto unique_acc = main.db.UniqueAccess();
    ASSERT_FALSE(unique_acc
                     ->CreateExistenceConstraint(main.db.storage()->NameToLabel(label),
                                                 main.db.storage()->NameToProperty(property))
                     .HasError());
    ASSERT_FALSE(unique_acc->Commit({}, main.db_acc).HasError());
  }
  {
    auto unique_acc = main.db.UniqueAccess();
    ASSERT_FALSE(unique_acc
                     ->CreateUniqueConstraint(main.db.storage()->NameToLabel(label),
                                              {main.db.storage()->NameToProperty(property),
                                               main.db.storage()->NameToProperty(property_extra)})
                     .HasError());
    ASSERT_FALSE(unique_acc->Commit({}, main.db_acc).HasError());
  }

  {
    const auto indices = replica.db.Access()->ListAllIndices();
    ASSERT_THAT(indices.label, UnorderedElementsAre(replica.db.storage()->NameToLabel(label)));
    ASSERT_THAT(
        indices.label_properties,
        UnorderedElementsAre(std::make_pair(replica.db.storage()->NameToLabel(label),
                                            std::vector{replica.db.storage()->NameToProperty(property)}),
                             std::make_pair(replica.db.storage()->NameToLabel(label),
                                            std::vector{replica.db.storage()->NameToProperty(property),
                                                        replica.db.storage()->NameToProperty(property_extra)})));
    const auto &l_stats_rep = replica.db.Access()->GetIndexStats(replica.db.storage()->NameToLabel(label));
    ASSERT_TRUE(l_stats_rep);
    ASSERT_EQ(l_stats_rep->count, l_stats.count);
    ASSERT_EQ(l_stats_rep->avg_degree, l_stats.avg_degree);
    const auto &lp_stats_rep = replica.db.Access()->GetIndexStats(
        replica.db.storage()->NameToLabel(label), std::array{replica.db.storage()->NameToProperty(property)});
    ASSERT_TRUE(lp_stats_rep);
    ASSERT_EQ(lp_stats_rep->count, lp_stats.count);
    ASSERT_EQ(lp_stats_rep->distinct_values_count, lp_stats.distinct_values_count);
    ASSERT_EQ(lp_stats_rep->statistic, lp_stats.statistic);
    ASSERT_EQ(lp_stats_rep->avg_group_size, lp_stats.avg_group_size);
    ASSERT_EQ(lp_stats_rep->avg_degree, lp_stats.avg_degree);

    const auto &lps_stats_rep = replica.db.Access()->GetIndexStats(
        replica.db.storage()->NameToLabel(label), std::array{
                                                      replica.db.storage()->NameToProperty(property),
                                                      replica.db.storage()->NameToProperty(property_extra),
                                                  });
    ASSERT_TRUE(lps_stats_rep);
    ASSERT_EQ(lps_stats_rep->count, lp_stats.count);
    ASSERT_EQ(lps_stats_rep->distinct_values_count, lp_stats.distinct_values_count);
    ASSERT_EQ(lps_stats_rep->statistic, lp_stats.statistic);
    ASSERT_EQ(lps_stats_rep->avg_group_size, lp_stats.avg_group_size);
    ASSERT_EQ(lps_stats_rep->avg_degree, lp_stats.avg_degree);

    const auto constraints = replica.db.Access()->ListAllConstraints();
    ASSERT_THAT(constraints.existence,
                UnorderedElementsAre(std::make_pair(replica.db.storage()->NameToLabel(label),
                                                    replica.db.storage()->NameToProperty(property))));
    ASSERT_THAT(constraints.unique,
                UnorderedElementsAre(std::make_pair(replica.db.storage()->NameToLabel(label),
                                                    std::set{replica.db.storage()->NameToProperty(property),
                                                             replica.db.storage()->NameToProperty(property_extra)})));
  }

  // label index drop
  // label property index drop
  // existence constraint drop
  // unique constriant drop
  {
    auto unique_acc = main.db.UniqueAccess();
    unique_acc->DeleteLabelIndexStats(main.db.storage()->NameToLabel(label));
    ASSERT_FALSE(unique_acc->Commit({}, main.db_acc).HasError());
  }
  {
    auto unique_acc = main.db.UniqueAccess();
    ASSERT_FALSE(unique_acc->DropIndex(main.db.storage()->NameToLabel(label)).HasError());
    ASSERT_FALSE(unique_acc->Commit({}, main.db_acc).HasError());
  }
  {
    auto unique_acc = main.db.UniqueAccess();
    unique_acc->DeleteLabelPropertyIndexStats(main.db.storage()->NameToLabel(label));
    ASSERT_FALSE(unique_acc->Commit({}, main.db_acc).HasError());
  }
  {
    auto unique_acc = main.db.ReadOnlyAccess();
    ASSERT_FALSE(
        unique_acc->DropIndex(main.db.storage()->NameToLabel(label), {main.db.storage()->NameToProperty(property)})
            .HasError());
    ASSERT_FALSE(unique_acc->Commit({}, main.db_acc).HasError());
  }
  {
    auto unique_acc = main.db.ReadOnlyAccess();
    ASSERT_FALSE(
        unique_acc
            ->DropIndex(main.db.storage()->NameToLabel(label), {main.db.storage()->NameToProperty(property),
                                                                main.db.storage()->NameToProperty(property_extra)})
            .HasError());
    ASSERT_FALSE(unique_acc->Commit({}, main.db_acc).HasError());
  }
  {
    auto unique_acc = main.db.UniqueAccess();
    ASSERT_FALSE(unique_acc
                     ->DropExistenceConstraint(main.db.storage()->NameToLabel(label),
                                               main.db.storage()->NameToProperty(property))
                     .HasError());
    ASSERT_FALSE(unique_acc->Commit({}, main.db_acc).HasError());
  }
  {
    auto unique_acc = main.db.UniqueAccess();
    ASSERT_EQ(unique_acc->DropUniqueConstraint(
                  main.db.storage()->NameToLabel(label),
                  {main.db.storage()->NameToProperty(property), main.db.storage()->NameToProperty(property_extra)}),
              memgraph::storage::UniqueConstraints::DeletionStatus::SUCCESS);
    ASSERT_FALSE(unique_acc->Commit({}, main.db_acc).HasError());
  }

  {
    const auto indices = replica.db.Access()->ListAllIndices();
    ASSERT_EQ(indices.label.size(), 0);
    ASSERT_EQ(indices.label_properties.size(), 0);

    const auto &l_stats_rep = replica.db.Access()->GetIndexStats(replica.db.storage()->NameToLabel(label));
    ASSERT_FALSE(l_stats_rep);
    const auto &lp_stats_rep = replica.db.Access()->GetIndexStats(
        replica.db.storage()->NameToLabel(label), std::array{replica.db.storage()->NameToProperty(property)});
    ASSERT_FALSE(lp_stats_rep);

    const auto constraints = replica.db.Access()->ListAllConstraints();
    ASSERT_EQ(constraints.existence.size(), 0);
    ASSERT_EQ(constraints.unique.size(), 0);
  }
}

TEST_F(ReplicationTest, MultipleSynchronousReplicationTest) {
  MinMemgraph main(main_conf);
  MinMemgraph replica1(repl_conf);
  MinMemgraph replica2(repl2_conf);

  replica1.repl_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{
          .repl_server = Endpoint(local_host, ports[0]),
      },
      std::nullopt);
  replica2.repl_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{
          .repl_server = Endpoint(local_host, ports[1]),
      },
      std::nullopt);

  ASSERT_FALSE(main.repl_handler
                   .TryRegisterReplica(ReplicationClientConfig{
                       .name = replicas[0],
                       .mode = ReplicationMode::SYNC,
                       .repl_server_endpoint = Endpoint(local_host, ports[0]),
                   })
                   .HasError());
  ASSERT_FALSE(main.repl_handler
                   .TryRegisterReplica(ReplicationClientConfig{
                       .name = replicas[1],
                       .mode = ReplicationMode::SYNC,
                       .repl_server_endpoint = Endpoint(local_host, ports[1]),
                   })
                   .HasError());

  const auto *vertex_label = "label";
  const auto *vertex_property = "property";
  const auto *vertex_property_value = "property_value";
  std::optional<Gid> vertex_gid;
  {
    auto acc = main.db.Access();
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.AddLabel(main.db.storage()->NameToLabel(vertex_label)).HasValue());
    ASSERT_TRUE(v.SetProperty(main.db.storage()->NameToProperty(vertex_property), PropertyValue(vertex_property_value))
                    .HasValue());
    vertex_gid.emplace(v.Gid());
    ASSERT_FALSE(acc->Commit({}, main.db_acc).HasError());
  }

  const auto check_replica = [&](memgraph::dbms::Database &replica_database) {
    auto acc = replica_database.Access();
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    const auto labels = v->Labels(View::OLD);
    ASSERT_TRUE(labels.HasValue());
    ASSERT_THAT(*labels, UnorderedElementsAre(replica_database.storage()->NameToLabel(vertex_label)));
    ASSERT_FALSE(acc->Commit().HasError());
  };

  check_replica(replica1.db);
  check_replica(replica2.db);

  auto handler = main.repl_handler;
  handler.UnregisterReplica(replicas[1]);
  {
    auto acc = main.db.Access();
    auto v = acc->CreateVertex();
    vertex_gid.emplace(v.Gid());
    ASSERT_FALSE(acc->Commit({}, main.db_acc).HasError());
  }

  // REPLICA1 should contain the new vertex
  {
    auto acc = replica1.db.Access();
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    ASSERT_FALSE(acc->Commit().HasError());
  }

  // REPLICA2 should not contain the new vertex
  {
    auto acc = replica2.db.Access();
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
            .recover_on_startup = true,
            .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_on_exit = true,
        }};
    UpdatePaths(conf, storage_directory);
    MinMemgraph main(conf);

    {
      auto acc = main.db.Access();
      // Create the vertex before registering a replica
      auto v = acc->CreateVertex();
      vertex_gids.emplace_back(v.Gid());
      ASSERT_FALSE(acc->Commit({}, main.db_acc).HasError());
    }
  }

  {
    // Create second WAL
    memgraph::storage::Config conf{
        .durability = {.recover_on_startup = true,
                       .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL}};
    UpdatePaths(conf, storage_directory);
    MinMemgraph main(conf);
    // Create vertices in 2 different transactions
    {
      auto acc = main.db.Access();
      auto v = acc->CreateVertex();
      vertex_gids.emplace_back(v.Gid());
      ASSERT_FALSE(acc->Commit({}, main.db_acc).HasError());
    }
    {
      auto acc = main.db.Access();
      auto v = acc->CreateVertex();
      vertex_gids.emplace_back(v.Gid());
      ASSERT_FALSE(acc->Commit({}, main.db_acc).HasError());
    }
  }

  memgraph::storage::Config conf{
      .durability = {
          .recover_on_startup = true,
          .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
      }};
  UpdatePaths(conf, storage_directory);
  MinMemgraph main(conf);

  static constexpr const auto *property_name = "property_name";
  static constexpr const auto property_value = 1;
  {
    // Force the creation of current WAL file
    auto acc = main.db.Access();
    for (const auto &vertex_gid : vertex_gids) {
      auto v = acc->FindVertex(vertex_gid, View::OLD);
      ASSERT_TRUE(v);
      ASSERT_TRUE(
          v->SetProperty(main.db.storage()->NameToProperty(property_name), PropertyValue(property_value)).HasValue());
    }
    ASSERT_FALSE(acc->Commit({}, main.db_acc).HasError());
  }

  static constexpr const auto *vertex_label = "vertex_label";
  {
    MinMemgraph replica(repl_conf);
    auto replica_store_handler = replica.repl_handler;

    replica_store_handler.TrySetReplicationRoleReplica(
        ReplicationServerConfig{
            .repl_server = Endpoint(local_host, ports[0]),
        },
        std::nullopt);
    ASSERT_FALSE(main.repl_handler
                     .TryRegisterReplica(ReplicationClientConfig{
                         .name = replicas[0],
                         .mode = ReplicationMode::SYNC,
                         .repl_server_endpoint = Endpoint(local_host, ports[0]),
                     })
                     .HasError());

    while (main.db.storage()->GetReplicaState(replicas[0]) != ReplicaState::READY) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    {
      auto acc = main.db.Access();
      for (const auto &vertex_gid : vertex_gids) {
        auto v = acc->FindVertex(vertex_gid, View::OLD);
        ASSERT_TRUE(v);
        ASSERT_TRUE(v->AddLabel(main.db.storage()->NameToLabel(vertex_label)).HasValue());
      }
      ASSERT_FALSE(acc->Commit({}, main.db_acc).HasError());
    }
    {
      auto acc = replica.db.Access();
      for (const auto &vertex_gid : vertex_gids) {
        auto v = acc->FindVertex(vertex_gid, View::OLD);
        ASSERT_TRUE(v);
        const auto labels = v->Labels(View::OLD);
        ASSERT_TRUE(labels.HasValue());
        ASSERT_THAT(*labels, UnorderedElementsAre(replica.db.storage()->NameToLabel(vertex_label)));
        const auto properties = v->Properties(View::OLD);
        ASSERT_TRUE(properties.HasValue());
        ASSERT_THAT(*properties,
                    UnorderedElementsAre(std::make_pair(replica.db.storage()->NameToProperty(property_name),
                                                        PropertyValue(property_value))));
      }
      ASSERT_FALSE(acc->Commit().HasError());
    }
  }
  {
    memgraph::storage::Config repl_conf{
        .durability = {.recover_on_startup = true,
                       .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL}};
    UpdatePaths(repl_conf, repl_storage_directory);
    MinMemgraph replica(repl_conf);
    {
      auto acc = replica.db.Access();
      for (const auto &vertex_gid : vertex_gids) {
        auto v = acc->FindVertex(vertex_gid, View::OLD);
        ASSERT_TRUE(v);
        const auto labels = v->Labels(View::OLD);
        ASSERT_TRUE(labels.HasValue());
        ASSERT_THAT(*labels, UnorderedElementsAre(replica.db.storage()->NameToLabel(vertex_label)));
        const auto properties = v->Properties(View::OLD);
        ASSERT_TRUE(properties.HasValue());
        ASSERT_THAT(*properties,
                    UnorderedElementsAre(std::make_pair(replica.db.storage()->NameToProperty(property_name),
                                                        PropertyValue(property_value))));
      }
      ASSERT_FALSE(acc->Commit().HasError());
    }
  }
}

TEST_F(ReplicationTest, BasicAsynchronousReplicationTest) {
  MinMemgraph main(main_conf);
  MinMemgraph replica_async(repl_conf);

  auto replica_store_handler = replica_async.repl_handler;
  replica_store_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{
          .repl_server = Endpoint(local_host, ports[1]),
      },
      std::nullopt);

  ASSERT_FALSE(main.repl_handler
                   .TryRegisterReplica(ReplicationClientConfig{
                       .name = "REPLICA_ASYNC",
                       .mode = ReplicationMode::ASYNC,
                       .repl_server_endpoint = Endpoint(local_host, ports[1]),
                   })
                   .HasError());

  static constexpr size_t vertices_create_num = 10;
  std::vector<Gid> created_vertices;
  for (size_t i = 0; i < vertices_create_num; ++i) {
    auto acc = main.db.Access();
    auto v = acc->CreateVertex();
    created_vertices.push_back(v.Gid());
    ASSERT_FALSE(acc->Commit({}, main.db_acc).HasError());

    if (i == 0) {
      ASSERT_EQ(main.db.storage()->GetReplicaState("REPLICA_ASYNC"), ReplicaState::REPLICATING);
    } else {
      auto const state = main.db.storage()->GetReplicaState("REPLICA_ASYNC");
      ASSERT_TRUE(state == ReplicaState::RECOVERY || state == ReplicaState::MAYBE_BEHIND);
    }
  }

  while (main.db.storage()->GetReplicaState("REPLICA_ASYNC") != ReplicaState::READY) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(std::all_of(created_vertices.begin(), created_vertices.end(), [&](const auto vertex_gid) {
    auto acc = replica_async.db.Access();
    auto v = acc->FindVertex(vertex_gid, View::OLD);
    const bool exists = v.has_value();
    EXPECT_FALSE(acc->Commit().HasError());
    return exists;
  }));
}

TEST_F(ReplicationTest, EpochTest) {
  MinMemgraph main(main_conf);
  MinMemgraph replica1(repl_conf);

  replica1.repl_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{
          .repl_server = Endpoint(local_host, ports[0]),
      },
      std::nullopt);

  MinMemgraph replica2(repl2_conf);
  replica2.repl_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{
          .repl_server = Endpoint(local_host, 10001),
      },
      std::nullopt);

  ASSERT_FALSE(main.repl_handler
                   .TryRegisterReplica(ReplicationClientConfig{
                       .name = replicas[0],
                       .mode = ReplicationMode::SYNC,
                       .repl_server_endpoint = Endpoint(local_host, ports[0]),
                   })
                   .HasError());

  ASSERT_FALSE(main.repl_handler
                   .TryRegisterReplica(ReplicationClientConfig{
                       .name = replicas[1],
                       .mode = ReplicationMode::SYNC,
                       .repl_server_endpoint = Endpoint(local_host, 10001),
                   })
                   .HasError());

  std::optional<Gid> vertex_gid;
  {
    auto acc = main.db.Access();
    const auto v = acc->CreateVertex();
    vertex_gid.emplace(v.Gid());
    ASSERT_FALSE(acc->Commit({}, main.db_acc).HasError());
  }
  {
    auto acc = replica1.db.Access();
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    ASSERT_FALSE(acc->Commit({}, main.db_acc).HasError());
  }
  {
    auto acc = replica2.db.Access();
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    ASSERT_FALSE(acc->Commit().HasError());
  }

  main.repl_handler.UnregisterReplica(replicas[0]);
  main.repl_handler.UnregisterReplica(replicas[1]);

  ASSERT_TRUE(replica1.repl_handler.SetReplicationRoleMain());

  ASSERT_FALSE(replica1.repl_handler
                   .TryRegisterReplica(ReplicationClientConfig{
                       .name = replicas[1],
                       .mode = ReplicationMode::SYNC,
                       .repl_server_endpoint = Endpoint(local_host, 10001),
                   })
                   .HasError());

  {
    auto acc = main.db.Access();
    acc->CreateVertex();
    ASSERT_FALSE(acc->Commit({}, main.db_acc).HasError());
  }
  {
    auto acc = replica1.db.Access();
    auto v = acc->CreateVertex();
    vertex_gid.emplace(v.Gid());
    ASSERT_FALSE(acc->Commit({}, replica1.db_acc).HasError());
  }
  // Replica1 should forward it's vertex to Replica2
  {
    auto acc = replica2.db.Access();
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    ASSERT_FALSE(acc->Commit().HasError());
  }

  replica1.repl_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{
          .repl_server = Endpoint(local_host, ports[0]),
      },
      std::nullopt);
  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replicas[0],
                      .mode = ReplicationMode::SYNC,
                      .repl_server_endpoint = Endpoint(local_host, ports[0]),
                  })
                  .HasError());

  {
    auto acc = main.db.Access();
    const auto v = acc->CreateVertex();
    vertex_gid.emplace(v.Gid());
    ASSERT_FALSE(acc->Commit({}, main.db_acc).HasError());
  }
  // Replica1 is not compatible with the main so it shouldn't contain
  // it's newest vertex
  {
    auto acc = replica1.db.Access();
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_FALSE(v);
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

TEST_F(ReplicationTest, ReplicationInformation) {
  MinMemgraph main(main_conf);
  MinMemgraph replica1(repl_conf);

  uint16_t replica1_port = 10001;
  replica1.repl_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{
          .repl_server = Endpoint(local_host, replica1_port),
      },
      std::nullopt);

  uint16_t replica2_port = 10002;
  MinMemgraph replica2(repl2_conf);
  replica2.repl_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{
          .repl_server = Endpoint(local_host, replica2_port),
      },
      std::nullopt);

  ASSERT_FALSE(main.repl_handler
                   .TryRegisterReplica(ReplicationClientConfig{
                       .name = replicas[0],
                       .mode = ReplicationMode::SYNC,
                       .repl_server_endpoint = Endpoint(local_host, replica1_port),
                   })
                   .HasError());

  ASSERT_FALSE(main.repl_handler
                   .TryRegisterReplica(ReplicationClientConfig{
                       .name = replicas[1],
                       .mode = ReplicationMode::ASYNC,
                       .repl_server_endpoint = Endpoint(local_host, replica2_port),
                   })
                   .HasError());

  ASSERT_TRUE(main.repl_state->IsMain());
  ASSERT_TRUE(replica1.repl_state->IsReplica());
  ASSERT_TRUE(replica2.repl_state->IsReplica());

  auto const maybe_replicas_info = main.repl_handler.ShowReplicas();
  ASSERT_TRUE(maybe_replicas_info.HasValue());
  auto const &replicas_info = maybe_replicas_info.GetValue();
  ASSERT_EQ(replicas_info.entries_.size(), 2);

  auto const &first_info = replicas_info.entries_[0];
  ASSERT_EQ(first_info.name_, replicas[0]);
  ASSERT_EQ(first_info.sync_mode_, ReplicationMode::SYNC);
  ASSERT_EQ(first_info.socket_address_, fmt::format("{}:{}", local_host, replica1_port));

  auto const &second_info = replicas_info.entries_[1];
  ASSERT_EQ(second_info.name_, replicas[1]);
  ASSERT_EQ(second_info.sync_mode_, ReplicationMode::ASYNC);
  ASSERT_EQ(second_info.socket_address_, fmt::format("{}:{}", local_host, replica2_port));
}

TEST_F(ReplicationTest, ReplicationReplicaWithExistingName) {
  MinMemgraph main(main_conf);
  MinMemgraph replica1(repl_conf);

  uint16_t replica1_port = 10001;
  replica1.repl_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{
          .repl_server = Endpoint(local_host, replica1_port),
      },
      std::nullopt);

  uint16_t replica2_port = 10002;
  MinMemgraph replica2(repl2_conf);
  replica2.repl_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{
          .repl_server = Endpoint(local_host, replica2_port),
      },
      std::nullopt);
  ASSERT_FALSE(main.repl_handler
                   .TryRegisterReplica(ReplicationClientConfig{
                       .name = replicas[0],
                       .mode = ReplicationMode::SYNC,
                       .repl_server_endpoint = Endpoint(local_host, replica1_port),
                   })
                   .HasError());

  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replicas[0],
                      .mode = ReplicationMode::ASYNC,
                      .repl_server_endpoint = Endpoint(local_host, replica2_port),
                  })
                  .GetError() == RegisterReplicaError::NAME_EXISTS);
}

TEST_F(ReplicationTest, ReplicationReplicaWithExistingEndPoint) {
  uint16_t common_port = 10001;

  MinMemgraph main(main_conf);
  MinMemgraph replica1(repl_conf);
  replica1.repl_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{
          .repl_server = Endpoint(local_host, common_port),
      },
      std::nullopt);

  MinMemgraph replica2(repl2_conf);
  replica2.repl_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{
          .repl_server = Endpoint(local_host, common_port),
      },
      std::nullopt);

  ASSERT_FALSE(main.repl_handler
                   .TryRegisterReplica(ReplicationClientConfig{
                       .name = replicas[0],
                       .mode = ReplicationMode::SYNC,
                       .repl_server_endpoint = Endpoint(local_host, common_port),
                   })
                   .HasError());

  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replicas[1],
                      .mode = ReplicationMode::ASYNC,
                      .repl_server_endpoint = Endpoint(local_host, common_port),
                  })
                  .GetError() == RegisterReplicaError::ENDPOINT_EXISTS);
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

  UpdatePaths(replica1_config, replica1_storage_directory);
  UpdatePaths(replica2_config, replica2_storage_directory);

  std::optional<MinMemgraph> main(main_config);
  MinMemgraph replica1(replica1_config);

  replica1.repl_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{
          .repl_server = Endpoint(local_host, ports[0]),
      },
      std::nullopt);

  MinMemgraph replica2(replica2_config);
  replica2.repl_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{
          .repl_server = Endpoint(local_host, ports[1]),
      },
      std::nullopt);

  auto res = main->repl_handler.TryRegisterReplica(ReplicationClientConfig{
      .name = replicas[0],
      .mode = ReplicationMode::SYNC,
      .repl_server_endpoint = Endpoint(local_host, ports[0]),
  });
  ASSERT_FALSE(res.HasError()) << (int)res.GetError();
  res = main->repl_handler.TryRegisterReplica(ReplicationClientConfig{
      .name = replicas[1],
      .mode = ReplicationMode::SYNC,
      .repl_server_endpoint = Endpoint(local_host, ports[1]),
  });
  ASSERT_FALSE(res.HasError()) << (int)res.GetError();

  {
    auto const maybe_replicas_info = main->repl_handler.ShowReplicas();
    ASSERT_TRUE(maybe_replicas_info.HasValue());
    auto const &replicas_info = maybe_replicas_info.GetValue();
    ASSERT_EQ(replicas_info.entries_.size(), 2);

    ASSERT_EQ(replicas_info.entries_[0].name_, replicas[0]);
    ASSERT_EQ(replicas_info.entries_[0].socket_address_, fmt::format("{}:{}", local_host, ports[0]));
    ASSERT_EQ(replicas_info.entries_[1].name_, replicas[1]);
    ASSERT_EQ(replicas_info.entries_[1].socket_address_, fmt::format("{}:{}", local_host, ports[1]));
  }

  main.reset();

  {
    MinMemgraph other_main(main_config);
    auto const maybe_replicas_info = other_main.repl_handler.ShowReplicas();
    ASSERT_TRUE(maybe_replicas_info.HasValue());
    auto const &replicas_info = maybe_replicas_info.GetValue();

    ASSERT_EQ(replicas_info.entries_.size(), 2);
    ASSERT_EQ(replicas_info.entries_[0].name_, replicas[0]);
    ASSERT_EQ(replicas_info.entries_[0].socket_address_, fmt::format("{}:{}", local_host, ports[0]));
    ASSERT_EQ(replicas_info.entries_[1].name_, replicas[1]);
    ASSERT_EQ(replicas_info.entries_[1].socket_address_, fmt::format("{}:{}", local_host, ports[1]));
  }
}

TEST_F(ReplicationTest, RestoringReplicationAtStartup) {
  auto main_config = main_conf;
  main_config.durability.restore_replication_state_on_startup = true;

  std::optional<MinMemgraph> main(main_config);
  MinMemgraph replica1(repl_conf);

  replica1.repl_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{
          .repl_server = Endpoint(local_host, ports[0]),
      },
      std::nullopt);

  MinMemgraph replica2(repl2_conf);

  replica2.repl_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{
          .repl_server = Endpoint(local_host, ports[1]),
      },
      std::nullopt);
  auto res = main->repl_handler.TryRegisterReplica(ReplicationClientConfig{
      .name = replicas[0],
      .mode = ReplicationMode::SYNC,
      .repl_server_endpoint = Endpoint(local_host, ports[0]),
  });
  ASSERT_FALSE(res.HasError());
  res = main->repl_handler.TryRegisterReplica(ReplicationClientConfig{
      .name = replicas[1],
      .mode = ReplicationMode::SYNC,
      .repl_server_endpoint = Endpoint(local_host, ports[1]),
  });
  ASSERT_FALSE(res.HasError());

  {
    auto const maybe_replicas_info = main->repl_handler.ShowReplicas();
    ASSERT_TRUE(maybe_replicas_info.HasValue());
    auto const &replicas_info = maybe_replicas_info.GetValue();

    ASSERT_EQ(replicas_info.entries_.size(), 2);
    ASSERT_EQ(replicas_info.entries_[0].name_, replicas[0]);
    ASSERT_EQ(replicas_info.entries_[0].socket_address_, fmt::format("{}:{}", local_host, ports[0]));
    ASSERT_EQ(replicas_info.entries_[1].name_, replicas[1]);
    ASSERT_EQ(replicas_info.entries_[1].socket_address_, fmt::format("{}:{}", local_host, ports[1]));
  }

  auto handler = main->repl_handler;
  const auto unregister_res = handler.UnregisterReplica(replicas[0]);
  ASSERT_EQ(unregister_res, UnregisterReplicaResult::SUCCESS);

  {
    auto const maybe_replicas_info = main->repl_handler.ShowReplicas();
    ASSERT_TRUE(maybe_replicas_info.HasValue());
    auto const &replicas_info = maybe_replicas_info.GetValue();

    ASSERT_EQ(replicas_info.entries_.size(), 1);
    ASSERT_EQ(replicas_info.entries_[0].name_, replicas[1]);
    ASSERT_EQ(replicas_info.entries_[0].socket_address_, fmt::format("{}:{}", local_host, ports[1]));
  }

  main.reset();

  {
    MinMemgraph other_main(main_config);
    auto const maybe_replicas_info = other_main.repl_handler.ShowReplicas();
    ASSERT_TRUE(maybe_replicas_info.HasValue());
    auto const &replicas_info = maybe_replicas_info.GetValue();

    ASSERT_EQ(replicas_info.entries_.size(), 1);
    ASSERT_EQ(replicas_info.entries_[0].name_, replicas[1]);
    ASSERT_EQ(replicas_info.entries_[0].socket_address_, fmt::format("{}:{}", local_host, ports[1]));
  }
}

TEST_F(ReplicationTest, AddingInvalidReplica) {
  MinMemgraph main(main_conf);

  ASSERT_TRUE(
      main.repl_handler
          .TryRegisterReplica(ReplicationClientConfig{
              .name = "REPLICA", .mode = ReplicationMode::SYNC, .repl_server_endpoint = Endpoint(local_host, ports[0])})
          .GetError() == RegisterReplicaError::ERROR_ACCEPTING_MAIN);
}

TEST_F(ReplicationTest, RecoverySteps) {
  auto config = main_conf;
  config.durability.recover_on_startup = true;
  config.durability.wal_file_size_kibibytes = 1;   // Easy way to control when a new WAL is created
  config.durability.snapshot_retention_count = 3;  // Easy way to control when to clean WALs
  std::optional<MinMemgraph> main(config);
  auto *in_mem = static_cast<InMemoryStorage *>(main->db.storage());

  auto p = in_mem->NameToProperty("p1");
  const auto large_property = PropertyValue{PropertyValue::list_t{1024 / sizeof(int64_t), PropertyValue{int64_t{}}}};

  // Dummy file retained; not testing concurrency, just recovery steps generation
  memgraph::utils::FileRetainer file_retainer;
  auto file_locker = file_retainer.AddLocker();

  auto large_write_to_finalize_wal = [&]() {
    auto acc = in_mem->Access();
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.SetProperty(p, large_property).HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  };

  auto create_vertex_and_commit = [&]() {
    auto acc = in_mem->Access();
    acc->CreateVertex();
    ASSERT_FALSE(acc->Commit().HasError());
  };

  // Nothing
  {
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 0);
  }

  // Only Current
  {
    create_vertex_and_commit();
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 1);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryCurrentWal>(recovery_steps[0]));
  }

  // Only a single WAL
  {
    // Create a vertex with a property large enough to trigger WAL finalization and closing
    // Current is generated on the next transaction
    large_write_to_finalize_wal();
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 1);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryWals>(recovery_steps[0]));
  }

  // Multiple WALs
  {
    // Create a vertex with a property large enough to trigger WAL finalization and closing
    // Current is generated on the next transaction
    large_write_to_finalize_wal();
    large_write_to_finalize_wal();
    large_write_to_finalize_wal();
    large_write_to_finalize_wal();
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 1);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryWals>(recovery_steps[0]));
  }

  // WALs + Current
  {
    // A new current WAL is created on the next transaction after the previous one has been finalized
    create_vertex_and_commit();
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 2);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryWals>(recovery_steps[0]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryCurrentWal>(recovery_steps[1]));
  }

  // Snapshot (with dirty WALs)
  {
    large_write_to_finalize_wal();
    ASSERT_FALSE(in_mem->CreateSnapshot(memgraph::replication_coordination_glue::ReplicationRole::MAIN).HasError());
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 1);
    // TODO Currently we prefer WALs over Snapshots when creating the recovery plan
    // This is an inefficiency when the snapshot is smaller than the WALs we would send
    // Calculate how large the two payloads would be and pick the smaller plan
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryWals>(recovery_steps[0]));
  }

  // Only snapshot (without dirty WALs)
  {
    // Once we are over the allowed number of snapshots, we clean both snapshots and wals
    // Have to make a change to the db so the snapshot doesn't get aborted (to bypass SnapshotDigest)
    create_vertex_and_commit();
    ASSERT_FALSE(in_mem->CreateSnapshot(memgraph::replication_coordination_glue::ReplicationRole::MAIN).HasError());
    create_vertex_and_commit();
    ASSERT_FALSE(in_mem->CreateSnapshot(memgraph::replication_coordination_glue::ReplicationRole::MAIN).HasError());
    create_vertex_and_commit();
    ASSERT_FALSE(in_mem->CreateSnapshot(memgraph::replication_coordination_glue::ReplicationRole::MAIN).HasError());
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 1);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoverySnapshot>(recovery_steps[0]));
  }

  // Snapshot + Current
  {
    auto acc = in_mem->Access();
    acc->CreateVertex();
    ASSERT_FALSE(acc->Commit().HasError());
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 2);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoverySnapshot>(recovery_steps[0]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryCurrentWal>(recovery_steps[1]));
  }

  // Snapshot + WALs (chain starts before snapshot)
  {
    large_write_to_finalize_wal();
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 2);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoverySnapshot>(recovery_steps[0]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryWals>(recovery_steps[1]));
  }

  // Snapshot + WALs + Current
  {
    auto acc = in_mem->Access();
    acc->CreateVertex();
    ASSERT_FALSE(acc->Commit().HasError());
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 3);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoverySnapshot>(recovery_steps[0]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryWals>(recovery_steps[1]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryCurrentWal>(recovery_steps[2]));
  }

  // Snapshot + WALs (chain starts after the snapshot)
  // Restart memgraph
  // Recover only from a snapshot
  // Create a new WAL chain (should start from sequence number 0)
  std::error_code ec;
  // remove all wals
  for (const auto &entry : std::filesystem::directory_iterator(
           in_mem->config_.durability.storage_directory / memgraph::storage::durability::kWalDirectory, ec)) {
    std::filesystem::remove_all(entry, ec);
    ASSERT_FALSE(ec);
  }
  // remove all but the last snapshot
  std::optional<std::filesystem::path> newest_snapshot{};
  // file clock has an unspecified epoch; this way we don't have to think about it
  std::filesystem::file_time_type newest_write_time = std::chrono::file_clock::now() - std::chrono::years{10};
  for (const auto &snapshot : std::filesystem::directory_iterator(in_mem->config_.durability.storage_directory /
                                                                  memgraph::storage::durability::kSnapshotDirectory)) {
    if (std::filesystem::is_regular_file(snapshot.status())) {
      auto last_write_time = std::filesystem::last_write_time(snapshot);
      if (last_write_time > newest_write_time) {  // Newer file; delete the previous file
        newest_write_time = last_write_time;
        if (newest_snapshot) {
          std::filesystem::remove(*newest_snapshot, ec);
          ASSERT_FALSE(ec);
        }
        newest_snapshot = snapshot.path();
      } else {  // Delete this file
        std::filesystem::remove(snapshot.path(), ec);
        ASSERT_FALSE(ec);
      }
    }
  }
  // restart Memgraph
  main.reset();
  main.emplace(config);
  in_mem = static_cast<InMemoryStorage *>(main->db.storage());
  {
    // On start we only have the snapshot to send
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 1);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoverySnapshot>(recovery_steps[0]));
  }
  {
    // Add current wal
    auto acc = in_mem->Access();
    acc->CreateVertex();
    ASSERT_FALSE(acc->Commit().HasError());
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 2);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoverySnapshot>(recovery_steps[0]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryCurrentWal>(recovery_steps[1]));
  }
  {
    // Add finalized wal
    large_write_to_finalize_wal();
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 2);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoverySnapshot>(recovery_steps[0]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryWals>(recovery_steps[1]));
  }
  {
    // Add both
    auto acc = in_mem->Access();
    acc->CreateVertex();
    ASSERT_FALSE(acc->Commit().HasError());
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 3);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoverySnapshot>(recovery_steps[0]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryWals>(recovery_steps[1]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryCurrentWal>(recovery_steps[2]));
  }

  // Snapshot + WALs (broken chain)
  // Create a couple of WAL files
  // Create a single snapshot
  // Create more WALs
  // Break the WAL chain somewhere before the snapshot
  {
    large_write_to_finalize_wal();
    large_write_to_finalize_wal();
    std::filesystem::path wal_file;
    std::filesystem::file_time_type newest_write_time = std::chrono::file_clock::now() - std::chrono::years{10};
    for (const auto &wal : std::filesystem::directory_iterator(in_mem->config_.durability.storage_directory /
                                                               memgraph::storage::durability::kWalDirectory)) {
      if (std::filesystem::is_regular_file(wal.status())) {
        auto last_write_time = std::filesystem::last_write_time(wal);
        if (last_write_time > newest_write_time) {
          newest_write_time = last_write_time;
          wal_file = wal.path();
        }
      }
    }
    large_write_to_finalize_wal();
    ASSERT_FALSE(in_mem->CreateSnapshot(memgraph::replication_coordination_glue::ReplicationRole::MAIN).HasError());
    large_write_to_finalize_wal();
    large_write_to_finalize_wal();
    large_write_to_finalize_wal();
    std::error_code ec;
    std::filesystem::remove(wal_file, ec);
    ASSERT_FALSE(ec);
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 2);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoverySnapshot>(recovery_steps[0]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryWals>(recovery_steps[1]));
  }
  // + Current
  {
    auto acc = in_mem->Access();
    acc->CreateVertex();
    ASSERT_FALSE(acc->Commit().HasError());
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 3);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoverySnapshot>(recovery_steps[0]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryWals>(recovery_steps[1]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryCurrentWal>(recovery_steps[2]));
  }
}

TEST_F(ReplicationTest, SchemaReplication) {
  memgraph::storage::Config conf{
      .durability =
          {
              .recover_on_startup = true,
              .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
              .snapshot_retention_count = 1,
              .restore_replication_state_on_startup = true,
          },
      .salient.items =
          {
              .properties_on_edges = true,
              .enable_schema_info = true,
          },
  };

  auto repl_conf = conf;
  UpdatePaths(conf, storage_directory);
  std::optional<MinMemgraph> main(conf);

  repl_conf.durability.recover_on_startup = false;
  UpdatePaths(repl_conf, repl_storage_directory);
  std::optional<MinMemgraph> replica(repl_conf);

  replica->repl_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{.repl_server = Endpoint(local_host, ports[0])}, std::nullopt);

  const auto &reg = main->repl_handler.TryRegisterReplica(ReplicationClientConfig{
      .name = "REPLICA",
      .mode = ReplicationMode::SYNC,
      .repl_server_endpoint = Endpoint(local_host, ports[0]),
  });
  ASSERT_FALSE(reg.HasError()) << (int)reg.GetError();

  auto get_schema = [](auto &instance) {
    return instance.db.storage()->schema_info_.ToJson(*instance.db.storage()->name_id_mapper_,
                                                      instance.db.storage()->enum_store_);
  };

  auto l1 = main->db.storage()->NameToLabel("L1");
  auto l2 = main->db.storage()->NameToLabel("L2");
  auto l3 = main->db.storage()->NameToLabel("L3");
  auto p1 = main->db.storage()->NameToProperty("p1");
  auto p2 = main->db.storage()->NameToProperty("p2");
  auto e = main->db.storage()->NameToEdgeType("E");

  // Check current delta replication
  {
    auto acc = main->db.Access();
    acc->CreateVertex();
    ASSERT_FALSE(acc->Commit({}, main->db_acc).HasError());
    EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
  }

  {
    auto acc = main->db.Access();
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.AddLabel(l1).HasValue());
    ASSERT_TRUE(v.AddLabel(l2).HasValue());
    ASSERT_TRUE(v.AddLabel(l3).HasValue());
    ASSERT_FALSE(acc->Commit({}, main->db_acc).HasError());
    EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
  }

  {
    auto acc = main->db.Access();
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.SetProperty(p1, PropertyValue{123}).HasValue());
    ASSERT_TRUE(v.SetProperty(p1, PropertyValue{123.45}).HasValue());
    ASSERT_FALSE(acc->Commit({}, main->db_acc).HasError());
    EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
  }

  {
    auto acc = main->db.Access();
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.SetProperty(p1, PropertyValue{true}).HasValue());
    ASSERT_TRUE(v.AddLabel(l3).HasValue());
    ASSERT_FALSE(acc->Commit({}, main->db_acc).HasError());
    EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
  }

  {
    auto acc = main->db.Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    ASSERT_TRUE(acc->CreateEdge(&v1, &v2, e).HasValue());
    ASSERT_FALSE(acc->Commit({}, main->db_acc).HasError());
    EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
  }

  {
    auto acc = main->db.Access();
    auto v1 = acc->CreateVertex();
    ASSERT_TRUE(v1.AddLabel(l1).HasValue());
    ASSERT_TRUE(v1.AddLabel(l3).HasValue());
    auto v2 = acc->CreateVertex();
    ASSERT_TRUE(v2.AddLabel(l2).HasValue());
    ASSERT_TRUE(acc->CreateEdge(&v1, &v2, e).HasValue());
    ASSERT_FALSE(acc->Commit({}, main->db_acc).HasError());
    EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
  }

  {
    auto acc = main->db.Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    auto edge = acc->CreateEdge(&v1, &v2, e);
    ASSERT_TRUE(edge->SetProperty(p2, PropertyValue{""}).HasValue());
    ASSERT_TRUE(edge->SetProperty(p1, PropertyValue{123}).HasValue());
    ASSERT_FALSE(acc->Commit({}, main->db_acc).HasError());
    EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
  }

  {
    auto acc = main->db.Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    auto edge = acc->CreateEdge(&v1, &v2, e);
    ASSERT_TRUE(edge->SetProperty(p2, PropertyValue{""}).HasValue());
    ASSERT_TRUE(edge->SetProperty(p1, PropertyValue{123}).HasValue());
    ASSERT_TRUE(v2.AddLabel(l2).HasValue());
    ASSERT_TRUE(v1.AddLabel(l2).HasValue());
    ASSERT_FALSE(acc->Commit({}, main->db_acc).HasError());
    EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
  }

  {
    auto acc = main->db.Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    auto edge = acc->CreateEdge(&v1, &v2, e);
    const auto v1_gid = v1.Gid();
    const auto v2_gid = v2.Gid();
    const auto edge_gid = edge->Gid();
    ASSERT_FALSE(acc->Commit({}, main->db_acc).HasError());

    auto acc2 = main->db.Access();
    auto prev_v1 = acc2->FindVertex(v1_gid, View::NEW);
    auto prev_v2 = acc2->FindVertex(v2_gid, View::NEW);
    auto prev_edge = acc2->FindEdge(edge_gid, View::NEW);
    ASSERT_TRUE(prev_edge->SetProperty(p2, PropertyValue{""}).HasValue());
    ASSERT_TRUE(prev_edge->SetProperty(p1, PropertyValue{123}).HasValue());
    ASSERT_TRUE(prev_v2->AddLabel(l2).HasValue());
    ASSERT_TRUE(prev_v1->AddLabel(l2).HasValue());
    ASSERT_FALSE(acc2->Commit({}, main->db_acc).HasError());
    EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
  }

  auto stop_replica = [&]() {
    replica.reset();
    {
      int tries = 0;
      while (main->repl_handler.ShowReplicas().GetValue().entries_[0].data_info_.at("memgraph").state_ !=
             ReplicaState::MAYBE_BEHIND) {
        std::this_thread::sleep_for(std::chrono::seconds{1});
        ASSERT_LE(++tries, 20) << "Waited too long for shutdown";
      }
    }
  };

  auto start_replica = [&]() {
    replica.emplace(repl_conf);
    replica->repl_handler.TrySetReplicationRoleReplica(
        ReplicationServerConfig{.repl_server = Endpoint(local_host, ports[0])}, std::nullopt);
    {
      int tries = 0;
      while (main->repl_handler.ShowReplicas().GetValue().entries_[0].data_info_.at("memgraph").state_ !=
             ReplicaState::READY) {
        std::this_thread::sleep_for(std::chrono::seconds{1});
        ASSERT_LE(++tries, 20) << "Waited too long for recovery";
      }
    }
  };

  // Check current wal recovery
  stop_replica();
  start_replica();
  EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica))) << "MAIN:\n"
                                                                     << get_schema(*main) << "\nREPLICA:\n"
                                                                     << get_schema(*replica);

  // Check wal recovery
  // Exiting will finalize the current wal
  main.reset();
  replica.reset();
  conf.durability.snapshot_on_exit = true;  // Allow next restart to test snapshot recovery
  main.emplace(conf);
  start_replica();
  EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));

  // Check snapshot recovery
  main.reset();
  replica.reset();
  std::error_code dummy_ec;
  std::filesystem::remove_all(conf.durability.storage_directory / memgraph::storage::durability::kWalDirectory,
                              dummy_ec);
  main.emplace(conf);  // Important to have a snapshot to recover from
  start_replica();
  EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
}
