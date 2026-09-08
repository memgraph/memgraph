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

#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <fstream>
#include <latch>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <storage/v2/inmemory/storage.hpp>
#include <storage/v2/property_value.hpp>
#include <storage/v2/replication/enums.hpp>
#include "auth/auth.hpp"
#include "dbms/database.hpp"
#include "dbms/database_protector.hpp"
#include "dbms/dbms_handler.hpp"
#include "memory/db_arena.hpp"
#include "parameters/parameters.hpp"
#include "query/interpreter_context.hpp"
#include "replication/config.hpp"
#include "replication/state.hpp"
#include "replication_handler/replication_handler.hpp"
#include "slk/streams.hpp"
#include "storage/v2/commit_probe.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/inmemory/replication/recovery.hpp"
#include "storage/v2/replication/recovery.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "tests/unit/replication_min_memgraph.hpp"
#include "tests/unit/replication_process_fixture.hpp"
#include "tests/unit/storage_test_utils.hpp"
#include "utils/exceptions.hpp"

using testing::UnorderedElementsAre;

using memgraph::io::network::Endpoint;
using memgraph::query::RegisterReplicaError;
using memgraph::query::UnregisterReplicaResult;
using memgraph::replication::ReplicationClientConfig;
using memgraph::replication::ReplicationHandler;
using memgraph::replication::ReplicationServerConfig;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::storage::Config;
using memgraph::storage::EdgeAccessor;
using memgraph::storage::Gid;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::PropertyValue;
using memgraph::storage::View;

using memgraph::tests::MakeCommitArgs;
using memgraph::tests::MinMemgraph;

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
                .root_data_directory = storage_directory,
                .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,

            },
        .salient.items = {.properties_on_edges = true},
        .register_metrics = false,
    };
    UpdatePaths(config, storage_directory);
    return config;
  }();
  Config repl_conf = [&] {
    Config config{
        .durability =
            {
                .root_data_directory = repl_storage_directory,
                .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,

            },
        .salient.items = {.properties_on_edges = true},
        .register_metrics = false,
    };
    UpdatePaths(config, repl_storage_directory);
    return config;
  }();
  Config repl2_conf = [&] {
    Config config{
        .durability =
            {
                .root_data_directory = repl2_storage_directory,
                .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,

            },
        .salient.items = {.properties_on_edges = true},
        .register_metrics = false,
    };
    UpdatePaths(config, repl2_storage_directory);
    return config;
  }();

  const std::string local_host = ("127.0.0.1");
  const std::array<uint16_t, 2> ports{10'000, 20'000};
  const std::array<std::string, 2> replicas = {"REPLICA1", "REPLICA2"};

 private:
  void Clear() {
    if (std::filesystem::exists(storage_directory)) std::filesystem::remove_all(storage_directory);
    if (std::filesystem::exists(repl_storage_directory)) std::filesystem::remove_all(repl_storage_directory);
    if (std::filesystem::exists(repl2_storage_directory)) std::filesystem::remove_all(repl2_storage_directory);
  }
};

TEST_F(ReplicationTest, BasicSynchronousReplicationTest) {
  MinMemgraph main(main_conf);
  MinMemgraph replica(repl_conf);

  auto replica_store_handler = replica.repl_handler;
  replica_store_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{.repl_server = Endpoint(local_host, ports[0])});

  const auto &reg = main.repl_handler.TryRegisterReplica(ReplicationClientConfig{
      .name = "REPLICA",
      .mode = ReplicationMode::SYNC,
      .repl_server_endpoint = Endpoint(local_host, ports[0]),
  });
  ASSERT_TRUE(reg.has_value()) << (int)reg.error();

  // vertex create
  // vertex add label
  // vertex set property
  const auto *vertex_label = "vertex_label";
  const auto *vertex_property = "vertex_property";
  const auto *vertex_property_value = "vertex_property_value";
  std::optional<Gid> vertex_gid;
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    vertex_gid.emplace(v.Gid());
    ASSERT_TRUE(v.AddLabel(main.db.storage()->NameToLabel(vertex_label)).has_value());
    ASSERT_TRUE(v.SetProperty(main.db.storage()->NameToProperty(vertex_property), PropertyValue(vertex_property_value))
                    .has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }

  {
    const memgraph::memory::DbArenaScope arena_scope{&replica.db.Arena()};
    auto acc = replica.db.Access(memgraph::storage::WRITE);
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    const auto labels = v->Labels(View::OLD);
    ASSERT_TRUE(labels.has_value());
    ASSERT_EQ(labels->size(), 1);
    ASSERT_THAT(*labels, UnorderedElementsAre(replica.db.storage()->NameToLabel(vertex_label)));
    const auto properties = v->Properties(View::OLD);
    ASSERT_TRUE(properties.has_value());
    ASSERT_EQ(properties->size(), 1);
    ASSERT_THAT(*properties,
                UnorderedElementsAre(std::make_pair(replica.db.storage()->NameToProperty(vertex_property),
                                                    PropertyValue(vertex_property_value))));

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // vertex remove label
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    ASSERT_TRUE(v->RemoveLabel(main.db.storage()->NameToLabel(vertex_label)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }

  {
    const memgraph::memory::DbArenaScope arena_scope{&replica.db.Arena()};
    auto acc = replica.db.Access(memgraph::storage::WRITE);
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    const auto labels = v->Labels(View::OLD);
    ASSERT_TRUE(labels.has_value());
    ASSERT_EQ(labels->size(), 0);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // vertex delete
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    ASSERT_TRUE(acc->DeleteVertex(&*v).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }

  {
    const memgraph::memory::DbArenaScope arena_scope{&replica.db.Arena()};
    auto acc = replica.db.Access(memgraph::storage::WRITE);
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_FALSE(v);
    vertex_gid.reset();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // edge create
  // edge set property
  const auto *edge_type = "edge_type";
  const auto *edge_property = "edge_property";
  const auto *edge_property_value = "edge_property_value";
  std::optional<Gid> edge_gid;
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    vertex_gid.emplace(v.Gid());
    auto edgeRes = acc->CreateEdge(&v, &v, main.db.storage()->NameToEdgeType(edge_type));
    ASSERT_TRUE(edgeRes.has_value());
    auto edge = edgeRes.value();
    ASSERT_TRUE(edge.SetProperty(main.db.storage()->NameToProperty(edge_property), PropertyValue(edge_property_value))
                    .has_value());
    edge_gid.emplace(edge.Gid());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
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
    const memgraph::memory::DbArenaScope arena_scope{&replica.db.Arena()};
    auto acc = replica.db.Access(memgraph::storage::WRITE);
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    const auto out_edges = v->OutEdges(View::OLD);
    ASSERT_TRUE(out_edges.has_value());
    const auto edge = find_edge(out_edges->edges, *edge_gid);
    ASSERT_EQ(edge->EdgeType(), replica.db.storage()->NameToEdgeType(edge_type));
    const auto properties = edge->Properties(View::OLD);
    ASSERT_TRUE(properties.has_value());
    ASSERT_EQ(properties->size(), 1);
    ASSERT_THAT(*properties,
                UnorderedElementsAre(std::make_pair(replica.db.storage()->NameToProperty(edge_property),
                                                    PropertyValue(edge_property_value))));
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // delete edge
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    auto out_edges = v->OutEdges(View::OLD);
    auto edge = find_edge(out_edges->edges, *edge_gid);
    ASSERT_TRUE(edge);
    ASSERT_TRUE(acc->DeleteEdge(&*edge).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }

  {
    const memgraph::memory::DbArenaScope arena_scope{&replica.db.Arena()};
    auto acc = replica.db.Access(memgraph::storage::WRITE);
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    const auto out_edges = v->OutEdges(View::OLD);
    ASSERT_TRUE(out_edges.has_value());
    ASSERT_FALSE(find_edge(out_edges->edges, *edge_gid));
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // label index create
  // label property index create
  // existence constraint create
  // unique constriant create
  const auto *label = "label";
  const auto *property = "property";
  const auto *property_extra = "property_extra";
  const auto *nested_property1 = "nested_property1";
  const auto *nested_property2 = "nested_property2";
  const auto *nested_property3 = "nested_property3";
  const memgraph::storage::LabelIndexStats l_stats{12, 34};
  const memgraph::storage::LabelPropertyIndexStats lp_stats{98, 76, 5.4, 3.2, 1.0};

  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto unique_acc = main.db.UniqueAccess();
    ASSERT_TRUE(unique_acc->CreateIndex(main.db.storage()->NameToLabel(label)).has_value());
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto unique_acc = main.db.UniqueAccess();
    unique_acc->SetIndexStats(main.db.storage()->NameToLabel(label), l_stats);
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto unique_acc = main.CreateIndexAccessor();
    ASSERT_FALSE(
        !unique_acc->CreateIndex(main.db.storage()->NameToLabel(label), {main.db.storage()->NameToProperty(property)})
             .has_value());
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto unique_acc = main.CreateIndexAccessor();
    ASSERT_FALSE(!unique_acc
                      ->CreateIndex(main.db.storage()->NameToLabel(label),
                                    {main.db.storage()->NameToProperty(property),
                                     main.db.storage()->NameToProperty(property_extra)})
                      .has_value());
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto unique_acc = main.db.UniqueAccess();
    unique_acc->SetIndexStats(main.db.storage()->NameToLabel(label),
                              std::array{memgraph::storage::PropertyPath{main.db.storage()->NameToProperty(property)}},
                              lp_stats);
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto unique_acc = main.db.UniqueAccess();
    unique_acc->SetIndexStats(
        main.db.storage()->NameToLabel(label),
        std::array{memgraph::storage::PropertyPath{main.db.storage()->NameToProperty(property)},
                   memgraph::storage::PropertyPath{main.db.storage()->NameToProperty(property_extra)}},
        lp_stats);
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    // Create nested index
    auto unique_acc = main.CreateIndexAccessor();
    memgraph::storage::PropertyPath property_path{main.db.storage()->NameToProperty(nested_property1),
                                                  main.db.storage()->NameToProperty(nested_property2),
                                                  main.db.storage()->NameToProperty(nested_property3)};
    ASSERT_TRUE(unique_acc->CreateIndex(main.db.storage()->NameToLabel(label), {property_path}).has_value());
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    // Create nested index stats
    auto unique_acc = main.db.UniqueAccess();
    memgraph::storage::PropertyPath property_path{main.db.storage()->NameToProperty(nested_property1),
                                                  main.db.storage()->NameToProperty(nested_property2),
                                                  main.db.storage()->NameToProperty(nested_property3)};
    unique_acc->SetIndexStats(main.db.storage()->NameToLabel(label), std::array{property_path}, lp_stats);
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto read_only_access = main.db.ReadOnlyAccess();
    ASSERT_TRUE(read_only_access
                    ->CreateExistenceConstraint(main.db.storage()->NameToLabel(label),
                                                main.db.storage()->NameToProperty(property))
                    .has_value());
    ASSERT_TRUE(read_only_access->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto read_only_access = main.db.ReadOnlyAccess();
    ASSERT_TRUE(read_only_access
                    ->CreateUniqueConstraint(main.db.storage()->NameToLabel(label),
                                             {main.db.storage()->NameToProperty(property),
                                              main.db.storage()->NameToProperty(property_extra)})
                    .has_value());
    ASSERT_TRUE(read_only_access->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }

  {
    const memgraph::memory::DbArenaScope arena_scope{&replica.db.Arena()};
    const auto indices = replica.db.Access(memgraph::storage::WRITE)->ListAllIndices();
    ASSERT_THAT(indices.label, UnorderedElementsAre(replica.db.storage()->NameToLabel(label)));
    ASSERT_THAT(
        indices.label_properties,
        UnorderedElementsAre(
            memgraph::storage::LabelPropertyIndexEntry{
                replica.db.storage()->NameToLabel(label),
                std::vector{memgraph::storage::PropertyPath{replica.db.storage()->NameToProperty(property)}}},
            memgraph::storage::LabelPropertyIndexEntry{
                replica.db.storage()->NameToLabel(label),
                std::vector{memgraph::storage::PropertyPath{replica.db.storage()->NameToProperty(property)},
                            memgraph::storage::PropertyPath{replica.db.storage()->NameToProperty(property_extra)}}},
            memgraph::storage::LabelPropertyIndexEntry{
                replica.db.storage()->NameToLabel(label),
                std::vector{memgraph::storage::PropertyPath{main.db.storage()->NameToProperty(nested_property1),
                                                            main.db.storage()->NameToProperty(nested_property2),
                                                            main.db.storage()->NameToProperty(nested_property3)}}}));
    const auto &l_stats_rep =
        replica.db.Access(memgraph::storage::WRITE)->GetIndexStats(replica.db.storage()->NameToLabel(label));
    ASSERT_TRUE(l_stats_rep);
    ASSERT_EQ(l_stats_rep->count, l_stats.count);
    ASSERT_EQ(l_stats_rep->avg_degree, l_stats.avg_degree);
    const auto &lp_stats_rep =
        replica.db.Access(memgraph::storage::WRITE)
            ->GetIndexStats(
                replica.db.storage()->NameToLabel(label),
                std::array{memgraph::storage::PropertyPath{replica.db.storage()->NameToProperty(property)}});
    ASSERT_TRUE(lp_stats_rep);
    ASSERT_EQ(lp_stats_rep->count, lp_stats.count);
    ASSERT_EQ(lp_stats_rep->distinct_values_count, lp_stats.distinct_values_count);
    ASSERT_EQ(lp_stats_rep->statistic, lp_stats.statistic);
    ASSERT_EQ(lp_stats_rep->avg_group_size, lp_stats.avg_group_size);
    ASSERT_EQ(lp_stats_rep->avg_degree, lp_stats.avg_degree);

    const auto &lps_stats_rep =
        replica.db.Access(memgraph::storage::WRITE)
            ->GetIndexStats(replica.db.storage()->NameToLabel(label),
                            std::array{
                                memgraph::storage::PropertyPath{replica.db.storage()->NameToProperty(property)},
                                memgraph::storage::PropertyPath{replica.db.storage()->NameToProperty(property_extra)},
                            });
    ASSERT_TRUE(lps_stats_rep);
    ASSERT_EQ(lps_stats_rep->count, lp_stats.count);
    ASSERT_EQ(lps_stats_rep->distinct_values_count, lp_stats.distinct_values_count);
    ASSERT_EQ(lps_stats_rep->statistic, lp_stats.statistic);
    ASSERT_EQ(lps_stats_rep->avg_group_size, lp_stats.avg_group_size);
    ASSERT_EQ(lps_stats_rep->avg_degree, lp_stats.avg_degree);

    const auto &nested_lps_stats_rep =
        replica.db.Access(memgraph::storage::WRITE)
            ->GetIndexStats(
                replica.db.storage()->NameToLabel(label),
                std::array{memgraph::storage::PropertyPath{main.db.storage()->NameToProperty(nested_property1),
                                                           main.db.storage()->NameToProperty(nested_property2),
                                                           main.db.storage()->NameToProperty(nested_property3)}});
    ASSERT_TRUE(nested_lps_stats_rep);
    ASSERT_EQ(nested_lps_stats_rep->count, lp_stats.count);
    ASSERT_EQ(nested_lps_stats_rep->distinct_values_count, lp_stats.distinct_values_count);
    ASSERT_EQ(nested_lps_stats_rep->statistic, lp_stats.statistic);
    ASSERT_EQ(nested_lps_stats_rep->avg_group_size, lp_stats.avg_group_size);
    ASSERT_EQ(nested_lps_stats_rep->avg_degree, lp_stats.avg_degree);

    const auto constraints = replica.db.Access(memgraph::storage::WRITE)->ListAllConstraints();
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
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto unique_acc = main.db.UniqueAccess();
    unique_acc->DeleteLabelIndexStats(main.db.storage()->NameToLabel(label));
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto unique_acc = main.db.UniqueAccess();
    ASSERT_TRUE(unique_acc->DropIndex(main.db.storage()->NameToLabel(label)).has_value());
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto unique_acc = main.db.UniqueAccess();
    unique_acc->DeleteLabelPropertyIndexStats(main.db.storage()->NameToLabel(label));
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto unique_acc = main.DropIndexAccessor();
    ASSERT_FALSE(
        !unique_acc->DropIndex(main.db.storage()->NameToLabel(label), {main.db.storage()->NameToProperty(property)})
             .has_value());
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    // Drop nested index
    auto unique_acc = main.DropIndexAccessor();
    memgraph::storage::PropertyPath property_path{main.db.storage()->NameToProperty(nested_property1),
                                                  main.db.storage()->NameToProperty(nested_property2),
                                                  main.db.storage()->NameToProperty(nested_property3)};
    ASSERT_TRUE(unique_acc->DropIndex(main.db.storage()->NameToLabel(label), {property_path}).has_value());
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    // Drop nested index stats
    auto unique_acc = main.db.UniqueAccess();
    unique_acc->DeleteLabelPropertyIndexStats(main.db.storage()->NameToLabel(label));
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto unique_acc = main.DropIndexAccessor();
    ASSERT_FALSE(!unique_acc
                      ->DropIndex(main.db.storage()->NameToLabel(label),
                                  {main.db.storage()->NameToProperty(property),
                                   main.db.storage()->NameToProperty(property_extra)})
                      .has_value());
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto read_only_access = main.db.ReadOnlyAccess();
    ASSERT_TRUE(read_only_access
                    ->DropExistenceConstraint(main.db.storage()->NameToLabel(label),
                                              main.db.storage()->NameToProperty(property))
                    .has_value());
    ASSERT_TRUE(read_only_access->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto read_only_access = main.db.ReadOnlyAccess();
    ASSERT_EQ(read_only_access->DropUniqueConstraint(
                  main.db.storage()->NameToLabel(label),
                  {main.db.storage()->NameToProperty(property), main.db.storage()->NameToProperty(property_extra)}),
              memgraph::storage::UniqueConstraints::DeletionStatus::SUCCESS);
    ASSERT_TRUE(read_only_access->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }

  {
    const memgraph::memory::DbArenaScope arena_scope{&replica.db.Arena()};
    const auto indices = replica.db.Access(memgraph::storage::WRITE)->ListAllIndices();
    ASSERT_EQ(indices.label.size(), 0);
    ASSERT_EQ(indices.label_properties.size(), 0);

    const auto &l_stats_rep =
        replica.db.Access(memgraph::storage::WRITE)->GetIndexStats(replica.db.storage()->NameToLabel(label));
    ASSERT_FALSE(l_stats_rep);
    const auto &lp_stats_rep =
        replica.db.Access(memgraph::storage::WRITE)
            ->GetIndexStats(
                replica.db.storage()->NameToLabel(label),
                std::array{memgraph::storage::PropertyPath{replica.db.storage()->NameToProperty(property)}});
    ASSERT_FALSE(lp_stats_rep);

    const auto constraints = replica.db.Access(memgraph::storage::WRITE)->ListAllConstraints();
    ASSERT_EQ(constraints.existence.size(), 0);
    ASSERT_EQ(constraints.unique.size(), 0);
  }
}

TEST_F(ReplicationTest, MultipleSynchronousReplicationTest) {
  MinMemgraph main(main_conf);
  MinMemgraph replica1(repl_conf);
  MinMemgraph replica2(repl2_conf);

  replica1.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
      .repl_server = Endpoint(local_host, ports[0]),
  });
  replica2.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
      .repl_server = Endpoint(local_host, ports[1]),
  });

  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replicas[0],
                      .mode = ReplicationMode::SYNC,
                      .repl_server_endpoint = Endpoint(local_host, ports[0]),
                  })
                  .has_value());
  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replicas[1],
                      .mode = ReplicationMode::SYNC,
                      .repl_server_endpoint = Endpoint(local_host, ports[1]),
                  })
                  .has_value());

  const auto *vertex_label = "label";
  const auto *vertex_property = "property";
  const auto *vertex_property_value = "property_value";
  std::optional<Gid> vertex_gid;
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.AddLabel(main.db.storage()->NameToLabel(vertex_label)).has_value());
    ASSERT_TRUE(v.SetProperty(main.db.storage()->NameToProperty(vertex_property), PropertyValue(vertex_property_value))
                    .has_value());
    vertex_gid.emplace(v.Gid());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }

  const auto check_replica = [&](memgraph::dbms::Database &replica_database) {
    const memgraph::memory::DbArenaScope arena_scope{&replica_database.Arena()};
    auto acc = replica_database.Access(memgraph::storage::WRITE);
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    const auto labels = v->Labels(View::OLD);
    ASSERT_TRUE(labels.has_value());
    ASSERT_THAT(*labels, UnorderedElementsAre(replica_database.storage()->NameToLabel(vertex_label)));
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  };

  check_replica(replica1.db);
  check_replica(replica2.db);

  auto handler = main.repl_handler;
  handler.UnregisterReplica(replicas[1]);
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    vertex_gid.emplace(v.Gid());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }

  // REPLICA1 should contain the new vertex
  {
    const memgraph::memory::DbArenaScope arena_scope{&replica1.db.Arena()};
    auto acc = replica1.db.Access(memgraph::storage::WRITE);
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // REPLICA2 should not contain the new vertex
  {
    const memgraph::memory::DbArenaScope arena_scope{&replica2.db.Arena()};
    auto acc = replica2.db.Access(memgraph::storage::WRITE);
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_FALSE(v);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
}

TEST_F(ReplicationTest, RecoveryProcess) {
  std::vector<Gid> vertex_gids;
  // Force the creation of snapshot
  {
    memgraph::storage::Config conf{
        .durability = {
            .root_data_directory = storage_directory,
            .recover_on_startup = true,
            .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_on_exit = true,
        }};
    UpdatePaths(conf, storage_directory);
    MinMemgraph main(conf);

    {
      const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
      auto acc = main.db.Access(memgraph::storage::WRITE);
      // Create the vertex before registering a replica
      auto v = acc->CreateVertex();
      vertex_gids.emplace_back(v.Gid());
      ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
    }
  }

  {
    // Create second WAL
    memgraph::storage::Config conf{
        .durability = {.root_data_directory = storage_directory,
                       .recover_on_startup = true,
                       .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL}};
    UpdatePaths(conf, storage_directory);
    MinMemgraph main(conf);
    // Create vertices in 2 different transactions
    {
      const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
      auto acc = main.db.Access(memgraph::storage::WRITE);
      auto v = acc->CreateVertex();
      vertex_gids.emplace_back(v.Gid());
      ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
    }
    {
      const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
      auto acc = main.db.Access(memgraph::storage::WRITE);
      auto v = acc->CreateVertex();
      vertex_gids.emplace_back(v.Gid());
      ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
    }
  }

  memgraph::storage::Config conf{
      .durability = {
          .root_data_directory = storage_directory,

          .recover_on_startup = true,
          .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
      }};
  UpdatePaths(conf, storage_directory);
  MinMemgraph main(conf);

  static constexpr const auto *property_name = "property_name";
  static constexpr const auto property_value = 1;
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    // Force the creation of current WAL file
    auto acc = main.db.Access(memgraph::storage::WRITE);
    for (const auto &vertex_gid : vertex_gids) {
      auto v = acc->FindVertex(vertex_gid, View::OLD);
      ASSERT_TRUE(v);
      ASSERT_TRUE(
          v->SetProperty(main.db.storage()->NameToProperty(property_name), PropertyValue(property_value)).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }

  static constexpr const auto *vertex_label = "vertex_label";
  {
    MinMemgraph replica(repl_conf);
    auto replica_store_handler = replica.repl_handler;

    replica_store_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
        .repl_server = Endpoint(local_host, ports[0]),
    });
    ASSERT_TRUE(main.repl_handler
                    .TryRegisterReplica(ReplicationClientConfig{
                        .name = replicas[0],
                        .mode = ReplicationMode::SYNC,
                        .repl_server_endpoint = Endpoint(local_host, ports[0]),
                    })
                    .has_value());

    while (main.db.storage()->GetReplicaState(replicas[0]) != ReplicaState::READY) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    {
      const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
      auto acc = main.db.Access(memgraph::storage::WRITE);
      for (const auto &vertex_gid : vertex_gids) {
        auto v = acc->FindVertex(vertex_gid, View::OLD);
        ASSERT_TRUE(v);
        ASSERT_TRUE(v->AddLabel(main.db.storage()->NameToLabel(vertex_label)).has_value());
      }
      ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
    }
    {
      const memgraph::memory::DbArenaScope arena_scope{&replica.db.Arena()};
      auto acc = replica.db.Access(memgraph::storage::WRITE);
      for (const auto &vertex_gid : vertex_gids) {
        auto v = acc->FindVertex(vertex_gid, View::OLD);
        ASSERT_TRUE(v);
        const auto labels = v->Labels(View::OLD);
        ASSERT_TRUE(labels.has_value());
        ASSERT_THAT(*labels, UnorderedElementsAre(replica.db.storage()->NameToLabel(vertex_label)));
        const auto properties = v->Properties(View::OLD);
        ASSERT_TRUE(properties.has_value());
        ASSERT_THAT(*properties,
                    UnorderedElementsAre(std::make_pair(replica.db.storage()->NameToProperty(property_name),
                                                        PropertyValue(property_value))));
      }
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
  }
  {
    memgraph::storage::Config repl_conf{
        .durability = {.root_data_directory = storage_directory,
                       .recover_on_startup = true,
                       .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL}};
    UpdatePaths(repl_conf, repl_storage_directory);
    MinMemgraph replica(repl_conf);
    {
      const memgraph::memory::DbArenaScope arena_scope{&replica.db.Arena()};
      auto acc = replica.db.Access(memgraph::storage::WRITE);
      for (const auto &vertex_gid : vertex_gids) {
        auto v = acc->FindVertex(vertex_gid, View::OLD);
        ASSERT_TRUE(v);
        const auto labels = v->Labels(View::OLD);
        ASSERT_TRUE(labels.has_value());
        ASSERT_THAT(*labels, UnorderedElementsAre(replica.db.storage()->NameToLabel(vertex_label)));
        const auto properties = v->Properties(View::OLD);
        ASSERT_TRUE(properties.has_value());
        ASSERT_THAT(*properties,
                    UnorderedElementsAre(std::make_pair(replica.db.storage()->NameToProperty(property_name),
                                                        PropertyValue(property_value))));
      }
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
  }
}

TEST_F(ReplicationTest, BasicAsynchronousReplicationTest) {
  MinMemgraph main(main_conf);
  MinMemgraph replica_async(repl_conf);

  auto replica_store_handler = replica_async.repl_handler;
  replica_store_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
      .repl_server = Endpoint(local_host, ports[1]),
  });

  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = "REPLICA_ASYNC",
                      .mode = ReplicationMode::ASYNC,
                      .repl_server_endpoint = Endpoint(local_host, ports[1]),
                  })
                  .has_value());

  static constexpr size_t vertices_create_num = 10;
  std::vector<Gid> created_vertices;
  for (size_t i = 0; i < vertices_create_num; ++i) {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    created_vertices.push_back(v.Gid());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());

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

  ASSERT_TRUE(std::ranges::all_of(created_vertices, [&](const auto vertex_gid) {
    const memgraph::memory::DbArenaScope arena_scope{&replica_async.db.Arena()};
    auto acc = replica_async.db.Access(memgraph::storage::WRITE);
    auto v = acc->FindVertex(vertex_gid, View::OLD);
    const bool exists = v.has_value();
    EXPECT_FALSE(!acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    return exists;
  }));
}

TEST_F(ReplicationTest, EpochTest) {
  MinMemgraph main(main_conf);
  MinMemgraph replica1(repl_conf);

  replica1.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
      .repl_server = Endpoint(local_host, ports[0]),
  });

  MinMemgraph replica2(repl2_conf);
  replica2.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
      .repl_server = Endpoint(local_host, 10'001),
  });

  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replicas[0],
                      .mode = ReplicationMode::SYNC,
                      .repl_server_endpoint = Endpoint(local_host, ports[0]),
                  })
                  .has_value());

  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replicas[1],
                      .mode = ReplicationMode::SYNC,
                      .repl_server_endpoint = Endpoint(local_host, 10'001),
                  })
                  .has_value());

  std::optional<Gid> vertex_gid;
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = main.db.Access(memgraph::storage::WRITE);
    const auto v = acc->CreateVertex();
    vertex_gid.emplace(v.Gid());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&replica1.db.Arena()};
    auto acc = replica1.db.Access(memgraph::storage::WRITE);
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&replica2.db.Arena()};
    auto acc = replica2.db.Access(memgraph::storage::WRITE);
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  main.repl_handler.UnregisterReplica(replicas[0]);
  main.repl_handler.UnregisterReplica(replicas[1]);

  ASSERT_TRUE(replica1.repl_handler.SetReplicationRoleMain());

  ASSERT_TRUE(replica1.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replicas[1],
                      .mode = ReplicationMode::SYNC,
                      .repl_server_endpoint = Endpoint(local_host, 10'001),
                  })
                  .has_value());

  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = main.db.Access(memgraph::storage::WRITE);
    acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&replica1.db.Arena()};
    auto acc = replica1.db.Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    vertex_gid.emplace(v.Gid());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(replica1.db_acc)).has_value());
  }
  // Replica1 should forward it's vertex to Replica2
  {
    const memgraph::memory::DbArenaScope arena_scope{&replica2.db.Arena()};
    auto acc = replica2.db.Access(memgraph::storage::WRITE);
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_TRUE(v);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  replica1.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
      .repl_server = Endpoint(local_host, ports[0]),
  });
  ASSERT_FALSE(main.repl_handler
                   .TryRegisterReplica(ReplicationClientConfig{
                       .name = replicas[0],
                       .mode = ReplicationMode::SYNC,
                       .repl_server_endpoint = Endpoint(local_host, ports[0]),
                   })
                   .has_value());

  {
    auto acc = main.db.Access(memgraph::storage::WRITE);
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    const auto v = acc->CreateVertex();
    vertex_gid.emplace(v.Gid());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  // Replica1 is not compatible with the main so it shouldn't contain
  // it's newest vertex
  {
    const memgraph::memory::DbArenaScope arena_scope{&replica1.db.Arena()};
    auto acc = replica1.db.Access(memgraph::storage::WRITE);
    const auto v = acc->FindVertex(*vertex_gid, View::OLD);
    ASSERT_FALSE(v);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
}

TEST_F(ReplicationTest, ReplicationInformation) {
  MinMemgraph main(main_conf);
  MinMemgraph replica1(repl_conf);

  uint16_t replica1_port = 10'001;
  replica1.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
      .repl_server = Endpoint(local_host, replica1_port),
  });

  uint16_t replica2_port = 10'002;
  MinMemgraph replica2(repl2_conf);
  replica2.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
      .repl_server = Endpoint(local_host, replica2_port),
  });

  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replicas[0],
                      .mode = ReplicationMode::SYNC,
                      .repl_server_endpoint = Endpoint(local_host, replica1_port),
                  })
                  .has_value());

  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replicas[1],
                      .mode = ReplicationMode::ASYNC,
                      .repl_server_endpoint = Endpoint(local_host, replica2_port),
                  })
                  .has_value());

  ASSERT_TRUE(main.repl_state->IsMain());
  ASSERT_TRUE(replica1.repl_state->IsReplica());
  ASSERT_TRUE(replica2.repl_state->IsReplica());

  auto const maybe_replicas_info = main.repl_handler.ShowReplicas();
  ASSERT_TRUE(maybe_replicas_info.has_value());
  auto const &replicas_info = maybe_replicas_info.value();
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

  uint16_t replica1_port = 10'001;
  replica1.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
      .repl_server = Endpoint(local_host, replica1_port),
  });

  uint16_t replica2_port = 10'002;
  MinMemgraph replica2(repl2_conf);
  replica2.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
      .repl_server = Endpoint(local_host, replica2_port),
  });
  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replicas[0],
                      .mode = ReplicationMode::SYNC,
                      .repl_server_endpoint = Endpoint(local_host, replica1_port),
                  })
                  .has_value());

  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replicas[0],
                      .mode = ReplicationMode::ASYNC,
                      .repl_server_endpoint = Endpoint(local_host, replica2_port),
                  })
                  .error() == RegisterReplicaError::NAME_EXISTS);
}

TEST_F(ReplicationTest, ReplicationReplicaWithExistingEndPoint) {
  uint16_t common_port = 10'001;

  MinMemgraph main(main_conf);
  MinMemgraph replica1(repl_conf);
  replica1.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
      .repl_server = Endpoint(local_host, common_port),
  });

  MinMemgraph replica2(repl2_conf);
  replica2.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
      .repl_server = Endpoint(local_host, common_port),
  });

  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replicas[0],
                      .mode = ReplicationMode::SYNC,
                      .repl_server_endpoint = Endpoint(local_host, common_port),
                  })
                  .has_value());

  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replicas[1],
                      .mode = ReplicationMode::ASYNC,
                      .repl_server_endpoint = Endpoint(local_host, common_port),
                  })
                  .error() == RegisterReplicaError::ENDPOINT_EXISTS);
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

  replica1.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
      .repl_server = Endpoint(local_host, ports[0]),
  });

  MinMemgraph replica2(replica2_config);
  replica2.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
      .repl_server = Endpoint(local_host, ports[1]),
  });

  auto res = main->repl_handler.TryRegisterReplica(ReplicationClientConfig{
      .name = replicas[0],
      .mode = ReplicationMode::SYNC,
      .repl_server_endpoint = Endpoint(local_host, ports[0]),
  });
  ASSERT_TRUE(res.has_value()) << (int)res.error();
  res = main->repl_handler.TryRegisterReplica(ReplicationClientConfig{
      .name = replicas[1],
      .mode = ReplicationMode::SYNC,
      .repl_server_endpoint = Endpoint(local_host, ports[1]),
  });
  ASSERT_TRUE(res.has_value()) << (int)res.error();

  {
    auto const maybe_replicas_info = main->repl_handler.ShowReplicas();
    ASSERT_TRUE(maybe_replicas_info.has_value());
    auto const &replicas_info = maybe_replicas_info.value();
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
    ASSERT_TRUE(maybe_replicas_info.has_value());
    auto const &replicas_info = maybe_replicas_info.value();

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

  replica1.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
      .repl_server = Endpoint(local_host, ports[0]),
  });

  MinMemgraph replica2(repl2_conf);

  replica2.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
      .repl_server = Endpoint(local_host, ports[1]),
  });
  auto res = main->repl_handler.TryRegisterReplica(ReplicationClientConfig{
      .name = replicas[0],
      .mode = ReplicationMode::SYNC,
      .repl_server_endpoint = Endpoint(local_host, ports[0]),
  });
  ASSERT_TRUE(res.has_value());
  res = main->repl_handler.TryRegisterReplica(ReplicationClientConfig{
      .name = replicas[1],
      .mode = ReplicationMode::SYNC,
      .repl_server_endpoint = Endpoint(local_host, ports[1]),
  });
  ASSERT_TRUE(res.has_value());

  {
    auto const maybe_replicas_info = main->repl_handler.ShowReplicas();
    ASSERT_TRUE(maybe_replicas_info.has_value());
    auto const &replicas_info = maybe_replicas_info.value();

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
    ASSERT_TRUE(maybe_replicas_info.has_value());
    auto const &replicas_info = maybe_replicas_info.value();

    ASSERT_EQ(replicas_info.entries_.size(), 1);
    ASSERT_EQ(replicas_info.entries_[0].name_, replicas[1]);
    ASSERT_EQ(replicas_info.entries_[0].socket_address_, fmt::format("{}:{}", local_host, ports[1]));
  }

  main.reset();

  {
    MinMemgraph other_main(main_config);
    auto const maybe_replicas_info = other_main.repl_handler.ShowReplicas();
    ASSERT_TRUE(maybe_replicas_info.has_value());
    auto const &replicas_info = maybe_replicas_info.value();

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
          .error() == RegisterReplicaError::ERROR_ACCEPTING_MAIN);
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
    const memgraph::memory::DbArenaScope arena_scope{&main->db.Arena()};
    auto acc = in_mem->Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.SetProperty(p, large_property).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  };

  auto create_vertex_and_commit = [&]() {
    const memgraph::memory::DbArenaScope arena_scope{&main->db.Arena()};
    auto acc = in_mem->Access(memgraph::storage::WRITE);
    acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  };

  // Nothing
  {
    const memgraph::memory::DbArenaScope arena_scope{&main->db.Arena()};
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 0);
  }

  // Only Current
  {
    const memgraph::memory::DbArenaScope arena_scope{&main->db.Arena()};
    create_vertex_and_commit();
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 1);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryCurrentWal>(recovery_steps[0]));
  }

  // Only a single WAL
  {
    const memgraph::memory::DbArenaScope arena_scope{&main->db.Arena()};
    // Create a vertex with a property large enough to trigger WAL finalization and closing
    // Current is generated on the next transaction
    large_write_to_finalize_wal();
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 1);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryWals>(recovery_steps[0]));
  }

  // Multiple WALs
  {
    const memgraph::memory::DbArenaScope arena_scope{&main->db.Arena()};
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
    const memgraph::memory::DbArenaScope arena_scope{&main->db.Arena()};
    // A new current WAL is created on the next transaction after the previous one has been finalized
    create_vertex_and_commit();
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 2);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryWals>(recovery_steps[0]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryCurrentWal>(recovery_steps[1]));
  }

  // Snapshot (with dirty WALs)
  {
    const memgraph::memory::DbArenaScope arena_scope{&main->db.Arena()};
    large_write_to_finalize_wal();
    ASSERT_TRUE(in_mem->CreateSnapshot().has_value());
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 1);
    // TODO Currently we prefer WALs over Snapshots when creating the recovery plan
    // This is an inefficiency when the snapshot is smaller than the WALs we would send
    // Calculate how large the two payloads would be and pick the smaller plan
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryWals>(recovery_steps[0]));
  }

  // Only snapshot (without dirty WALs)
  {
    const memgraph::memory::DbArenaScope arena_scope{&main->db.Arena()};
    // Once we are over the allowed number of snapshots, we clean both snapshots and wals
    // Have to make a change to the db so the snapshot doesn't get aborted (to bypass SnapshotDigest)
    create_vertex_and_commit();
    ASSERT_TRUE(in_mem->CreateSnapshot().has_value());
    create_vertex_and_commit();
    ASSERT_TRUE(in_mem->CreateSnapshot().has_value());
    create_vertex_and_commit();
    ASSERT_TRUE(in_mem->CreateSnapshot().has_value());
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 1);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoverySnapshot>(recovery_steps[0]));
  }

  // Snapshot + Current
  {
    const memgraph::memory::DbArenaScope arena_scope{&main->db.Arena()};
    auto acc = in_mem->Access(memgraph::storage::WRITE);
    acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 2);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoverySnapshot>(recovery_steps[0]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryCurrentWal>(recovery_steps[1]));
  }

  // Snapshot + WALs (chain starts before snapshot)
  {
    const memgraph::memory::DbArenaScope arena_scope{&main->db.Arena()};
    large_write_to_finalize_wal();
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 2);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoverySnapshot>(recovery_steps[0]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryWals>(recovery_steps[1]));
  }

  // Snapshot + WALs + Current
  {
    const memgraph::memory::DbArenaScope arena_scope{&main->db.Arena()};
    auto acc = in_mem->Access(memgraph::storage::WRITE);
    acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
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
    const memgraph::memory::DbArenaScope arena_scope{&main->db.Arena()};
    // On start we only have the snapshot to send
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 1);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoverySnapshot>(recovery_steps[0]));
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main->db.Arena()};
    // Add current wal
    auto acc = in_mem->Access(memgraph::storage::WRITE);
    acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 2);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoverySnapshot>(recovery_steps[0]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryCurrentWal>(recovery_steps[1]));
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main->db.Arena()};
    // Add finalized wal
    large_write_to_finalize_wal();
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 2);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoverySnapshot>(recovery_steps[0]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryWals>(recovery_steps[1]));
  }
  {
    const memgraph::memory::DbArenaScope arena_scope{&main->db.Arena()};
    // Add both
    auto acc = in_mem->Access(memgraph::storage::WRITE);
    acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
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
    const memgraph::memory::DbArenaScope arena_scope{&main->db.Arena()};
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
    ASSERT_TRUE(in_mem->CreateSnapshot().has_value());
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
    const memgraph::memory::DbArenaScope arena_scope{&main->db.Arena()};
    auto acc = in_mem->Access(memgraph::storage::WRITE);
    acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    const auto recovery_steps = GetRecoverySteps(0, &file_locker, in_mem).value();
    ASSERT_EQ(recovery_steps.size(), 3);
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoverySnapshot>(recovery_steps[0]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryWals>(recovery_steps[1]));
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::RecoveryCurrentWal>(recovery_steps[2]));
  }
}

// A WAL whose [from, to] range contains the snapshot's timestamp proves the chain can reproduce the
// snapshot's contents. The pair below pins both verdicts, because a false positive costs a full
// snapshot transfer on every lagging replica.
TEST(SnapshotWalCoverage, CoveredAndUncovered) {
  using memgraph::storage::SnapshotTsCoveredByAnyWal;
  auto const wal = [](uint64_t const seq_num, uint64_t const from, uint64_t const to) {
    return memgraph::storage::durability::WalDurabilityInfo{seq_num, from, to, "uuid", "epoch", "path"};
  };
  std::vector const finalized{wal(0, 1, 100), wal(1, 101, 200)};

  // Ordinary periodic snapshot: its timestamp sits inside the still-open WAL's range.
  EXPECT_TRUE(SnapshotTsCoveredByAnyWal(finalized, 201, 300, 250));
  // ... or inside a finalized one, if the snapshot predates the current WAL.
  EXPECT_TRUE(SnapshotTsCoveredByAnyWal(finalized, 201, 300, 150));
  // Range ends are inclusive: a snapshot stamped exactly at a WAL boundary is covered.
  EXPECT_TRUE(SnapshotTsCoveredByAnyWal(finalized, std::nullopt, std::nullopt, 100));

  // After an analytical episode: the pre-import WAL was finalized at 200 and the post-import WAL
  // starts after the snapshot, so nothing holds the imported data.
  EXPECT_FALSE(SnapshotTsCoveredByAnyWal(finalized, 5001, 5100, 5000));
  // No open WAL at all, snapshot past the end of the chain.
  EXPECT_FALSE(SnapshotTsCoveredByAnyWal(finalized, std::nullopt, std::nullopt, 5000));
}

// Decision 9 of specs/ha-analytical-import.md. Entering analytical finalizes the WAL and the import
// writes none, so the switch-back snapshot holds data no WAL can reproduce. A replica sitting strictly
// behind the end of the finalized chain would otherwise be recovered from WALs alone and declared in
// sync while silently missing the whole import.
TEST_F(ReplicationTest, RecoveryStepsAfterAnalyticalEpisode) {
  auto config = main_conf;
  config.durability.wal_file_size_kibibytes = 1;  // Easy way to control when a new WAL is created
  MinMemgraph main(config);
  auto *in_mem = static_cast<InMemoryStorage *>(main.db.storage());

  memgraph::utils::FileRetainer file_retainer;
  auto file_locker = file_retainer.AddLocker();

  auto const p = in_mem->NameToProperty("p1");
  const auto large_property = PropertyValue{PropertyValue::list_t{1024 / sizeof(int64_t), PropertyValue{int64_t{}}}};

  auto large_write_to_finalize_wal = [&]() {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = in_mem->Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.SetProperty(p, large_property).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  };
  auto create_vertices_and_commit = [&](int const n) {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = in_mem->Access(memgraph::storage::WRITE);
    for (int i = 0; i < n; ++i) acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  };

  // A finalized chain of two files, the newest of which spans several commits so a replica can sit
  // strictly inside it.
  large_write_to_finalize_wal();
  create_vertices_and_commit(1);
  create_vertices_and_commit(1);
  large_write_to_finalize_wal();

  auto const wal_dir = in_mem->config_.durability.storage_directory / memgraph::storage::durability::kWalDirectory;
  auto const chain = memgraph::storage::durability::GetWalFiles(wal_dir, std::string{in_mem->uuid()});
  ASSERT_TRUE(chain.has_value());
  ASSERT_FALSE(chain->empty());
  ASSERT_GT(chain->back().to_timestamp, chain->back().from_timestamp);
  // Strictly behind the end of the chain, but inside the newest file's range: exactly the layout in
  // which the pre-fix code takes the WAL-only path.
  auto const replica_commit = chain->back().to_timestamp - 1;

  in_mem->SetStorageMode(memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL);
  create_vertices_and_commit(10);
  in_mem->SetStorageMode(memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);
  // First post-switch commit opens a fresh WAL, so the current WAL starts after the snapshot.
  create_vertices_and_commit(1);

  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto const steps = GetRecoverySteps(replica_commit, &file_locker, in_mem);
    ASSERT_TRUE(steps.has_value());
    EXPECT_TRUE(std::ranges::any_of(*steps, [](auto const &step) {
      return std::holds_alternative<memgraph::storage::RecoverySnapshot>(step);
    })) << "the mode-change snapshot is the only source of the imported data";
  }
}

// The negative half of the pair: an ordinary periodic snapshot must not force a snapshot transfer for
// a lagging replica, since its timestamp lies inside the open WAL's range.
TEST_F(ReplicationTest, RecoveryStepsAfterPeriodicSnapshot) {
  auto config = main_conf;
  config.durability.wal_file_size_kibibytes = 1;
  MinMemgraph main(config);
  auto *in_mem = static_cast<InMemoryStorage *>(main.db.storage());

  memgraph::utils::FileRetainer file_retainer;
  auto file_locker = file_retainer.AddLocker();

  auto const p = in_mem->NameToProperty("p1");
  const auto large_property = PropertyValue{PropertyValue::list_t{1024 / sizeof(int64_t), PropertyValue{int64_t{}}}};

  auto large_write_to_finalize_wal = [&]() {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = in_mem->Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.SetProperty(p, large_property).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  };
  auto create_vertex_and_commit = [&]() {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = in_mem->Access(memgraph::storage::WRITE);
    acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  };

  large_write_to_finalize_wal();
  create_vertex_and_commit();
  create_vertex_and_commit();
  large_write_to_finalize_wal();

  auto const wal_dir = in_mem->config_.durability.storage_directory / memgraph::storage::durability::kWalDirectory;
  auto const chain = memgraph::storage::durability::GetWalFiles(wal_dir, std::string{in_mem->uuid()});
  ASSERT_TRUE(chain.has_value());
  ASSERT_FALSE(chain->empty());
  ASSERT_GT(chain->back().to_timestamp, chain->back().from_timestamp);
  auto const replica_commit = chain->back().to_timestamp - 1;

  create_vertex_and_commit();  // opens the current WAL the snapshot's timestamp will fall inside
  ASSERT_TRUE(in_mem->CreateSnapshot().has_value());

  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto const steps = GetRecoverySteps(replica_commit, &file_locker, in_mem);
    ASSERT_TRUE(steps.has_value());
    EXPECT_FALSE(std::ranges::any_of(*steps, [](auto const &step) {
      return std::holds_alternative<memgraph::storage::RecoverySnapshot>(step);
    })) << "the WAL chain reproduces everything, so shipping the snapshot is pure waste";
  }
}

// Analytical writes are never appended to the WAL, so a storage that replicates must not be allowed to
// enter analytical mode; registration in the other order must be refused too.
TEST_F(ReplicationTest, AnalyticalModeAndReplicationAreMutuallyExclusive) {
  MinMemgraph main(main_conf);
  MinMemgraph replica(repl_conf);

  auto replica_store_handler = replica.repl_handler;
  replica_store_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{.repl_server = Endpoint(local_host, ports[0])});

  auto const reg = main.repl_handler.TryRegisterReplica(ReplicationClientConfig{
      .name = "REPLICA",
      .mode = ReplicationMode::SYNC,
      .repl_server_endpoint = Endpoint(local_host, ports[0]),
  });
  ASSERT_TRUE(reg.has_value()) << static_cast<int>(reg.error());

  auto *in_mem = static_cast<InMemoryStorage *>(main.db.storage());
  ASSERT_THROW(in_mem->SetStorageMode(memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL),
               memgraph::utils::BasicException);
  ASSERT_EQ(in_mem->GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);

  ASSERT_EQ(main.repl_handler.UnregisterReplica("REPLICA"), UnregisterReplicaResult::SUCCESS);
  ASSERT_NO_THROW(in_mem->SetStorageMode(memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL));

  // Now the other direction: registering while analytical is rejected before any state is mutated, so
  // it does not leave a persisted instance-level client that every retry then trips over.
  auto const reg_while_analytical = main.repl_handler.TryRegisterReplica(ReplicationClientConfig{
      .name = "REPLICA",
      .mode = ReplicationMode::SYNC,
      .repl_server_endpoint = Endpoint(local_host, ports[0]),
  });
  ASSERT_FALSE(reg_while_analytical.has_value());
  ASSERT_EQ(reg_while_analytical.error(), RegisterReplicaError::ANALYTICAL_MODE);

  // Rejected up-front means the name is still free once the instance is transactional again.
  in_mem->SetStorageMode(memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);
  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = "REPLICA",
                      .mode = ReplicationMode::SYNC,
                      .repl_server_endpoint = Endpoint(local_host, ports[0]),
                  })
                  .has_value());
}

// Unregistering while analytical would erase the persisted client while leaving the per-database
// clients untouched, so it is refused up-front too -- before the name is even looked up.
TEST_F(ReplicationTest, UnregisterReplicaRefusedWhileAnalytical) {
  MinMemgraph main(main_conf);
  auto *in_mem = static_cast<InMemoryStorage *>(main.db.storage());

  in_mem->SetStorageMode(memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL);
  ASSERT_EQ(main.repl_handler.UnregisterReplica("REPLICA"), UnregisterReplicaResult::ANALYTICAL_MODE);

  in_mem->SetStorageMode(memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);
  ASSERT_EQ(main.repl_handler.UnregisterReplica("REPLICA"), UnregisterReplicaResult::CANNOT_UNREGISTER);
}

TEST_F(ReplicationTest, SchemaReplication) {
  memgraph::storage::Config conf{
      .durability =
          {
              .root_data_directory = storage_directory,

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
      .register_metrics = false,
  };

  auto repl_conf = conf;
  UpdatePaths(conf, storage_directory);
  std::optional<MinMemgraph> main(conf);

  repl_conf.durability.recover_on_startup = false;
  UpdatePaths(repl_conf, repl_storage_directory);
  std::optional<MinMemgraph> replica(repl_conf);

  replica->repl_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{.repl_server = Endpoint(local_host, ports[0])});

  const auto &reg = main->repl_handler.TryRegisterReplica(ReplicationClientConfig{
      .name = "REPLICA",
      .mode = ReplicationMode::SYNC,
      .repl_server_endpoint = Endpoint(local_host, ports[0]),
  });
  ASSERT_TRUE(reg.has_value()) << (int)reg.error();

  auto get_schema = [](auto &instance) {
    return instance.db.storage()->schema_info_.ToJson(*instance.db.storage()->name_id_mapper_,
                                                      instance.db.storage()->enum_store_);
  };

  std::optional<memgraph::memory::DbArenaScope> main_scope{std::in_place, &main->db.Arena()};

  auto l1 = main->db.storage()->NameToLabel("L1");
  auto l2 = main->db.storage()->NameToLabel("L2");
  auto l3 = main->db.storage()->NameToLabel("L3");
  auto p1 = main->db.storage()->NameToProperty("p1");
  auto p2 = main->db.storage()->NameToProperty("p2");
  auto e = main->db.storage()->NameToEdgeType("E");

  // Check current delta replication
  {
    auto acc = main->db.Access(memgraph::storage::WRITE);
    acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main->db_acc)).has_value());
    EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
  }

  {
    auto acc = main->db.Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.AddLabel(l1).has_value());
    ASSERT_TRUE(v.AddLabel(l2).has_value());
    ASSERT_TRUE(v.AddLabel(l3).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main->db_acc)).has_value());
    EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
  }

  {
    auto acc = main->db.Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.SetProperty(p1, PropertyValue{123}).has_value());
    ASSERT_TRUE(v.SetProperty(p1, PropertyValue{123.45}).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main->db_acc)).has_value());
    EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
  }

  {
    auto acc = main->db.Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.SetProperty(p1, PropertyValue{true}).has_value());
    ASSERT_TRUE(v.AddLabel(l3).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main->db_acc)).has_value());
    EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
  }

  {
    auto acc = main->db.Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    ASSERT_TRUE(acc->CreateEdge(&v1, &v2, e).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main->db_acc)).has_value());
    EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
  }

  {
    auto acc = main->db.Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    ASSERT_TRUE(v1.AddLabel(l1).has_value());
    ASSERT_TRUE(v1.AddLabel(l3).has_value());
    auto v2 = acc->CreateVertex();
    ASSERT_TRUE(v2.AddLabel(l2).has_value());
    ASSERT_TRUE(acc->CreateEdge(&v1, &v2, e).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main->db_acc)).has_value());
    EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
  }

  {
    auto acc = main->db.Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    auto edge = acc->CreateEdge(&v1, &v2, e);
    ASSERT_TRUE(edge->SetProperty(p2, PropertyValue{""}).has_value());
    ASSERT_TRUE(edge->SetProperty(p1, PropertyValue{123}).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main->db_acc)).has_value());
    EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
  }

  {
    auto acc = main->db.Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    auto edge = acc->CreateEdge(&v1, &v2, e);
    ASSERT_TRUE(edge->SetProperty(p2, PropertyValue{""}).has_value());
    ASSERT_TRUE(edge->SetProperty(p1, PropertyValue{123}).has_value());
    ASSERT_TRUE(v2.AddLabel(l2).has_value());
    ASSERT_TRUE(v1.AddLabel(l2).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main->db_acc)).has_value());
    EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
  }

  {
    auto acc = main->db.Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    auto edge = acc->CreateEdge(&v1, &v2, e);
    const auto v1_gid = v1.Gid();
    const auto v2_gid = v2.Gid();
    const auto edge_gid = edge->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main->db_acc)).has_value());

    auto acc2 = main->db.Access(memgraph::storage::WRITE);
    auto prev_v1 = acc2->FindVertex(v1_gid, View::NEW);
    auto prev_v2 = acc2->FindVertex(v2_gid, View::NEW);
    auto prev_edge = acc2->FindEdge(edge_gid, View::NEW);
    ASSERT_TRUE(prev_edge->SetProperty(p2, PropertyValue{""}).has_value());
    ASSERT_TRUE(prev_edge->SetProperty(p1, PropertyValue{123}).has_value());
    ASSERT_TRUE(prev_v2->AddLabel(l2).has_value());
    ASSERT_TRUE(prev_v1->AddLabel(l2).has_value());
    ASSERT_TRUE(acc2->PrepareForCommitPhase(MakeCommitArgs(main->db_acc)).has_value());
    EXPECT_TRUE(ConfrontJSON(get_schema(*main), get_schema(*replica)));
  }

  main_scope.reset();

  auto stop_replica = [&]() {
    replica.reset();
    {
      int tries = 0;
      while (main->repl_handler.ShowReplicas().value().entries_[0].data_info_.at("memgraph").state_ !=
             ReplicaState::MAYBE_BEHIND) {
        std::this_thread::sleep_for(std::chrono::seconds{1});
        ASSERT_LE(++tries, 20) << "Waited too long for shutdown";
      }
    }
  };

  auto start_replica = [&]() {
    replica.emplace(repl_conf);
    replica->repl_handler.TrySetReplicationRoleReplica(
        ReplicationServerConfig{.repl_server = Endpoint(local_host, ports[0])});
    {
      int tries = 0;
      while (main->repl_handler.ShowReplicas().value().entries_[0].data_info_.at("memgraph").state_ !=
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

TEST_F(ReplicationTest, ReplicationWithNonSequentialDeltas) {
  MinMemgraph main(main_conf);
  MinMemgraph replica(repl_conf);

  auto replica_store_handler = replica.repl_handler;
  replica_store_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{.repl_server = Endpoint(local_host, ports[0])});

  const auto &reg = main.repl_handler.TryRegisterReplica(ReplicationClientConfig{
      .name = "REPLICA",
      .mode = ReplicationMode::SYNC,
      .repl_server_endpoint = Endpoint(local_host, ports[0]),
  });
  ASSERT_TRUE(reg.has_value()) << (int)reg.error();

  memgraph::storage::Gid v1_gid, v2_gid, v3_gid;
  memgraph::storage::EdgeTypeId edge_type_1, edge_type_2;

  // Create base vertices
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    auto v3 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    v3_gid = v3.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }

  // Wait for replication to catch up
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Transaction 1: Create edge from v1 to v2
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto tx1 = main.db.Access(memgraph::storage::WRITE);
    auto v1_tx1 = tx1->FindVertex(v1_gid, View::OLD);
    auto v2_tx1 = tx1->FindVertex(v2_gid, View::OLD);
    ASSERT_TRUE(v1_tx1.has_value());
    ASSERT_TRUE(v2_tx1.has_value());

    edge_type_1 = tx1->NameToEdgeType("Edge1");
    auto edge1 = tx1->CreateEdge(&*v1_tx1, &*v2_tx1, edge_type_1);
    ASSERT_TRUE(edge1.has_value());

    // Transaction 2: Create non-sequential edge from v1 to v3 (concurrent)
    auto tx2 = main.db.Access(memgraph::storage::WRITE);
    auto v1_tx2 = tx2->FindVertex(v1_gid, View::OLD);
    auto v3_tx2 = tx2->FindVertex(v3_gid, View::OLD);
    ASSERT_TRUE(v1_tx2.has_value());
    ASSERT_TRUE(v3_tx2.has_value());

    edge_type_2 = tx2->NameToEdgeType("Edge2");
    auto edge2 = tx2->CreateEdge(&*v1_tx2, &*v3_tx2, edge_type_2);
    ASSERT_TRUE(edge2.has_value());

    // Commit tx1 first (this will create non-sequential deltas for tx2)
    ASSERT_TRUE(tx1->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
    tx1.reset();

    // Commit tx2 (with non-sequential deltas)
    ASSERT_TRUE(tx2->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
    tx2.reset();
  }

  // Wait for replication to catch up
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Verify replica has both edges
  {
    const memgraph::memory::DbArenaScope arena_scope{&replica.db.Arena()};
    auto acc = replica.db.Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, View::OLD);
    ASSERT_TRUE(v1.has_value());

    auto edges = v1->OutEdges(View::OLD);
    ASSERT_TRUE(edges.has_value());
    ASSERT_EQ(edges->edges.size(), 2);

    // Verify both edges exist
    std::vector<memgraph::storage::EdgeTypeId> edge_types;
    for (const auto &edge : edges->edges) {
      edge_types.push_back(edge.EdgeType());
    }
    ASSERT_THAT(edge_types, UnorderedElementsAre(edge_type_1, edge_type_2));

    // Verify edge destinations
    bool found_edge1 = false, found_edge2 = false;
    for (const auto &edge : edges->edges) {
      if (edge.EdgeType() == edge_type_1 && edge.ToVertex().Gid() == v2_gid) {
        found_edge1 = true;
      }
      if (edge.EdgeType() == edge_type_2 && edge.ToVertex().Gid() == v3_gid) {
        found_edge2 = true;
      }
    }
    ASSERT_TRUE(found_edge1) << "Edge1 (v1->v2) should exist on replica";
    ASSERT_TRUE(found_edge2) << "Edge2 (v1->v3) should exist on replica";

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
}

TEST_F(ReplicationTest, GetTelemetryJson) {
  MinMemgraph main(main_conf);
  MinMemgraph replica1(repl_conf);
  MinMemgraph replica2(repl2_conf);

  replica1.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
      .repl_server = Endpoint(local_host, ports[0]),
  });
  replica2.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{
      .repl_server = Endpoint(local_host, ports[1]),
  });

  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replicas[0],
                      .mode = ReplicationMode::ASYNC,
                      .repl_server_endpoint = Endpoint(local_host, ports[0]),
                  })
                  .has_value());
  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replicas[1],
                      .mode = ReplicationMode::SYNC,
                      .repl_server_endpoint = Endpoint(local_host, ports[1]),
                  })
                  .has_value());

  ASSERT_FALSE(replica1.repl_state.ReadLock()->GetTelemetryJson().has_value());
  ASSERT_FALSE(replica2.repl_state.ReadLock()->GetTelemetryJson().has_value());

  auto const main_json = main.repl_state.ReadLock()->GetTelemetryJson();
  ASSERT_TRUE(main_json.has_value());

  auto const expected_json = nlohmann::json({{"async", 1}, {"sync", 1}, {"strict_sync", 0}});
  ASSERT_EQ(main_json.value(), expected_json);
}

// A replica recovered via snapshot transfer must end up with edge metadata, so
// a later (replicated) delete + GC on the replica does not hit "metadata not
// found". Exercises the snapshot-recovery path on the replica.
TEST_F(ReplicationTest, EdgeMetadataRecoveredOnReplicaSnapshotTransfer) {
  main_conf.salient.items.enable_edges_metadata = true;
  repl_conf.salient.items.enable_edges_metadata = true;

  Gid e0_gid{Gid::FromUint(0)};
  Gid e1_gid{Gid::FromUint(0)};

  MinMemgraph main(main_conf);

  // Create a couple of edges on main.
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    auto v3 = acc->CreateVertex();
    auto e0 = acc->CreateEdge(&v1, &v2, main.db.storage()->NameToEdgeType("et"));
    auto e1 = acc->CreateEdge(&v1, &v3, main.db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(e0.has_value() && e1.has_value());
    e0_gid = e0->Gid();
    e1_gid = e1->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }

  // Force a snapshot. With no finalized WAL yet, registering a fresh replica makes
  // GetRecoverySteps ship the SNAPSHOT (not WAL), exercising the snapshot-recovery
  // metadata rebuild on the replica.
  ASSERT_TRUE(static_cast<InMemoryStorage *>(main.db.storage())->CreateSnapshot(true).has_value());

  MinMemgraph replica(repl_conf);
  auto replica_store_handler = replica.repl_handler;
  replica_store_handler.TrySetReplicationRoleReplica(
      ReplicationServerConfig{.repl_server = Endpoint(local_host, ports[0])});
  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replicas[0],
                      .mode = ReplicationMode::SYNC,
                      .repl_server_endpoint = Endpoint(local_host, ports[0]),
                  })
                  .has_value());
  while (main.db.storage()->GetReplicaState(replicas[0]) != ReplicaState::READY) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // Replica recovered via snapshot transfer must resolve both edges by id (metadata).
  {
    auto acc = replica.db.Access(memgraph::storage::READ);
    ASSERT_TRUE(acc->FindEdge(e0_gid, View::OLD).has_value());
    ASSERT_TRUE(acc->FindEdge(e1_gid, View::OLD).has_value());
  }

  // Delete one edge on main; SYNC replication applies it on the replica.
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto e = acc->FindEdge(e0_gid, View::OLD);
    ASSERT_TRUE(e.has_value());
    ASSERT_TRUE(acc->DeleteEdge(&*e).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }
  while (main.db.storage()->GetReplicaState(replicas[0]) != ReplicaState::READY) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // Replica GC removes the deleted edge's metadata cleanly.
  replica.db.storage()->FreeMemory();

  // Deleted edge gone on replica; surviving edge intact.
  {
    auto acc = replica.db.Access(memgraph::storage::READ);
    ASSERT_FALSE(acc->FindEdge(e0_gid, View::OLD).has_value());
    ASSERT_TRUE(acc->FindEdge(e1_gid, View::OLD).has_value());
  }
}

// ---------------------------------------------------------------------------
// Light-edge replication tests
// ---------------------------------------------------------------------------

class ReplicationTestLightEdge : public ::testing::Test {
 protected:
  std::filesystem::path storage_directory{std::filesystem::temp_directory_path() /
                                          "MG_test_unit_storage_v2_replication_light_edge"};
  std::filesystem::path repl_storage_directory{std::filesystem::temp_directory_path() /
                                               "MG_test_unit_storage_v2_replication_light_edge_repl"};

  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

  Config MakeLightEdgeConfig(const std::filesystem::path &dir) const {
    Config config{
        .durability =
            {
                .root_data_directory = dir,
                .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            },
        .salient.items = {.properties_on_edges = true, .storage_light_edge = true},
    };
    UpdatePaths(config, dir);
    return config;
  }

  const std::string local_host = "127.0.0.1";
  const uint16_t port = 10'100;
  const std::string replica_name = "LIGHT_REPLICA";

 private:
  void Clear() {
    if (std::filesystem::exists(storage_directory)) std::filesystem::remove_all(storage_directory);
    if (std::filesystem::exists(repl_storage_directory)) std::filesystem::remove_all(repl_storage_directory);
  }
};

/// Verify that edge create, edge set-property, and edge delete are all
/// replicated correctly when storage_light_edge=true on both main and replica.
TEST_F(ReplicationTestLightEdge, EdgeCrudReplicatedSynchronously) {
  MinMemgraph main(MakeLightEdgeConfig(storage_directory));
  MinMemgraph replica(MakeLightEdgeConfig(repl_storage_directory));

  replica.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{.repl_server = Endpoint(local_host, port)});

  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replica_name,
                      .mode = ReplicationMode::SYNC,
                      .repl_server_endpoint = Endpoint(local_host, port),
                  })
                  .has_value());

  const auto *edge_type_name = "light_et";
  const auto *edge_prop_name = "eprop";
  const auto *edge_prop_value = "hello_light";

  Gid v1_gid, v2_gid;
  Gid edge_gid;

  // Create two vertices + one edge with a property.
  {
    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    auto edge_res = acc->CreateEdge(&v1, &v2, main.db.storage()->NameToEdgeType(edge_type_name));
    ASSERT_TRUE(edge_res.has_value());
    auto edge = edge_res.value();
    edge_gid = edge.Gid();
    ASSERT_TRUE(edge.SetProperty(main.db.storage()->NameToProperty(edge_prop_name), PropertyValue(edge_prop_value))
                    .has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }

  // Replica should see the edge and its property.
  {
    auto acc = replica.db.Access(memgraph::storage::WRITE);
    const auto v1 = acc->FindVertex(v1_gid, View::OLD);
    ASSERT_TRUE(v1.has_value());
    const auto out_edges = v1->OutEdges(View::OLD);
    ASSERT_TRUE(out_edges.has_value());
    ASSERT_EQ(out_edges->edges.size(), 1U);
    const auto &edge = out_edges->edges[0];
    ASSERT_EQ(edge.Gid(), edge_gid);
    ASSERT_EQ(edge.EdgeType(), replica.db.storage()->NameToEdgeType(edge_type_name));
    const auto props = edge.Properties(View::OLD);
    ASSERT_TRUE(props.has_value());
    ASSERT_EQ(props->size(), 1U);
    ASSERT_EQ(props->at(replica.db.storage()->NameToProperty(edge_prop_name)), PropertyValue(edge_prop_value));
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Update edge property in a separate transaction (exercises WAL SET_PROPERTY for light edges).
  const auto *updated_value = "updated_light";
  {
    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, View::OLD).value();
    auto out_edges = v1.OutEdges(View::OLD);
    ASSERT_TRUE(out_edges.has_value());
    auto edge = out_edges->edges[0];
    ASSERT_TRUE(
        edge.SetProperty(main.db.storage()->NameToProperty(edge_prop_name), PropertyValue(updated_value)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }

  {
    auto acc = replica.db.Access(memgraph::storage::WRITE);
    const auto v1 = acc->FindVertex(v1_gid, View::OLD).value();
    const auto out_edges = v1.OutEdges(View::OLD);
    ASSERT_TRUE(out_edges.has_value());
    const auto &edge = out_edges->edges[0];
    const auto props = edge.Properties(View::OLD);
    ASSERT_TRUE(props.has_value());
    ASSERT_EQ(props->at(replica.db.storage()->NameToProperty(edge_prop_name)), PropertyValue(updated_value));
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Delete the edge.
  {
    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, View::OLD).value();
    auto out_edges = v1.OutEdges(View::OLD);
    ASSERT_TRUE(out_edges.has_value());
    auto edge = out_edges->edges[0];
    ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }

  {
    auto acc = replica.db.Access(memgraph::storage::WRITE);
    const auto v1 = acc->FindVertex(v1_gid, View::OLD).value();
    const auto out_edges = v1.OutEdges(View::OLD);
    ASSERT_TRUE(out_edges.has_value());
    ASSERT_TRUE(out_edges->edges.empty());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
}

/// Verify that a replica recovers from snapshot+WAL written with light edges
/// and then accepts live replication of further changes.
TEST_F(ReplicationTestLightEdge, RecoveryProcess) {
  Gid v1_gid, v2_gid, edge_gid;

  // Phase 1: write data and snapshot to disk.
  {
    auto conf = MakeLightEdgeConfig(storage_directory);
    conf.durability.snapshot_on_exit = true;
    MinMemgraph main(conf);

    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    auto edge_res = acc->CreateEdge(&v1, &v2, main.db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge_res.has_value());
    edge_gid = edge_res->Gid();
    ASSERT_TRUE(edge_res->SetProperty(main.db.storage()->NameToProperty("p"), PropertyValue(42)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }

  // Phase 2: recover from snapshot, add a label, then register replica.
  auto conf = MakeLightEdgeConfig(storage_directory);
  conf.durability.recover_on_startup = true;
  MinMemgraph main(conf);

  // Verify recovery from snapshot restored the edge.
  {
    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, View::OLD);
    ASSERT_TRUE(v1.has_value());
    auto out_edges = v1->OutEdges(View::OLD);
    ASSERT_TRUE(out_edges.has_value());
    ASSERT_EQ(out_edges->edges.size(), 1U);
    ASSERT_EQ(out_edges->edges[0].Gid(), edge_gid);
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }

  // Register replica (it will receive the snapshot for catch-up).
  MinMemgraph replica(MakeLightEdgeConfig(repl_storage_directory));
  replica.repl_handler.TrySetReplicationRoleReplica(ReplicationServerConfig{.repl_server = Endpoint(local_host, port)});
  ASSERT_TRUE(main.repl_handler
                  .TryRegisterReplica(ReplicationClientConfig{
                      .name = replica_name,
                      .mode = ReplicationMode::SYNC,
                      .repl_server_endpoint = Endpoint(local_host, port),
                  })
                  .has_value());

  while (main.db.storage()->GetReplicaState(replica_name) != ReplicaState::READY) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // Send one more live transaction; replica must apply it.
  {
    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, View::OLD).value();
    ASSERT_TRUE(v1.AddLabel(main.db.storage()->NameToLabel("recovered")).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
  }

  {
    auto acc = replica.db.Access(memgraph::storage::WRITE);
    const auto v1 = acc->FindVertex(v1_gid, View::OLD);
    ASSERT_TRUE(v1.has_value());
    const auto labels = v1->Labels(View::OLD);
    ASSERT_TRUE(labels.has_value());
    ASSERT_THAT(*labels, ::testing::Contains(replica.db.storage()->NameToLabel("recovered")));
    // Edge must also be present on replica after recovery.
    const auto out_edges = v1->OutEdges(View::OLD);
    ASSERT_TRUE(out_edges.has_value());
    ASSERT_EQ(out_edges->edges.size(), 1U);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
}

// ---- Pipelined commit (--experimental-enabled=lockfree-read-snapshot,pipelined-commit) --------------------------

namespace {

using memgraph::storage::CommitProbe;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::PropertyValue;
using memgraph::storage::ReplicationTestHooks;

template <class Pred>
bool WaitFor(Pred pred, std::chrono::milliseconds timeout) {
  auto const deadline = std::chrono::steady_clock::now() + timeout;
  while (!pred()) {
    if (std::chrono::steady_clock::now() > deadline) return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return true;
}

bool WaitForReplicaState(MinMemgraph &main, std::string const &name, ReplicaState state,
                         std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
  return WaitFor([&] { return main.db.storage()->GetReplicaState(name) == state; }, timeout);
}

auto MainStorage(MinMemgraph &main) -> InMemoryStorage * { return static_cast<InMemoryStorage *>(main.db.storage()); }

auto LastDurableTimestamp(MinMemgraph &instance) -> uint64_t {
  return instance.db.storage()->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_;
}

auto NumCommittedTxns(MinMemgraph &instance) -> uint64_t {
  return instance.db.storage()->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).num_committed_txns_;
}

// Polls the replica's durable timestamp until it reaches main's.
bool WaitForReplicaToCatchUp(MinMemgraph &main, MinMemgraph &replica,
                             std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
  return WaitFor([&] { return LastDurableTimestamp(replica) >= LastDurableTimestamp(main); }, timeout);
}

std::optional<int64_t> ReadIntProperty(MinMemgraph &instance, Gid gid, std::string const &property) {
  const memgraph::memory::DbArenaScope arena_scope{&instance.db.Arena()};
  auto acc = instance.db.Access(memgraph::storage::READ);
  auto v = acc->FindVertex(gid, View::OLD);
  if (!v) return std::nullopt;
  auto const value = v->GetProperty(instance.db.storage()->NameToProperty(property), View::OLD);
  if (!value.has_value() || !value->IsInt()) return std::nullopt;
  return value->ValueInt();
}

// A payload larger than one SLK segment, so a partially transmitted request must be poisoned by the
// shutdown-while-locked rule rather than left on the connection.
std::string LargePayload() { return std::string(2 * memgraph::slk::kSegmentMaxDataSize, 'x'); }

// Installs test hooks and a probe on a storage and clears them on every exit, so a fatal assertion cannot leave a
// heartbeat or a scheduled task pointing at destroyed stack objects. Declared before any worker or wait.
class HookGuard {
 public:
  HookGuard(InMemoryStorage *storage, ReplicationTestHooks *hooks, CommitProbe *probe = nullptr) : storage_{storage} {
    if (hooks != nullptr) storage_->SetReplicationTestHooks(hooks);
    if (probe != nullptr) storage_->SetCommitProbe(probe);
  }

  HookGuard(HookGuard const &) = delete;
  HookGuard &operator=(HookGuard const &) = delete;

  ~HookGuard() {
    storage_->SetCommitProbe(nullptr);
    storage_->SetReplicationTestHooks(nullptr);
  }

 private:
  InMemoryStorage *storage_;
};

auto ReplicaStorage(MinMemgraph &replica) -> InMemoryStorage * {
  return static_cast<InMemoryStorage *>(replica.db.storage());
}

// Runs `f` on a thread and always releases `release` and joins on destruction, so an assertion that returns from
// the test body cannot destroy a joinable thread or the state the worker still references.
class JoinedThread {
 public:
  template <class F, class R>
  JoinedThread(F f, R release) : release_{std::move(release)}, thread_{std::move(f)} {}

  JoinedThread(JoinedThread const &) = delete;
  JoinedThread &operator=(JoinedThread const &) = delete;

  void Join() {
    if (thread_.joinable()) thread_.join();
  }

  ~JoinedThread() {
    release_();
    Join();
  }

 private:
  std::function<void()> release_;
  std::thread thread_;
};

}  // namespace

class PipelinedReplicationTest : public ReplicationTest {
 protected:
  void SetUp() override {
    ReplicationTest::SetUp();
    main_conf.experimental_lockfree_read_snapshot = true;
    main_conf.experimental_pipelined_commit = true;
    main_conf.durability.wal_file_flush_every_n_tx = 1;
  }

  // Registers `mode` replica REPLICA1 on ports[0] and waits for READY.
  void Register(MinMemgraph &main, MinMemgraph &replica, ReplicationMode mode, std::string const &name = "REPLICA1",
                uint16_t port = 10'000) {
    replica.repl_handler.TrySetReplicationRoleReplica(
        ReplicationServerConfig{.repl_server = Endpoint(local_host, port)});
    auto const reg = main.repl_handler.TryRegisterReplica(
        ReplicationClientConfig{.name = name, .mode = mode, .repl_server_endpoint = Endpoint(local_host, port)});
    ASSERT_TRUE(reg.has_value()) << static_cast<int>(reg.error());
    ASSERT_TRUE(WaitForReplicaState(main, name, ReplicaState::READY));
  }

  Gid Seed(MinMemgraph &main, int value) {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    EXPECT_TRUE(v.SetProperty(main.db.storage()->NameToProperty("p"), PropertyValue(value)).has_value());
    EXPECT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());
    return v.Gid();
  }

  // Sets p=value (and a large payload when asked) on `gid`; returns the outcome or rethrows a runtime_error as
  // `threw`.
  auto Update(MinMemgraph &main, Gid gid, int value, bool large, bool *threw = nullptr)
      -> std::expected<void, memgraph::storage::StorageManipulationError> {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = main.db.Access(memgraph::storage::WRITE);
    auto v = acc->FindVertex(gid, View::NEW);
    EXPECT_TRUE(v.has_value());
    if (!v.has_value()) return std::unexpected{memgraph::storage::SerializationError{}};
    EXPECT_TRUE(v->SetProperty(main.db.storage()->NameToProperty("p"), PropertyValue(value)).has_value());
    if (large) {
      EXPECT_TRUE(v->SetProperty(main.db.storage()->NameToProperty("blob"), PropertyValue(LargePayload())).has_value());
    }
    try {
      return acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc));
    } catch (std::runtime_error const &) {
      if (threw != nullptr) *threw = true;
      return {};
    }
  }

  // Main-side invariants after a nonfatal ticketed abort.
  void ExpectNothingCommitted(MinMemgraph &main, uint64_t watermark_before, uint64_t ldt_before,
                              uint64_t committed_before) {
    EXPECT_EQ(MainStorage(main)->LastCommittedMvccTimestamp(), watermark_before);
    EXPECT_EQ(LastDurableTimestamp(main), ldt_before);
    EXPECT_EQ(NumCommittedTxns(main), committed_before);
    EXPECT_EQ(MainStorage(main)->commit_order_gate_for_tests().Pending(), 0);
  }
};

// An eligible write with a STRICT_SYNC replica takes the ordered legacy continuation (2PC) and lands on the replica.
TEST_F(PipelinedReplicationTest, StrictSyncReplicaTakesTheOrderedLegacyContinuation) {
  MinMemgraph main(main_conf);
  MinMemgraph replica(repl_conf);
  Register(main, replica, ReplicationMode::STRICT_SYNC);
  auto const gid = Seed(main, 1);
  auto const fallbacks_before = MainStorage(main)->pipeline_stats_for_tests().two_pc_fallbacks.load();
  EXPECT_TRUE(Update(main, gid, 2, /*large=*/false).has_value());
  EXPECT_EQ(MainStorage(main)->pipeline_stats_for_tests().two_pc_fallbacks.load() - fallbacks_before, 1);
  EXPECT_TRUE(WaitForReplicaToCatchUp(main, replica));
  EXPECT_EQ(ReadIntProperty(replica, gid, "p"), 2);
  EXPECT_EQ(MainStorage(main)->commit_order_gate_for_tests().Pending(), 0);
}

// Failed-2PC regression (a): a prepared replica that votes no. Main aborts nonfatally in order, its progress is
// unchanged, the ticket retires, the replica's preparation is explicitly abandoned, a successor commits, and the
// aborted transaction is excluded on recovery.
TEST_F(PipelinedReplicationTest, FailedPrepareVoteAbortsInOrder) {
  std::optional<Gid> gid;
  {
    MinMemgraph main(main_conf);
    MinMemgraph replica(repl_conf);
    Register(main, replica, ReplicationMode::STRICT_SYNC);
    auto const seeded = Seed(main, 1);
    ASSERT_TRUE(WaitForReplicaToCatchUp(main, replica));

    ReplicationTestHooks replica_hooks;
    std::atomic<int> refusals{0};
    std::atomic<int> aborts_applied{0};
    replica_hooks.refuse_next_prepare = [&](uint64_t) { return refusals.fetch_add(1) == 0; };
    replica_hooks.on_abort_applied = [&](uint64_t) { ++aborts_applied; };
    HookGuard replica_guard{ReplicaStorage(replica), &replica_hooks};
    ReplicationTestHooks main_hooks;
    std::atomic<int> decision_results{0};
    std::atomic<bool> last_decision_ok{false};
    main_hooks.on_abort_decision_result = [&](uint64_t, bool ok) {
      last_decision_ok = ok;
      ++decision_results;
    };
    HookGuard main_guard{MainStorage(main), &main_hooks};

    auto const watermark_before = MainStorage(main)->LastCommittedMvccTimestamp();
    auto const ldt_before = LastDurableTimestamp(main);
    auto const committed_before = NumCommittedTxns(main);
    auto const result = Update(main, seeded, 2, /*large=*/false);
    ASSERT_FALSE(result.has_value());
    ASSERT_TRUE(std::holds_alternative<memgraph::storage::ReplicationError>(result.error()));
    EXPECT_FALSE(std::get<memgraph::storage::ReplicationError>(result.error()).transaction_committed);
    ExpectNothingCommitted(main, watermark_before, ldt_before, committed_before);
    EXPECT_TRUE(WaitFor([&] { return aborts_applied.load() == 1; }, std::chrono::seconds(10)));
    EXPECT_EQ(decision_results.load(), 1);
    EXPECT_TRUE(last_decision_ok.load());
    EXPECT_EQ(ReadIntProperty(main, seeded, "p"), 1);

    // A negative prepare response moves the client to MAYBE_BEHIND; wait for READY before the successor.
    ASSERT_TRUE(WaitForReplicaState(main, "REPLICA1", ReplicaState::READY));
    EXPECT_TRUE(Update(main, seeded, 3, /*large=*/false).has_value());
    EXPECT_TRUE(WaitForReplicaToCatchUp(main, replica));
    EXPECT_EQ(ReadIntProperty(replica, seeded, "p"), 3);
    gid = seeded;
  }
  ASSERT_TRUE(gid.has_value());
  // Recovery excludes the aborted prepare record.
  main_conf.durability.recover_on_startup = true;
  MinMemgraph recovered(main_conf);
  EXPECT_EQ(ReadIntProperty(recovered, *gid, "p"), 3);
}

// Failed-2PC regression (b): an exception after the complete commit=false prepare record, once the replica has cached
// its prepared accessor and answered. The unwind guard sends the abort decision exactly once.
TEST_F(PipelinedReplicationTest, ExceptionAfterCompletePrepareRecordAbortsInOrder) {
  MinMemgraph main(main_conf);
  MinMemgraph replica(repl_conf);
  Register(main, replica, ReplicationMode::STRICT_SYNC);
  auto const gid = Seed(main, 1);
  ASSERT_TRUE(WaitForReplicaToCatchUp(main, replica));

  ReplicationTestHooks replica_hooks;
  std::latch prepared{1};
  std::atomic<int> prepared_count{0};
  std::atomic<int> aborts_applied{0};
  replica_hooks.on_prepared = [&](uint64_t) {
    if (prepared_count.fetch_add(1) == 0) prepared.count_down();
  };
  replica_hooks.on_abort_applied = [&](uint64_t) { ++aborts_applied; };
  HookGuard replica_guard{ReplicaStorage(replica), &replica_hooks};
  ReplicationTestHooks main_hooks;
  std::atomic<int> decision_results{0};
  main_hooks.on_abort_decision_result = [&](uint64_t, bool) { ++decision_results; };
  HookGuard main_guard{MainStorage(main), &main_hooks};
  CommitProbe probe;
  std::atomic<bool> armed{true};
  probe.after_prepare_record = [&] {
    if (!armed.exchange(false)) return;
    prepared.wait();  // the replica has cached its prepared accessor and sent its vote
    throw std::runtime_error("injected after_prepare_record");
  };
  MainStorage(main)->SetCommitProbe(&probe);

  auto const decision_calls_before = MainStorage(main)->pipeline_test_counters().decision_calls.load();
  auto const watermark_before = MainStorage(main)->LastCommittedMvccTimestamp();
  auto const ldt_before = LastDurableTimestamp(main);
  auto const committed_before = NumCommittedTxns(main);
  bool threw = false;
  static_cast<void>(Update(main, gid, 2, /*large=*/false, &threw));
  EXPECT_TRUE(threw);
  ExpectNothingCommitted(main, watermark_before, ldt_before, committed_before);
  EXPECT_EQ(MainStorage(main)->pipeline_test_counters().decision_calls.load() - decision_calls_before, 1);
  EXPECT_EQ(decision_results.load(), 1);
  EXPECT_TRUE(WaitFor([&] { return aborts_applied.load() == 1; }, std::chrono::seconds(10)));
  MainStorage(main)->SetCommitProbe(nullptr);

  ASSERT_TRUE(WaitForReplicaState(main, "REPLICA1", ReplicaState::READY));
  EXPECT_TRUE(Update(main, gid, 3, /*large=*/false).has_value());
  EXPECT_TRUE(WaitForReplicaToCatchUp(main, replica));
  EXPECT_EQ(ReadIntProperty(replica, gid, "p"), 3);
}

// Nondeath returned-false abort decision, failed-vote caller: the replica answers the abort decision with failure.
// Nonfatal completion, one local abort, decision_ok observed false, successor succeeds.
TEST_F(PipelinedReplicationTest, RefusedAbortDecisionAfterFailedVoteIsNotFatal) {
  MinMemgraph main(main_conf);
  MinMemgraph replica(repl_conf);
  Register(main, replica, ReplicationMode::STRICT_SYNC);
  auto const gid = Seed(main, 1);
  ASSERT_TRUE(WaitForReplicaToCatchUp(main, replica));

  ReplicationTestHooks replica_hooks;
  std::atomic<int> refusals{0};
  std::atomic<int> decision_refusals{0};
  replica_hooks.refuse_next_prepare = [&](uint64_t) { return refusals.fetch_add(1) == 0; };
  replica_hooks.refuse_next_abort_decision = [&](uint64_t) { return decision_refusals.fetch_add(1) == 0; };
  HookGuard replica_guard{ReplicaStorage(replica), &replica_hooks};
  ReplicationTestHooks main_hooks;
  std::atomic<int> decision_results{0};
  std::atomic<bool> last_decision_ok{true};
  main_hooks.on_abort_decision_result = [&](uint64_t, bool ok) {
    last_decision_ok = ok;
    ++decision_results;
  };
  HookGuard main_guard{MainStorage(main), &main_hooks};

  auto const watermark_before = MainStorage(main)->LastCommittedMvccTimestamp();
  auto const ldt_before = LastDurableTimestamp(main);
  auto const committed_before = NumCommittedTxns(main);
  auto const result = Update(main, gid, 2, /*large=*/false);
  ASSERT_FALSE(result.has_value());
  ExpectNothingCommitted(main, watermark_before, ldt_before, committed_before);
  EXPECT_EQ(decision_results.load(), 1);
  EXPECT_FALSE(last_decision_ok.load());
  EXPECT_EQ(decision_refusals.load(), 1);

  ASSERT_TRUE(WaitForReplicaState(main, "REPLICA1", ReplicaState::READY));
  EXPECT_TRUE(Update(main, gid, 3, /*large=*/false).has_value());
  EXPECT_TRUE(WaitForReplicaToCatchUp(main, replica));
  EXPECT_EQ(ReadIntProperty(replica, gid, "p"), 3);
}

// Nondeath returned-false abort decision, unwind caller (after_prepare_record exception).
TEST_F(PipelinedReplicationTest, RefusedAbortDecisionAfterUnwindIsNotFatal) {
  MinMemgraph main(main_conf);
  MinMemgraph replica(repl_conf);
  Register(main, replica, ReplicationMode::STRICT_SYNC);
  auto const gid = Seed(main, 1);
  ASSERT_TRUE(WaitForReplicaToCatchUp(main, replica));

  ReplicationTestHooks replica_hooks;
  std::latch prepared{1};
  std::atomic<int> prepared_count{0};
  std::atomic<int> decision_refusals{0};
  replica_hooks.on_prepared = [&](uint64_t) {
    if (prepared_count.fetch_add(1) == 0) prepared.count_down();
  };
  replica_hooks.refuse_next_abort_decision = [&](uint64_t) { return decision_refusals.fetch_add(1) == 0; };
  HookGuard replica_guard{ReplicaStorage(replica), &replica_hooks};
  ReplicationTestHooks main_hooks;
  std::atomic<int> decision_results{0};
  std::atomic<bool> last_decision_ok{true};
  main_hooks.on_abort_decision_result = [&](uint64_t, bool ok) {
    last_decision_ok = ok;
    ++decision_results;
  };
  HookGuard main_guard{MainStorage(main), &main_hooks};
  CommitProbe probe;
  std::atomic<bool> armed{true};
  probe.after_prepare_record = [&] {
    if (!armed.exchange(false)) return;
    prepared.wait();
    throw std::runtime_error("injected after_prepare_record");
  };
  MainStorage(main)->SetCommitProbe(&probe);

  auto const watermark_before = MainStorage(main)->LastCommittedMvccTimestamp();
  auto const ldt_before = LastDurableTimestamp(main);
  auto const committed_before = NumCommittedTxns(main);
  bool threw = false;
  static_cast<void>(Update(main, gid, 2, /*large=*/false, &threw));
  EXPECT_TRUE(threw);
  ExpectNothingCommitted(main, watermark_before, ldt_before, committed_before);
  EXPECT_EQ(decision_results.load(), 1);
  EXPECT_FALSE(last_decision_ok.load());
  MainStorage(main)->SetCommitProbe(nullptr);

  ASSERT_TRUE(WaitForReplicaState(main, "REPLICA1", ReplicaState::READY));
  EXPECT_TRUE(Update(main, gid, 3, /*large=*/false).has_value());
  EXPECT_TRUE(WaitForReplicaToCatchUp(main, replica));
  EXPECT_EQ(ReadIntProperty(replica, gid, "p"), 3);
}

// 3b: live-borrower abort. A real replica task waits on the WAL promise while after_schedule_ship throws. The abort is
// bounded and nonfatal (not_started: streams discarded, no WAL finalization), the borrower completes before the
// command-owning scope is released, one retirement, READY handshake, successful successor.
class PipelinedLiveBorrowerTest : public PipelinedReplicationTest {
 protected:
  enum class Variant { kDirectSyncPipeline, kLocalSyncLegacyFallback, kBorrowedStrictSyncFallback };

  void Run(Variant variant) {
    MinMemgraph main(main_conf);
    MinMemgraph replica(repl_conf);
    auto const mode =
        variant == Variant::kBorrowedStrictSyncFallback ? ReplicationMode::STRICT_SYNC : ReplicationMode::SYNC;
    Register(main, replica, mode);
    auto const gid = Seed(main, 1);
    ASSERT_TRUE(WaitForReplicaToCatchUp(main, replica));
    auto *storage = MainStorage(main);
    auto const finalizations_before = storage->pipeline_test_counters().finalize_wal_calls.load();
    auto const decisions_before = storage->pipeline_test_counters().decision_calls.load();

    ReplicationTestHooks hooks;
    std::latch borrower_waiting{1};
    std::atomic<int> borrower_waits{0};
    std::atomic<int> tasks_done{0};
    std::atomic<int> tasks_done_at_release{-1};
    std::atomic<int> commands_released{0};
    std::string const expected_scope = variant == Variant::kDirectSyncPipeline ? "pipeline" : "legacy";
    hooks.before_wal_result_wait = [&](std::string const &, uint64_t) {
      if (borrower_waits.fetch_add(1) == 0) borrower_waiting.count_down();  // once; the successor fires it too
    };
    hooks.on_task_done = [&](std::string const &, uint64_t) { ++tasks_done; };
    hooks.on_commands_released = [&](std::string_view scope, uint64_t) {
      if (scope != expected_scope) return;
      tasks_done_at_release = tasks_done.load();
      ++commands_released;
    };
    HookGuard hook_guard{storage, &hooks};
    CommitProbe probe;
    std::atomic<bool> armed{true};
    probe.after_schedule_ship = [&] {
      if (!armed.exchange(false)) return;
      borrower_waiting.wait();  // the task is parked on the WAL gate with a partially transmitted request
      throw std::runtime_error("injected after_schedule_ship");
    };
    if (variant == Variant::kLocalSyncLegacyFallback) {
      // Force the ordered legacy fallback: the first ticketed commit after installation is refused at the materializer.
      probe.budget_refuse.site = memgraph::storage::BudgetRefuse::kMaterializer;
      probe.after_mint = [&probe] {
        if (probe.budget_refuse.ticket.load() == 0) probe.budget_refuse.ticket = probe.minted_ticket.load();
      };
    }
    storage->SetCommitProbe(&probe);

    auto const watermark_before = storage->LastCommittedMvccTimestamp();
    auto const ldt_before = LastDurableTimestamp(main);
    auto const committed_before = NumCommittedTxns(main);
    bool threw = false;
    static_cast<void>(Update(main, gid, 2, /*large=*/true, &threw));
    EXPECT_TRUE(threw);
    ExpectNothingCommitted(main, watermark_before, ldt_before, committed_before);
    EXPECT_EQ(storage->pipeline_test_counters().finalize_wal_calls.load(), finalizations_before);
    EXPECT_EQ(storage->pipeline_test_counters().decision_calls.load(), decisions_before);
    EXPECT_EQ(commands_released.load(), 1);
    EXPECT_EQ(tasks_done_at_release.load(), 1);  // the borrower finished before the commands were destroyed
    if (variant == Variant::kLocalSyncLegacyFallback) {
      EXPECT_TRUE(probe.budget_refuse.fired.load());
    }
    storage->SetCommitProbe(nullptr);

    // The discarded stream left the client MAYBE_BEHIND; reconciliation brings it back to READY.
    ASSERT_TRUE(WaitForReplicaState(main, "REPLICA1", ReplicaState::READY));
    EXPECT_TRUE(Update(main, gid, 3, /*large=*/false).has_value());
    EXPECT_TRUE(WaitForReplicaToCatchUp(main, replica));
    EXPECT_EQ(ReadIntProperty(replica, gid, "p"), 3);
  }
};

TEST_F(PipelinedLiveBorrowerTest, DirectSyncPipeline) { Run(Variant::kDirectSyncPipeline); }

TEST_F(PipelinedLiveBorrowerTest, LocallyOwnedSyncLegacyFallback) { Run(Variant::kLocalSyncLegacyFallback); }

TEST_F(PipelinedLiveBorrowerTest, BorrowedStrictSyncFallback) { Run(Variant::kBorrowedStrictSyncFallback); }

// 9b: all-unscheduled unstarted-record unwind with a real replica: streams open, no shipping task exists.
class PipelinedUnscheduledAbortTest : public PipelinedReplicationTest {
 protected:
  enum class Site { kPipelineBeforeAppend, kLegacyBeforeMaterialize, kBorrowedStrictSyncBeforeMaterialize };

  void Run(Site site) {
    MinMemgraph main(main_conf);
    MinMemgraph replica(repl_conf);
    auto const mode =
        site == Site::kBorrowedStrictSyncBeforeMaterialize ? ReplicationMode::STRICT_SYNC : ReplicationMode::SYNC;
    Register(main, replica, mode);
    auto const gid = Seed(main, 1);
    ASSERT_TRUE(WaitForReplicaToCatchUp(main, replica));
    auto *storage = MainStorage(main);
    auto const finalizations_before = storage->pipeline_test_counters().finalize_wal_calls.load();
    auto const decisions_before = storage->pipeline_test_counters().decision_calls.load();
    uint64_t aborts_before = 0;
    storage->repl_storage_state_.replication_storage_clients_.WithReadLock(
        [&](auto const &clients) { aborts_before = clients.front()->abort_rpc_client_calls(); });

    CommitProbe probe;
    std::atomic<bool> armed{true};
    auto const fault = [&] {
      if (armed.exchange(false)) throw std::runtime_error("injected before any frame");
    };
    if (site == Site::kPipelineBeforeAppend) {
      probe.before_append = fault;
    } else {
      probe.before_legacy_materialize = fault;
    }
    storage->SetCommitProbe(&probe);

    auto const watermark_before = storage->LastCommittedMvccTimestamp();
    auto const ldt_before = LastDurableTimestamp(main);
    auto const committed_before = NumCommittedTxns(main);
    bool threw = false;
    if (site == Site::kLegacyBeforeMaterialize) {
      // An ineligible (metadata) transaction routed through the locally owned legacy continuation.
      const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
      auto acc = main.db.ReadOnlyAccess();
      ASSERT_TRUE(acc->CreateIndex(main.db.storage()->NameToLabel("L")).has_value());
      try {
        static_cast<void>(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)));
      } catch (std::runtime_error const &) {
        threw = true;
      }
    } else {
      static_cast<void>(Update(main, gid, 2, /*large=*/false, &threw));
    }
    EXPECT_TRUE(threw);
    ExpectNothingCommitted(main, watermark_before, ldt_before, committed_before);
    EXPECT_EQ(storage->pipeline_test_counters().finalize_wal_calls.load(), finalizations_before);
    EXPECT_EQ(storage->pipeline_test_counters().decision_calls.load(), decisions_before);
    uint64_t aborts_after = 0;
    storage->repl_storage_state_.replication_storage_clients_.WithReadLock(
        [&](auto const &clients) { aborts_after = clients.front()->abort_rpc_client_calls(); });
    EXPECT_EQ(aborts_after - aborts_before, 1);  // exactly one effective stream cleanup
    storage->SetCommitProbe(nullptr);

    ASSERT_TRUE(WaitForReplicaState(main, "REPLICA1", ReplicaState::READY));
    EXPECT_TRUE(Update(main, gid, 3, /*large=*/false).has_value());
    EXPECT_TRUE(WaitForReplicaToCatchUp(main, replica));
    EXPECT_EQ(ReadIntProperty(replica, gid, "p"), 3);
  }
};

TEST_F(PipelinedUnscheduledAbortTest, PipelineBeforeAppend) { Run(Site::kPipelineBeforeAppend); }

TEST_F(PipelinedUnscheduledAbortTest, LegacyBeforeMaterialize) { Run(Site::kLegacyBeforeMaterialize); }

TEST_F(PipelinedUnscheduledAbortTest, BorrowedStrictSyncBeforeMaterialize) {
  Run(Site::kBorrowedStrictSyncBeforeMaterialize);
}

// 9c: partial replication-object construction. Two SYNC replicas; the constructor throws before opening the second
// stream, after the first was retained: the rollback shuts the first stream down, resets it and marks its client
// MAYBE_BEHIND, then a nonfatal abort, the READY handshake, and a successful successor.
TEST_F(PipelinedReplicationTest, PartialReplicationConstructionRollsBackTheOpenedStream) {
  MinMemgraph main(main_conf);
  MinMemgraph replica1(repl_conf);
  MinMemgraph replica2(repl2_conf);
  Register(main, replica1, ReplicationMode::SYNC, "REPLICA1", ports[0]);
  Register(main, replica2, ReplicationMode::SYNC, "REPLICA2", ports[1]);
  auto const gid = Seed(main, 1);
  ASSERT_TRUE(WaitForReplicaToCatchUp(main, replica1));
  ASSERT_TRUE(WaitForReplicaToCatchUp(main, replica2));
  auto *storage = MainStorage(main);
  uint64_t aborts_before = 0;
  storage->repl_storage_state_.replication_storage_clients_.WithReadLock(
      [&](auto const &clients) { aborts_before = clients.front()->abort_rpc_client_calls(); });

  ReplicationTestHooks hooks;
  std::atomic<bool> armed{true};
  hooks.throw_on_open_for = [&](std::string const &name, uint64_t) {
    return name == "REPLICA2" && armed.exchange(false);
  };
  HookGuard hook_guard{storage, &hooks};

  auto const watermark_before = storage->LastCommittedMvccTimestamp();
  auto const ldt_before = LastDurableTimestamp(main);
  auto const committed_before = NumCommittedTxns(main);
  bool threw = false;
  static_cast<void>(Update(main, gid, 2, /*large=*/false, &threw));
  EXPECT_TRUE(threw);
  ExpectNothingCommitted(main, watermark_before, ldt_before, committed_before);
  uint64_t aborts_after = 0;
  storage->repl_storage_state_.replication_storage_clients_.WithReadLock(
      [&](auto const &clients) { aborts_after = clients.front()->abort_rpc_client_calls(); });
  EXPECT_EQ(aborts_after - aborts_before, 1);

  ASSERT_TRUE(WaitForReplicaState(main, "REPLICA1", ReplicaState::READY));
  ASSERT_TRUE(WaitForReplicaState(main, "REPLICA2", ReplicaState::READY));
  EXPECT_TRUE(Update(main, gid, 3, /*large=*/false).has_value());
  EXPECT_TRUE(WaitForReplicaToCatchUp(main, replica1));
  EXPECT_TRUE(WaitForReplicaToCatchUp(main, replica2));
  EXPECT_EQ(ReadIntProperty(replica1, gid, "p"), 3);
  EXPECT_EQ(ReadIntProperty(replica2, gid, "p"), 3);
}

// 9f: the before_legacy_materialize probe is gated on the ticket, never on the flag: with both flags on the replica
// too, replicated writes never invoke it, while a locally owned main-side legacy commit invokes it exactly once.
TEST_F(PipelinedReplicationTest, LegacyMaterializeProbeIsTicketGatedNotFlagGated) {
  repl_conf.experimental_lockfree_read_snapshot = true;
  repl_conf.experimental_pipelined_commit = true;
  MinMemgraph main(main_conf);
  MinMemgraph replica(repl_conf);
  Register(main, replica, ReplicationMode::SYNC);
  CommitProbe replica_probe;
  std::atomic<int> replica_invocations{0};
  replica_probe.before_legacy_materialize = [&] { ++replica_invocations; };
  replica.db.storage()->SetCommitProbe(&replica_probe);
  CommitProbe main_probe;
  std::atomic<int> main_invocations{0};
  main_probe.before_legacy_materialize = [&] { ++main_invocations; };
  main.db.storage()->SetCommitProbe(&main_probe);

  auto const gid = Seed(main, 1);  // pipelined: no legacy materialization on main
  EXPECT_TRUE(Update(main, gid, 2, /*large=*/false).has_value());
  {
    const memgraph::memory::DbArenaScope arena_scope{&main.db.Arena()};
    auto acc = main.db.ReadOnlyAccess();
    ASSERT_TRUE(acc->CreateIndex(main.db.storage()->NameToLabel("L")).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(MakeCommitArgs(main.db_acc)).has_value());  // legacy, once
  }
  EXPECT_TRUE(WaitForReplicaToCatchUp(main, replica));
  EXPECT_EQ(main_invocations.load(), 1);
  EXPECT_EQ(replica_invocations.load(), 0);
  replica.db.storage()->SetCommitProbe(nullptr);
  main.db.storage()->SetCommitProbe(nullptr);
}

// Task 7: replica ordering and counts under concurrent pipelined writers. Live replication is distinguished from
// recovery: a healthy SYNC stream moves the client READY<->REPLICATING only.
TEST_F(PipelinedReplicationTest, ConcurrentWritersReplicateInOrderWithConsistentCounts) {
  MinMemgraph main(main_conf);
  MinMemgraph replica(repl_conf);
  Register(main, replica, ReplicationMode::SYNC);
  constexpr int kWriters = 8;
  constexpr int kRounds = 20;
  std::vector<Gid> gids;
  for (int i = 0; i < kWriters; ++i) gids.push_back(Seed(main, 0));
  ASSERT_TRUE(WaitForReplicaToCatchUp(main, replica));
  auto const committed_before = NumCommittedTxns(main);

  ReplicationTestHooks replica_hooks;
  std::mutex prepared_mutex;
  std::vector<uint64_t> prepared_timestamps;
  replica_hooks.on_prepared = [&](uint64_t ts) {
    auto guard = std::lock_guard{prepared_mutex};
    prepared_timestamps.push_back(ts);
  };
  HookGuard replica_guard{ReplicaStorage(replica), &replica_hooks};

  std::atomic<bool> stop{false};
  std::atomic<bool> bad_state_seen{false};
  std::thread state_watcher{[&] {
    while (!stop.load()) {
      auto const state = main.db.storage()->GetReplicaState("REPLICA1");
      if (state != ReplicaState::READY && state != ReplicaState::REPLICATING) bad_state_seen = true;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }};
  std::vector<std::thread> writers;
  std::atomic<int> failures{0};
  for (int w = 0; w < kWriters; ++w) {
    writers.emplace_back([&, w] {
      for (int r = 0; r < kRounds; ++r) {
        if (!Update(main, gids[w], r, /*large=*/false).has_value()) ++failures;
      }
    });
  }
  for (auto &t : writers) t.join();
  stop = true;
  state_watcher.join();
  EXPECT_EQ(failures.load(), 0);
  EXPECT_FALSE(bad_state_seen.load());
  ASSERT_TRUE(WaitForReplicaToCatchUp(main, replica));
  for (int w = 0; w < kWriters; ++w) EXPECT_EQ(ReadIntProperty(replica, gids[w], "p"), kRounds - 1);
  {
    auto guard = std::lock_guard{prepared_mutex};
    EXPECT_EQ(prepared_timestamps.size(), kWriters * kRounds);
    EXPECT_TRUE(std::ranges::is_sorted(prepared_timestamps));
    EXPECT_TRUE(std::ranges::adjacent_find(prepared_timestamps) == prepared_timestamps.end());  // strictly increasing
  }
  EXPECT_EQ(NumCommittedTxns(main) - committed_before, kWriters * kRounds);
  uint64_t replica_cached = 0;
  MainStorage(main)->repl_storage_state_.replication_storage_clients_.WithReadLock(
      [&](auto const &clients) { replica_cached = clients.front()->GetNumCommittedTxns(); });
  EXPECT_EQ(replica_cached, NumCommittedTxns(main));
  EXPECT_EQ(MainStorage(main)->commit_order_gate_for_tests().Pending(), 0);
}

// ---- Process-isolated multi-replica cases ----------------------------------------------------------------------

namespace {

using memgraph::tests::ReplicaProcess;

std::filesystem::path ProcessReplicaDir(int index) {
  return std::filesystem::temp_directory_path() /
         ("MG_test_unit_storage_v2_replication_proc" + std::to_string(index) + "_" + std::to_string(getpid()));
}

extern "C" void ReplicationWatchdogHandler(int) { _exit(124); }

void ArmReplicationWatchdog(unsigned seconds) {
  struct sigaction action{};
  action.sa_handler = ReplicationWatchdogHandler;
  sigemptyset(&action.sa_mask);
  sigaction(SIGALRM, &action, nullptr);
  alarm(seconds);
  // A death child aborts on purpose; it must not leave a core file behind.
  struct rlimit limit{};
  limit.rlim_cur = 0;
  limit.rlim_max = 0;
  setrlimit(RLIMIT_CORE, &limit);
}

size_t CountLines(std::filesystem::path const &path) {
  std::ifstream file{path};
  size_t lines = 0;
  std::string line;
  while (std::getline(file, line)) {
    if (!line.empty()) ++lines;
  }
  return lines;
}

}  // namespace

// 8: abort-continuation-step death. Two STRICT_SYNC replicas in their own processes (so a "later decision" exists),
// a failed prepare vote from the first, and exactly one step of AbortTwoPcOrTerminate throwing once: the main
// terminates, and the attempt marker shows exactly one attempt for the selected step (no retry).
class PipelinedAbortStepDeathTest : public PipelinedReplicationTest, public ::testing::WithParamInterface<std::string> {
 protected:
  // The controller (the gtest parent) owns the replica processes; the death child inherits their ports through
  // the environment and must not spawn its own.
  void SetUp() override {
    PipelinedReplicationTest::SetUp();
    if (::testing::internal::InDeathTestChild()) return;
    replicas_.push_back(std::make_unique<ReplicaProcess>(ports[0], ProcessReplicaDir(0)));
    replicas_.push_back(std::make_unique<ReplicaProcess>(ports[1], ProcessReplicaDir(1)));
    for (auto &replica : replicas_) ASSERT_TRUE(replica->WaitReady());
    marker_ = std::filesystem::temp_directory_path() / ("MG_abort_step_marker_" + std::to_string(getpid()));
    go_file_ = std::filesystem::temp_directory_path() / ("MG_abort_step_go_" + std::to_string(getpid()));
    std::filesystem::remove(marker_);
    std::filesystem::remove(go_file_);
    setenv("MG_ABORT_STEP_MARKER", marker_.c_str(), 1);
    setenv("MG_ABORT_STEP_GO", go_file_.c_str(), 1);
  }

  void TearDown() override {
    if (!::testing::internal::InDeathTestChild()) {
      if (controller_.joinable()) controller_.join();
      replicas_.clear();
      std::filesystem::remove(marker_);
      std::filesystem::remove(go_file_);
      unsetenv("MG_ABORT_STEP_MARKER");
      unsetenv("MG_ABORT_STEP_GO");
    }
    PipelinedReplicationTest::TearDown();
  }

  // The controller: once the seed transaction has been prepared on the first replica, arm its refusal and release
  // the main (which polls for the go file before issuing the faulting transaction).
  void StartController() {
    controller_ = std::thread{[this] {
      if (!replicas_[0]->WaitEvent("prepared", std::chrono::seconds(60))) return;
      if (!replicas_[0]->Send("refuse_prepare")) return;
      std::ofstream{go_file_} << "go\n";
    }};
  }

  // Runs in the death child.
  void RunMain(std::string const &step) {
    ArmReplicationWatchdog(120);
    MinMemgraph main(main_conf);
    for (int i = 0; i < 2; ++i) {
      auto const reg = main.repl_handler.TryRegisterReplica(
          ReplicationClientConfig{.name = replicas_names_[i],
                                  .mode = ReplicationMode::STRICT_SYNC,
                                  .repl_server_endpoint = Endpoint(local_host, ports[i])});
      if (!reg.has_value()) _exit(3);
      if (!WaitForReplicaState(main, replicas_names_[i], ReplicaState::READY)) _exit(3);
    }
    auto const gid = Seed(main, 1);
    // The controller arms the refusal after it saw the seed prepared, then releases us.
    std::filesystem::path const go_file{std::getenv("MG_ABORT_STEP_GO")};
    while (!std::filesystem::exists(go_file)) std::this_thread::sleep_for(std::chrono::milliseconds(10));
    CommitProbe probe;
    probe.fault_attempt_marker_path = std::getenv("MG_ABORT_STEP_MARKER");
    if (step == "finalize_wal") {
      probe.finalize_wal_throws_once = true;
    } else {
      probe.decision_schedule_throws_once = true;
    }
    MainStorage(main)->SetCommitProbe(&probe);
    // The first replica votes no.
    static_cast<void>(Update(main, gid, 2, /*large=*/false));
    _exit(5);  // the step did not throw
  }

  std::vector<std::unique_ptr<ReplicaProcess>> replicas_;
  std::array<std::string, 2> replicas_names_{"REPLICA1", "REPLICA2"};
  std::filesystem::path marker_;
  std::filesystem::path go_file_;
  std::thread controller_;
};

INSTANTIATE_TEST_SUITE_P(Steps, PipelinedAbortStepDeathTest, ::testing::Values("finalize_wal", "decision_schedule"));

TEST_P(PipelinedAbortStepDeathTest, StepThrowsOnceTerminatesWithoutRetry) {
  if (!::testing::internal::InDeathTestChild()) StartController();
  EXPECT_EXIT(RunMain(GetParam()), ::testing::KilledBySignal(SIGABRT), "");
  if (controller_.joinable()) controller_.join();
  EXPECT_EQ(CountLines(marker_), 1);
  // The controller reaps the replica processes in TearDown.
}

// 9e: mixed STRICT_SYNC + ASYNC complete-prepare abort with both replicas in their own processes, in both the
// refused-prepare and the after_prepare_record-exception forms, with the two forced interleavings.
class PipelinedMixedAbortTest : public PipelinedReplicationTest {
 protected:
  void SetUp() override {
    PipelinedReplicationTest::SetUp();
    strict_ = std::make_unique<ReplicaProcess>(ports[0], ProcessReplicaDir(0));
    async_ = std::make_unique<ReplicaProcess>(ports[1], ProcessReplicaDir(1));
    ASSERT_TRUE(strict_->WaitReady());
    ASSERT_TRUE(async_->WaitReady());
  }

  void TearDown() override {
    strict_.reset();
    async_.reset();
    PipelinedReplicationTest::TearDown();
  }

  void RegisterBoth(MinMemgraph &main) {
    for (auto const &[name, mode, port] : {std::tuple{"STRICT", ReplicationMode::STRICT_SYNC, ports[0]},
                                           std::tuple{"ASYNC", ReplicationMode::ASYNC, ports[1]}}) {
      auto const reg = main.repl_handler.TryRegisterReplica(ReplicationClientConfig{
          .name = name,
          .mode = mode,
          .repl_server_endpoint = Endpoint(local_host, port),
          .replica_check_frequency = std::chrono::seconds(1),
      });
      ASSERT_TRUE(reg.has_value()) << static_cast<int>(reg.error());
      ASSERT_TRUE(WaitForReplicaState(main, name, ReplicaState::READY));
    }
  }

  uint64_t AsyncAbortCalls(MinMemgraph &main) {
    uint64_t calls = 0;
    MainStorage(main)->repl_storage_state_.replication_storage_clients_.WithReadLock([&](auto const &clients) {
      for (auto const &client : clients) {
        if (client->Name() == "ASYNC") calls = client->abort_rpc_client_calls();
      }
    });
    return calls;
  }

  std::unique_ptr<ReplicaProcess> strict_;
  std::unique_ptr<ReplicaProcess> async_;
};

// Interleaving 1: the ASYNC shipping task is held at before_wal_result_wait while the STRICT_SYNC prepare
// completes and after_prepare_record throws. No decision call and no ASYNC cleanup happens until the borrower is
// released; the task's completion precedes the cleanup.
TEST_F(PipelinedMixedAbortTest, HeldAsyncBorrowerDelaysTheAbortContinuation) {
  MinMemgraph main(main_conf);
  RegisterBoth(main);
  auto const gid = Seed(main, 1);
  ASSERT_TRUE(strict_->WaitEvent("prepared", std::chrono::seconds(10)).has_value());
  // The ASYNC client returns to READY from its own finalize task; the faulting write needs it streaming.
  ASSERT_TRUE(WaitForReplicaState(main, "ASYNC", ReplicaState::READY));
  auto *storage = MainStorage(main);
  auto const async_aborts_before = AsyncAbortCalls(main);
  auto const decisions_before = storage->pipeline_test_counters().decision_calls.load();

  ReplicationTestHooks hooks;
  std::mutex hold_mutex;
  std::condition_variable hold_cv;
  bool release_borrower = false;
  std::atomic<bool> borrower_held{false};
  std::atomic<int> tasks_done{0};
  std::atomic<int> async_cleanups{0};
  std::atomic<int> tasks_done_at_cleanup{-1};
  std::atomic<int> wal_waits_seen{0};
  hooks.before_wal_result_wait = [&](std::string const &name, uint64_t) {
    ++wal_waits_seen;
    if (name != "ASYNC") return;
    std::unique_lock lock{hold_mutex};
    borrower_held = true;
    hold_cv.wait_for(lock, std::chrono::seconds(30), [&] { return release_borrower; });
  };
  hooks.on_task_done = [&](std::string const &, uint64_t) { ++tasks_done; };
  hooks.after_async_abort_cleanup = [&] {
    tasks_done_at_cleanup = tasks_done.load();
    ++async_cleanups;
  };
  HookGuard hook_guard{storage, &hooks};
  CommitProbe probe;
  std::atomic<bool> armed{true};
  probe.after_prepare_record = [&] {
    if (!armed.exchange(false)) return;
    // The STRICT_SYNC replica has voted (its task ran ShipOne); the ASYNC borrower is still held.
    throw std::runtime_error("injected after_prepare_record");
  };
  storage->SetCommitProbe(&probe);

  auto const watermark_before = storage->LastCommittedMvccTimestamp();
  auto const ldt_before = LastDurableTimestamp(main);
  auto const committed_before = NumCommittedTxns(main);
  std::atomic<bool> threw{false};
  std::atomic<bool> commit_returned{false};
  auto const release = [&] {
    {
      std::lock_guard lock{hold_mutex};
      release_borrower = true;
    }
    hold_cv.notify_all();
  };
  // Released and joined on every exit, so an assertion cannot return over a live committer.
  JoinedThread committer{[&] {
                           bool local_threw = false;
                           static_cast<void>(Update(main, gid, 2, /*large=*/false, &local_threw));
                           threw = local_threw;
                           commit_returned = true;
                         },
                         release};
  ASSERT_TRUE(WaitFor([&] { return borrower_held.load(); }, std::chrono::seconds(10)))
      << "the ASYNC borrower was never held (WAL waits seen: " << wal_waits_seen.load() << ")";
  // The STRICT_SYNC prepare completes while the ASYNC borrower is held.
  ASSERT_TRUE(strict_->WaitEvent("prepared", std::chrono::seconds(10)).has_value());
  EXPECT_FALSE(WaitFor([&] { return commit_returned.load(); }, std::chrono::milliseconds(500)));
  EXPECT_EQ(storage->pipeline_test_counters().decision_calls.load(), decisions_before);  // no decision yet
  EXPECT_EQ(async_cleanups.load(), 0);                                                   // no ASYNC cleanup yet
  release();
  committer.Join();
  EXPECT_TRUE(threw.load());
  EXPECT_EQ(storage->pipeline_test_counters().decision_calls.load() - decisions_before, 1);
  EXPECT_EQ(async_cleanups.load(), 1);
  EXPECT_EQ(tasks_done_at_cleanup.load(), 2);  // both shipping tasks existed and finished before the cleanup
  EXPECT_EQ(AsyncAbortCalls(main) - async_aborts_before, 1);
  ExpectNothingCommitted(main, watermark_before, ldt_before, committed_before);
  ASSERT_TRUE(strict_->WaitEvent("abort_applied", std::chrono::seconds(10)).has_value());
  storage->SetCommitProbe(nullptr);

  ASSERT_TRUE(WaitForReplicaState(main, "STRICT", ReplicaState::READY));
  ASSERT_TRUE(WaitForReplicaState(main, "ASYNC", ReplicaState::READY));
  EXPECT_TRUE(Update(main, gid, 3, /*large=*/false).has_value());
  ASSERT_TRUE(strict_->WaitEvent("prepared", std::chrono::seconds(10)).has_value());
  ASSERT_TRUE(WaitForReplicaState(main, "ASYNC", ReplicaState::READY));
}

// Interleaving 2: the abort is parked at after_async_abort_cleanup while a heartbeat enqueues reconciliation for the
// ASYNC client; reconciliation reaches its quiescence point but cannot complete until the ticket retires.
TEST_F(PipelinedMixedAbortTest, ReconciliationWaitsForTheParkedAbortToRetire) {
  MinMemgraph main(main_conf);
  RegisterBoth(main);
  auto const gid = Seed(main, 1);
  ASSERT_TRUE(strict_->WaitEvent("prepared", std::chrono::seconds(10)).has_value());
  // The ASYNC client returns to READY from its own finalize task; the faulting write needs it streaming.
  ASSERT_TRUE(WaitForReplicaState(main, "ASYNC", ReplicaState::READY));
  auto *storage = MainStorage(main);
  auto const async_aborts_before = AsyncAbortCalls(main);

  ReplicationTestHooks hooks;
  std::mutex park_mutex;
  std::condition_variable park_cv;
  bool release_abort = false;
  std::atomic<bool> abort_parked{false};
  std::atomic<int> async_reconcile_arrivals{0};
  std::atomic<int> tasks_done{0};
  hooks.on_task_done = [&](std::string const &, uint64_t) { ++tasks_done; };
  hooks.after_async_abort_cleanup = [&] {
    std::unique_lock lock{park_mutex};
    abort_parked = true;
    park_cv.wait_for(lock, std::chrono::seconds(30), [&] { return release_abort; });
  };
  hooks.before_reconcile_quiesce = [&](std::string const &name) {
    if (name == "ASYNC") ++async_reconcile_arrivals;  // the STRICT client also goes MAYBE_BEHIND; select by name
  };
  HookGuard hook_guard{storage, &hooks};
  ASSERT_TRUE(strict_->Send("refuse_prepare"));

  auto const watermark_before = storage->LastCommittedMvccTimestamp();
  auto const ldt_before = LastDurableTimestamp(main);
  auto const committed_before = NumCommittedTxns(main);
  std::atomic<bool> commit_returned{false};
  std::expected<void, memgraph::storage::StorageManipulationError> result;
  auto const release = [&] {
    {
      std::lock_guard lock{park_mutex};
      release_abort = true;
    }
    park_cv.notify_all();
  };
  JoinedThread committer{[&] {
                           result = Update(main, gid, 2, /*large=*/false);
                           commit_returned = true;
                         },
                         release};
  ASSERT_TRUE(WaitFor([&] { return abort_parked.load(); }, std::chrono::seconds(10)));
  // A frequent heartbeat enqueues reconciliation for the MAYBE_BEHIND ASYNC client; it arrives at its quiescence
  // point but cannot complete while the abort still holds the ticket.
  ASSERT_TRUE(WaitFor([&] { return async_reconcile_arrivals.load() >= 1; }, std::chrono::seconds(10)));
  EXPECT_FALSE(WaitFor([&] { return main.db.storage()->GetReplicaState("ASYNC") == ReplicaState::READY; },
                       std::chrono::milliseconds(500)));
  EXPECT_FALSE(commit_returned.load());
  release();
  committer.Join();
  ASSERT_FALSE(result.has_value());
  ExpectNothingCommitted(main, watermark_before, ldt_before, committed_before);
  EXPECT_EQ(tasks_done.load(), 2);
  EXPECT_EQ(AsyncAbortCalls(main) - async_aborts_before, 1);
  ASSERT_TRUE(strict_->WaitEvent("abort_applied", std::chrono::seconds(10)).has_value());

  ASSERT_TRUE(WaitForReplicaState(main, "STRICT", ReplicaState::READY));
  ASSERT_TRUE(WaitForReplicaState(main, "ASYNC", ReplicaState::READY));
  EXPECT_TRUE(Update(main, gid, 3, /*large=*/false).has_value());
  ASSERT_TRUE(strict_->WaitEvent("prepared", std::chrono::seconds(10)).has_value());
  ASSERT_TRUE(WaitForReplicaState(main, "ASYNC", ReplicaState::READY));
}

// 3b, second variant: two replicas and a scheduling failure part-way through ScheduleEncodeAndShip. The first
// task is already parked on the WAL gate when the second enqueue throws; the mixed case must drain the scheduled
// task (which poisons its own stream) and discard the never-scheduled stream, in both the direct pipeline and the
// locally owned legacy scope.
class PipelinedPartialSchedulingTest : public PipelinedReplicationTest {
 protected:
  void Run(bool legacy_scope) {
    MinMemgraph main(main_conf);
    MinMemgraph replica1(repl_conf);
    MinMemgraph replica2(repl2_conf);
    Register(main, replica1, ReplicationMode::SYNC, "REPLICA1", ports[0]);
    Register(main, replica2, ReplicationMode::SYNC, "REPLICA2", ports[1]);
    auto const gid = Seed(main, 1);
    ASSERT_TRUE(WaitForReplicaToCatchUp(main, replica1));
    ASSERT_TRUE(WaitForReplicaToCatchUp(main, replica2));
    auto *storage = MainStorage(main);
    auto const finalizations_before = storage->pipeline_test_counters().finalize_wal_calls.load();
    std::array<uint64_t, 2> aborts_before{};
    storage->repl_storage_state_.replication_storage_clients_.WithReadLock([&](auto const &clients) {
      for (size_t i = 0; i < 2; ++i) aborts_before[i] = clients[i]->abort_rpc_client_calls();
    });

    ReplicationTestHooks hooks;
    std::latch first_waiting{1};
    std::atomic<int> first_waits{0};
    std::atomic<int> tasks_done{0};
    std::atomic<int> tasks_done_at_release{-1};
    std::atomic<int> commands_released{0};
    std::atomic<bool> armed{true};
    std::string const expected_scope = legacy_scope ? "legacy" : "pipeline";
    hooks.before_wal_result_wait = [&](std::string const &name, uint64_t) {
      if (name == "REPLICA1" && first_waits.fetch_add(1) == 0) first_waiting.count_down();
    };
    hooks.on_task_done = [&](std::string const &, uint64_t) { ++tasks_done; };
    hooks.on_commands_released = [&](std::string_view scope, uint64_t) {
      if (scope != expected_scope) return;
      tasks_done_at_release = tasks_done.load();
      ++commands_released;
    };
    hooks.throw_before_enqueue_for = [&](std::string const &name, uint64_t) {
      if (name != "REPLICA2" || !armed.exchange(false)) return false;
      first_waiting.wait();  // the first task holds a partially transmitted request on the WAL gate
      return true;
    };
    HookGuard hook_guard{storage, &hooks};
    CommitProbe probe;
    if (legacy_scope) {
      probe.budget_refuse.site = memgraph::storage::BudgetRefuse::kMaterializer;
      probe.after_mint = [&probe] {
        if (probe.budget_refuse.ticket.load() == 0) probe.budget_refuse.ticket = probe.minted_ticket.load();
      };
      storage->SetCommitProbe(&probe);
    }

    auto const watermark_before = storage->LastCommittedMvccTimestamp();
    auto const ldt_before = LastDurableTimestamp(main);
    auto const committed_before = NumCommittedTxns(main);
    bool threw = false;
    static_cast<void>(Update(main, gid, 2, /*large=*/true, &threw));
    EXPECT_TRUE(threw);
    ExpectNothingCommitted(main, watermark_before, ldt_before, committed_before);
    EXPECT_EQ(storage->pipeline_test_counters().finalize_wal_calls.load(), finalizations_before);
    EXPECT_EQ(commands_released.load(), 1);
    EXPECT_EQ(tasks_done_at_release.load(), 1);  // exactly the one successfully scheduled task, finished first
    std::array<uint64_t, 2> aborts_after{};
    storage->repl_storage_state_.replication_storage_clients_.WithReadLock([&](auto const &clients) {
      for (size_t i = 0; i < 2; ++i) aborts_after[i] = clients[i]->abort_rpc_client_calls();
    });
    EXPECT_EQ(aborts_after[0] - aborts_before[0], 1);  // the scheduled task poisoned its own stream
    EXPECT_EQ(aborts_after[1] - aborts_before[1], 1);  // the never-scheduled stream was discarded by the owner
    storage->SetCommitProbe(nullptr);

    ASSERT_TRUE(WaitForReplicaState(main, "REPLICA1", ReplicaState::READY));
    ASSERT_TRUE(WaitForReplicaState(main, "REPLICA2", ReplicaState::READY));
    EXPECT_TRUE(Update(main, gid, 3, /*large=*/false).has_value());
    EXPECT_TRUE(WaitForReplicaToCatchUp(main, replica1));
    EXPECT_TRUE(WaitForReplicaToCatchUp(main, replica2));
    EXPECT_EQ(ReadIntProperty(replica1, gid, "p"), 3);
    EXPECT_EQ(ReadIntProperty(replica2, gid, "p"), 3);
  }
};

TEST_F(PipelinedPartialSchedulingTest, DirectPipeline) { Run(/*legacy_scope=*/false); }

TEST_F(PipelinedPartialSchedulingTest, LocallyOwnedLegacyScope) { Run(/*legacy_scope=*/true); }

// 9: an exception between the frames of a legacy STRICT_SYNC prepare record leaves an incomplete record on disk,
// which is fatal. The replica runs in its own process so the death child owns nothing but the main.
class PipelinedBetweenFramesDeathTest : public PipelinedReplicationTest {
 protected:
  void SetUp() override {
    PipelinedReplicationTest::SetUp();
    if (::testing::internal::InDeathTestChild()) return;
    replica_ = std::make_unique<ReplicaProcess>(ports[0], ProcessReplicaDir(0));
    ASSERT_TRUE(replica_->WaitReady());
  }

  void TearDown() override {
    if (!::testing::internal::InDeathTestChild()) replica_.reset();
    PipelinedReplicationTest::TearDown();
  }

  void RunMain() {
    ArmReplicationWatchdog(120);
    MinMemgraph main(main_conf);
    auto const reg = main.repl_handler.TryRegisterReplica(
        ReplicationClientConfig{.name = "REPLICA1",
                                .mode = ReplicationMode::STRICT_SYNC,
                                .repl_server_endpoint = Endpoint(local_host, ports[0])});
    if (!reg.has_value()) _exit(3);
    if (!WaitForReplicaState(main, "REPLICA1", ReplicaState::READY)) _exit(3);
    auto const gid = Seed(main, 1);
    CommitProbe probe;
    probe.between_frames = [] { throw std::runtime_error("injected between frames"); };
    MainStorage(main)->SetCommitProbe(&probe);
    static_cast<void>(Update(main, gid, 2, /*large=*/false));
    _exit(5);  // the incomplete record did not terminate
  }

  std::unique_ptr<ReplicaProcess> replica_;
};

TEST_F(PipelinedBetweenFramesDeathTest, ExceptionWithAnIncompleteRecordIsFatal) {
  EXPECT_EXIT(RunMain(), ::testing::KilledBySignal(SIGABRT), "");
}

int main(int argc, char **argv) {
  if (argc > 1 && std::string_view{argv[1]} == memgraph::tests::kReplicaRoleFlag) {
    return memgraph::tests::RunReplicaRole(argc, argv);
  }
  ::testing::InitGoogleTest(&argc, argv);
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  return RUN_ALL_TESTS();
}
