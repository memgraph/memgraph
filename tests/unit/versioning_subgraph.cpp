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
#include <array>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <limits>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <typeinfo>
#include <unordered_set>
#include <vector>

#include <gmock/gmock.h>
#include "gtest/gtest.h"

#include "auth/models.hpp"
#include "flags/general.hpp"
#include "glue/auth_checker.hpp"
#include "interpreter_faker.hpp"
#include "io/network/endpoint.hpp"
#include "license/license.hpp"
#include "metrics/prometheus_metrics.hpp"
#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/query/auth_query.hpp"
#include "query/graph.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/query_user.hpp"
#include "query/vertex_accessor.hpp"
#include "replication/config.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/exceptions.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/synchronized.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/version_store.hpp"

namespace {
constexpr auto kNoHandler = nullptr;
}  // namespace

class VersioningSubgraphTest : public ::testing::Test {
 protected:
  std::filesystem::path data_directory = std::filesystem::temp_directory_path() / "MG_tests_unit_versioning_subgraph";

  memgraph::storage::Config config{[&] {
    memgraph::storage::Config config{};
    config.durability.storage_directory = data_directory;
    config.experimental_versioning = true;
    config.experimental_versioning = true;
    config.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    config.disk.main_storage_directory = data_directory / "disk";
    return config;
  }()};

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config};
  memgraph::dbms::DatabaseAccess db{[&] {
    auto db_acc_opt = db_gk.access();
    MG_ASSERT(db_acc_opt, "Failed to access db");
    auto &db_acc = *db_acc_opt;
    MG_ASSERT(db_acc->GetStorageMode() == memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL,
              "Wrong storage mode!");
    return db_acc;
  }()};

  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          kNoHandler,
                                                          &repl_state,
                                                          system_state,
                                                          nullptr
#ifdef MG_ENTERPRISE
                                                          ,
                                                          nullptr,
                                                          nullptr
#endif
  };

  InterpreterFaker faker{&interpreter_context, db};

  void SetUp() override { SetVersioningFlag(false); }

  // The flag is a startup setting; flipping it on the live config is how these tests exercise the gate.
  void SetVersioningFlag(bool enabled) { db->storage()->config_.experimental_versioning = enabled; }

  void TearDown() override {
    memgraph::license::global_license_checker.DisableTesting();
    std::filesystem::remove_all(data_directory);
  }

  void SatisfyGate() {
    SetVersioningFlag(true);
    memgraph::license::global_license_checker.EnableTesting();
  }
};

namespace {
using memgraph::query::DbAccessor;
using memgraph::query::EdgeAccessor;
using memgraph::query::Graph;
using memgraph::query::SubgraphVertexAccessor;
using memgraph::query::VertexAccessor;
using memgraph::storage::View;

constexpr std::string_view kMarkerPrefix = "__mg";

std::vector<std::string> LabelNames(DbAccessor &dba, const VertexAccessor &v) {
  auto labels = v.Labels(View::NEW);
  EXPECT_TRUE(labels.has_value());
  std::vector<std::string> names;
  for (auto l : *labels) names.push_back(dba.LabelToName(l));
  std::ranges::sort(names);
  return names;
}

std::vector<std::string> PropertyNames(DbAccessor &dba, const VertexAccessor &v) {
  auto props = v.Properties(View::NEW);
  EXPECT_TRUE(props.has_value());
  std::vector<std::string> names;
  for (auto const &[k, _] : *props) names.push_back(dba.PropertyToName(k));
  std::ranges::sort(names);
  return names;
}

bool HasMarker(const std::vector<std::string> &names) {
  return std::ranges::any_of(names, [](auto const &n) { return n.starts_with(kMarkerPrefix); });
}

std::string EdgeKey(DbAccessor &dba, const EdgeAccessor &e) { return dba.EdgeTypeToName(e.EdgeType()); }

std::vector<std::string> EdgeKeys(DbAccessor &dba, const auto &edges) {
  std::vector<std::string> keys;
  for (auto const &e : edges) keys.push_back(EdgeKey(dba, e));
  std::ranges::sort(keys);
  return keys;
}
}  // namespace

// On a branch, SubgraphVertexAccessor adjacency must use the branch-aware adjacency, then apply the membership filter.
TEST_F(VersioningSubgraphTest, SubgraphAdjacencyOnBranchIsBranchView) {
  SatisfyGate();

  faker.Interpret("CREATE (a:A {name:'a'})-[:R1]->(b:B {name:'b'}), (a)-[:R2]->(c:C {name:'c'})");
  faker.Interpret("CREATE (d:D {name:'d'})-[:R4]->(e:E {name:'e'})");
  faker.Interpret("CREATE BRANCH 'br'");
  faker.Interpret("USE BRANCH 'br'");
  ASSERT_EQ(faker.interpreter.current_db_.CurrentVersion(), "br");

  faker.Interpret("MATCH (b:B), (c:C) CREATE (b)-[:R3]->(c)");
  faker.Interpret("MATCH (:A)-[r:R2]->() DELETE r");
  faker.Interpret("MATCH (a:A) REMOVE a:A SET a:X, a.name = 'a2'");

  auto *ctx = faker.interpreter.current_db_.branch_context();
  ASSERT_NE(ctx, nullptr);

  // Mirrors CurrentDB::SetupDatabaseTransaction: historical first, then the diff accessor.
  ASSERT_TRUE(ctx->OpenHistorical().has_value());
  auto diff_acc = ctx->diff_engine().Access(memgraph::storage::StorageAccessType::READ);
  ctx->SetCurrentDiffTxn(diff_acc.get());
  {
    DbAccessor dba{diff_acc.get(), ctx};

    std::optional<VertexAccessor> a, b, c, d, e;
    for (auto v : dba.Vertices(View::NEW)) {
      auto names = PropertyNames(dba, v);
      EXPECT_FALSE(HasMarker(names)) << "marker property leaked through the scan";
      auto name = v.GetProperty(View::NEW, dba.NameToProperty("name"));
      ASSERT_TRUE(name.has_value());
      if (name->IsString() && name->ValueString() == "a2") a = v;
      if (name->IsString() && name->ValueString() == "b") b = v;
      if (name->IsString() && name->ValueString() == "c") c = v;
      if (name->IsString() && name->ValueString() == "d") d = v;
      if (name->IsString() && name->ValueString() == "e") e = v;
    }
    ASSERT_TRUE(a && b && c && d && e);

    Graph full{memgraph::utils::NewDeleteResource()};
    for (auto const &v : {*a, *b, *c}) {
      full.InsertVertex(v);
      auto out = v.OutEdges(View::NEW);
      ASSERT_TRUE(out.has_value());
      for (auto const &e : out->edges) full.InsertEdge(e);
    }
    ASSERT_EQ(full.edges().size(), 2U);

    SubgraphVertexAccessor sa{*a, &full}, sb{*b, &full}, sc{*c, &full};

    auto a_out = sa.OutEdges(View::NEW);
    auto a_in = sa.InEdges(View::NEW);
    auto b_out = sb.OutEdges(View::NEW);
    auto b_in = sb.InEdges(View::NEW);
    auto c_out = sc.OutEdges(View::NEW);
    auto c_in = sc.InEdges(View::NEW);
    ASSERT_TRUE(a_out && a_in && b_out && b_in && c_out && c_in);

    // R2 (deleted on the branch) is hidden; R3 (branch-created) shows from both endpoints.
    EXPECT_EQ(EdgeKeys(dba, a_out->edges), (std::vector<std::string>{"R1"}));
    EXPECT_TRUE(a_in->edges.empty());
    EXPECT_EQ(EdgeKeys(dba, b_in->edges), (std::vector<std::string>{"R1"}));
    EXPECT_EQ(EdgeKeys(dba, b_out->edges), (std::vector<std::string>{"R3"}));
    EXPECT_EQ(EdgeKeys(dba, c_in->edges), (std::vector<std::string>{"R3"}));
    EXPECT_TRUE(c_out->edges.empty());

    // Endpoints must be reconstructed from the branch, not read as raw stubs.
    for (auto const &e : a_out->edges) {
      EXPECT_NE(e.branch_ctx_, nullptr);
      auto from = e.From();
      auto to = e.To();
      EXPECT_EQ(LabelNames(dba, from), (std::vector<std::string>{"X"}));
      EXPECT_EQ(LabelNames(dba, to), (std::vector<std::string>{"B"}));
      EXPECT_FALSE(HasMarker(LabelNames(dba, from)));
      EXPECT_FALSE(HasMarker(PropertyNames(dba, from)));
      auto from_name = from.GetProperty(View::NEW, dba.NameToProperty("name"));
      ASSERT_TRUE(from_name.has_value());
      EXPECT_EQ(from_name->ValueString(), "a2");
    }
    for (auto const &e : b_out->edges) {
      EXPECT_NE(e.branch_ctx_, nullptr);
      EXPECT_EQ(LabelNames(dba, e.To()), (std::vector<std::string>{"C"}));
      EXPECT_FALSE(HasMarker(PropertyNames(dba, e.To())));
    }

    // A projection holding only R3 hides R1 everywhere.
    Graph partial{memgraph::utils::NewDeleteResource()};
    for (auto const &v : {*a, *b, *c}) partial.InsertVertex(v);
    for (auto const &e : b_out->edges) partial.InsertEdge(e);
    SubgraphVertexAccessor pa{*a, &partial}, pb{*b, &partial};
    EXPECT_TRUE(pa.OutEdges(View::NEW)->edges.empty());
    EXPECT_TRUE(pb.InEdges(View::NEW)->edges.empty());
    EXPECT_EQ(EdgeKeys(dba, pb.OutEdges(View::NEW)->edges), (std::vector<std::string>{"R3"}));

    // d -R4-> e is untouched on the branch; the filter must still apply.
    Graph with_r4{memgraph::utils::NewDeleteResource()};
    Graph without_r4{memgraph::utils::NewDeleteResource()};
    for (auto const &v : {*d, *e}) {
      with_r4.InsertVertex(v);
      without_r4.InsertVertex(v);
    }
    auto d_out_raw = d->OutEdges(View::NEW);
    ASSERT_TRUE(d_out_raw.has_value());
    ASSERT_EQ(d_out_raw->edges.size(), 1U);
    with_r4.InsertEdge(d_out_raw->edges[0]);

    SubgraphVertexAccessor wd{*d, &with_r4}, we{*e, &with_r4}, nd{*d, &without_r4}, ne{*e, &without_r4};
    EXPECT_EQ(EdgeKeys(dba, wd.OutEdges(View::NEW)->edges), (std::vector<std::string>{"R4"}));
    EXPECT_TRUE(wd.InEdges(View::NEW)->edges.empty());
    EXPECT_EQ(EdgeKeys(dba, we.InEdges(View::NEW)->edges), (std::vector<std::string>{"R4"}));
    EXPECT_TRUE(we.OutEdges(View::NEW)->edges.empty());
    EXPECT_TRUE(nd.OutEdges(View::NEW)->edges.empty());
    EXPECT_TRUE(ne.InEdges(View::NEW)->edges.empty());
  }
  diff_acc.reset();
  ctx->EndTransaction();
}

TEST_F(VersioningSubgraphTest, SubgraphAdjacencyOnMainUnchanged) {
  faker.Interpret("CREATE (a:A)-[:R1]->(b:B), (a)-[:R2]->(b)");
  auto acc = db->storage()->Access(memgraph::storage::StorageAccessType::READ);
  DbAccessor dba{acc.get()};
  std::vector<VertexAccessor> vs;
  for (auto v : dba.Vertices(View::NEW)) vs.push_back(v);
  ASSERT_EQ(vs.size(), 2U);
  Graph g{memgraph::utils::NewDeleteResource()};
  for (auto const &v : vs) {
    g.InsertVertex(v);
    auto out = v.OutEdges(View::NEW);
    ASSERT_TRUE(out.has_value());
    if (!out->edges.empty()) g.InsertEdge(out->edges[0]);
  }
  ASSERT_EQ(g.edges().size(), 1U);
  size_t out_total = 0;
  size_t in_total = 0;
  for (auto const &v : vs) {
    SubgraphVertexAccessor sv{v, &g};
    out_total += sv.OutEdges(View::NEW)->edges.size();
    in_total += sv.InEdges(View::NEW)->edges.size();
  }
  EXPECT_EQ(out_total, 1U);
  EXPECT_EQ(in_total, 1U);
}
