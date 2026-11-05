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

// A branch comes back after a restart of the dbms::Database by replaying its diff-engine WAL. The log holds user
// operations: a record on a fork object is written again through the BranchContext write resolver, a record on a
// branch-native object is the diff engine's stock operation. Each case runs a branch workload, captures what a session
// reads (and what the change filters, touched sets and fork-delete counts say), restarts, replays, and compares.
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "dbms/database.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/scheduler.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/branch_ledger.hpp"
#include "versioning/branch_recovery.hpp"
#include "versioning/version_store.hpp"
#include "versioning_recovery_support.hpp"

import memgraph.storage.property_value;

namespace ms = memgraph::storage;
namespace mv = memgraph::versioning;

using ::testing::HasSubstr;
using namespace memgraph::tests::versioning_recovery;  // NOLINT(google-build-using-namespace)

namespace {
class VersioningBranchRecoveryTest : public RecoveryTest {};
}  // namespace

// ---- Properties of fork vertices

TEST_F(VersioningBranchRecoveryTest, RemovingAForkPropertyReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.Fork(e.base.v1);
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue()).has_value());
    });
  });
  ASSERT_NE(r.replayed.Find(r.base.v1), nullptr);
  EXPECT_EQ(r.replayed.At(r.base.v1).props.size(), 1u);
  EXPECT_EQ(r.replayed.At(r.base.v1).props.at("q"), "5");
}

TEST_F(VersioningBranchRecoveryTest, RemovingThenSettingAForkPropertyInOneTransactionReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.Fork(e.base.v1);
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue()).has_value());
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue(7)).has_value());
    });
  });
  EXPECT_EQ(r.replayed.At(r.base.v1).props.at("p"), "7");
}

TEST_F(VersioningBranchRecoveryTest, SettingThenRemovingAForkPropertyReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.Fork(e.base.v1);
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue(5)).has_value());
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue()).has_value());
    });
  });
  EXPECT_EQ(r.replayed.At(r.base.v1).props.count("p"), 0u);
  EXPECT_EQ(r.replayed.At(r.base.v1).props.at("q"), "5");
}

TEST_F(VersioningBranchRecoveryTest, SettingThenRemovingAPropertyTheForkLacksReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v3 = e.Fork(e.base.v3);
      ASSERT_TRUE(c.SetProperty(v3, e.Prop("a"), ms::PropertyValue(1)).has_value());
      ASSERT_TRUE(c.SetProperty(v3, e.Prop("a"), ms::PropertyValue()).has_value());
    });
  });
  EXPECT_TRUE(r.replayed.At(r.base.v3).props.empty());
}

TEST_F(VersioningBranchRecoveryTest, APropertyRemovedInOneTransactionAndSetInANextReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.Fork(e.base.v1);
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue()).has_value());
    });
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.Fork(e.base.v1);
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue(9)).has_value());
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("q"), ms::PropertyValue(6)).has_value());
    });
  });
  EXPECT_EQ(r.replayed.At(r.base.v1).props.at("p"), "9");
  EXPECT_EQ(r.replayed.At(r.base.v1).props.at("q"), "6");
}

// ---- Labels of fork vertices

TEST_F(VersioningBranchRecoveryTest, RemovingAndReAddingAForkLabelInOneTransactionReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.Fork(e.base.v1);
      ASSERT_TRUE(*c.RemoveLabel(v1, e.Label("L")));
      ASSERT_TRUE(*c.AddLabel(v1, e.Label("L")));
    });
  });
  EXPECT_EQ(Joined(r.replayed.At(r.base.v1).labels), "L,M,");
}

TEST_F(VersioningBranchRecoveryTest, RemovingAForkLabelAndAddingAnotherReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.Fork(e.base.v1);
      ASSERT_TRUE(*c.RemoveLabel(v1, e.Label("L")));
    });
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.Fork(e.base.v1);
      ASSERT_TRUE(*c.AddLabel(v1, e.Label("X")));
    });
  });
  EXPECT_EQ(Joined(r.replayed.At(r.base.v1).labels), "M,X,");
}

TEST_F(VersioningBranchRecoveryTest, ReAddingALabelRemovedByAnEarlierTransactionReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.Fork(e.base.v1);
      ASSERT_TRUE(*c.RemoveLabel(v1, e.Label("L")));
    });
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.Fork(e.base.v1);
      ASSERT_TRUE(*c.AddLabel(v1, e.Label("L")));
    });
  });
  EXPECT_EQ(Joined(r.replayed.At(r.base.v1).labels), "L,M,");
}

TEST_F(VersioningBranchRecoveryTest, AddingThenRemovingALabelTheForkLacksReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.Fork(e.base.v1);
      ASSERT_TRUE(*c.AddLabel(v1, e.Label("NewL")));
      ASSERT_TRUE(*c.RemoveLabel(v1, e.Label("NewL")));
    });
  });
  EXPECT_EQ(Joined(r.replayed.At(r.base.v1).labels), "L,M,");
}

// ---- Deletes

TEST_F(VersioningBranchRecoveryTest, DeletingAForkVertexAfterWritesReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v3 = e.Fork(e.base.v3);
      ASSERT_TRUE(*c.AddLabel(v3, e.Label("X")));
      ASSERT_TRUE(c.SetProperty(v3, e.Prop("a"), ms::PropertyValue(2)).has_value());
      ASSERT_TRUE(*c.DeleteForkVertex(e.base.v3));
    });
  });
  EXPECT_EQ(r.replayed.Find(r.base.v3), nullptr);
  EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("deletes v=1 e=1"));
}

TEST_F(VersioningBranchRecoveryTest, DetachDeletingAForkVertexReplaysItsHiddenEdgesAndCounts) {
  auto const r = RunRoundTrip(
      [](Env &e) { e.Txn([&](mv::BranchContext &c, auto &) { ASSERT_TRUE(*c.DeleteForkVertex(e.base.v1)); }); });
  EXPECT_EQ(r.replayed.Find(r.base.v1), nullptr);
  EXPECT_TRUE(r.replayed.At(r.base.v3).out.empty()) << "e2 is hidden with its endpoint";
  EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("deletes v=1 e=2"));
}

TEST_F(VersioningBranchRecoveryTest, DeletingAForkEdgeReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto edge = e.ForkEdge(e.base.e1, e.base.v1);
      ASSERT_TRUE(*c.DeleteForkEdge(edge));
    });
  });
  EXPECT_TRUE(r.replayed.At(r.base.v1).out.empty());
  EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("deletes v=0 e=1"));
  EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("filter e" + std::to_string(r.base.e1.AsUint()) + " deleted=1"));
}

TEST_F(VersioningBranchRecoveryTest, DeletingAForkEdgeReplaysWhenMainHasNoEdgeProperties) {
  auto const r = RunRoundTrip(
      [](Env &e) {
        e.Txn([&](mv::BranchContext &c, auto &) {
          auto edge = e.ForkEdge(e.base.e1, e.base.v1);
          ASSERT_TRUE(*c.DeleteForkEdge(edge));
        });
      },
      {.properties_on_edges = false});
  EXPECT_TRUE(r.replayed.At(r.base.v1).out.empty());
  EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("deletes v=0 e=1"));
}

// The WAL puts a fork vertex's delete before the records of its transaction's edges, so each of these orders needs
// the delete applied last.
TEST_F(VersioningBranchRecoveryTest, AForkEdgeDeletedThenItsEndpointInOneTransactionReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto edge = e.ForkEdge(e.base.e1, e.base.v1);
      ASSERT_TRUE(*c.DeleteForkEdge(edge));
      ASSERT_TRUE(*c.DeleteForkVertex(e.base.v1));
    });
  });
  EXPECT_EQ(r.replayed.Find(r.base.v1), nullptr);
  EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("deletes v=1 e=2")) << "e1 counted once, e2 with the vertex";
  EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("filter e" + std::to_string(r.base.e1.AsUint()) + " deleted=1"))
      << "the edge's own marker must exist, not only the vertex's";
}

TEST_F(VersioningBranchRecoveryTest, AForkEdgeDeletedThenItsEndpointInALaterTransactionReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto edge = e.ForkEdge(e.base.e1, e.base.v1);
      ASSERT_TRUE(*c.DeleteForkEdge(edge));
    });
    e.Txn([&](mv::BranchContext &c, auto &) { ASSERT_TRUE(*c.DeleteForkVertex(e.base.v1)); });
  });
  EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("deletes v=1 e=2"));
}

TEST_F(VersioningBranchRecoveryTest, ANativeEdgeCreatedThenDetachDeletedWithItsForkEndpointReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &diff) {
      auto stub = e.Stub(e.base.v2);
      auto native = c.CreateNativeVertex();
      auto edge = c.CreateNativeEdge(stub, native, e.EdgeType("N"));
      ASSERT_TRUE(edge.has_value());
      ASSERT_TRUE(diff.DeleteEdge(&*edge).has_value());
      ASSERT_TRUE(*c.DeleteForkVertex(e.base.v2));
    });
  });
  EXPECT_EQ(r.replayed.Find(r.base.v2), nullptr);
}

TEST_F(VersioningBranchRecoveryTest, ANativeEdgeCreatedToAForkVertexThenItsDeleteLeavingTheEdgeReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto stub = e.Stub(e.base.v2);
      auto native = c.CreateNativeVertex();
      ASSERT_TRUE(c.CreateNativeEdge(native, stub, e.EdgeType("N")).has_value());
      ASSERT_TRUE(*c.DeleteForkVertex(e.base.v2));
    });
  });
  EXPECT_EQ(r.replayed.Find(r.base.v2), nullptr);
}

TEST_F(VersioningBranchRecoveryTest, DetachDeletingAForkVertexWithACommittedNativeEdgeReplays) {
  uint64_t native_edge = 0;
  auto const r = RunRoundTrip([&](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto stub = e.Stub(e.base.v2);
      auto native = c.CreateNativeVertex();
      auto edge = c.CreateNativeEdge(stub, native, e.EdgeType("N"));
      ASSERT_TRUE(edge.has_value());
      native_edge = edge->Gid().AsUint();
    });
    e.Txn([&](mv::BranchContext &c, auto &diff) {
      auto edge = c.FindDiffEdge(ms::Gid::FromUint(native_edge), e.base.v2, ms::View::NEW);
      ASSERT_TRUE(edge.has_value());
      ASSERT_TRUE(diff.DeleteEdge(&*edge).has_value());
      ASSERT_TRUE(*c.DeleteForkVertex(e.base.v2));
    });
  });
  EXPECT_EQ(r.replayed.Find(r.base.v2), nullptr);
  EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("deletes v=1 e=1")) << "e1 goes with v2";
}

// ---- Edges

TEST_F(VersioningBranchRecoveryTest, ForkEdgePropertyWritesReplay) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto e1 = e.ForkEdge(e.base.e1, e.base.v1);
      ASSERT_TRUE(c.SetProperty(e1, e.Prop("w"), ms::PropertyValue(5)).has_value());
      auto e2 = e.ForkEdge(e.base.e2, e.base.v3);
      ASSERT_TRUE(c.SetProperty(e2, e.Prop("a"), ms::PropertyValue(3)).has_value());
    });
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto e2 = e.ForkEdge(e.base.e2, e.base.v3);
      ASSERT_TRUE(c.SetProperty(e2, e.Prop("w"), ms::PropertyValue()).has_value());
    });
  });
  const auto &e1 = r.replayed.At(r.base.v1).out.at(0);
  EXPECT_EQ(e1.props.at("w"), "5");
  const auto &e2 = r.replayed.At(r.base.v3).out.at(0);
  EXPECT_EQ(e2.props.count("w"), 0u);
  EXPECT_EQ(e2.props.at("a"), "3");
}

// ---- Names

// A name that spells what a marker encoding might have used is the user's own name: it is in the file as typed and
// replays as a user label and property, next to a real removal.
TEST_F(VersioningBranchRecoveryTest, UserNamesThatLookLikeMarkersReplayAsUserNames) {
  const std::vector<std::string> names = {
      "__mg_removed_x", "__mg_deleted", "__mg_lit_x", "__mg_id__", std::string{"a\0b", 3}, "\xff\xfe_x"};
  auto const r = RunRoundTrip([&](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.Fork(e.base.v1);
      for (const auto &name : names) {
        ASSERT_TRUE(*c.AddLabel(v1, e.Label(name)));
        ASSERT_TRUE(c.SetProperty(v1, e.Prop(name), ms::PropertyValue(1)).has_value());
      }
      ASSERT_TRUE(*c.RemoveLabel(v1, e.Label("L")));
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue()).has_value());
      ASSERT_TRUE(*c.DeleteForkVertex(e.base.v2));
    });
  });
  const auto *v1 = r.replayed.Find(r.base.v1);
  ASSERT_NE(v1, nullptr);
  for (const auto &name : names) {
    EXPECT_TRUE(v1->labels.contains(name)) << name;
    EXPECT_EQ(v1->props.at(name), "1") << name;
  }
  EXPECT_FALSE(v1->labels.contains("L"));
  EXPECT_EQ(v1->props.count("p"), 0u);
  EXPECT_EQ(r.replayed.Find(r.base.v2), nullptr);
}

// ---- Native objects

namespace {
struct NativeWorkload {
  ms::Gid n1;
  ms::Gid n2;
  // The highest native gids the workload allocated, whether or not the object survived.
  ms::Gid last_vertex;
  ms::Gid last_edge;
};

// Every native record kind in two transactions, with fork objects around them: vertices with labels and properties,
// edges between natives and on the stubs of fork vertices (one with a property); then a property on an edge of the
// first transaction, a label removal, the deletes of a stub-to-stub edge and of a vertex, and writes to fork objects.
NativeWorkload RunNativeWorkload(Env &e) {
  NativeWorkload out{};
  ms::Gid n3;
  ms::Gid n1_to_n2;
  ms::Gid between_stubs;

  e.Txn([&](mv::BranchContext &c, auto &) {
    auto n1 = c.CreateNativeVertex();
    auto n2 = c.CreateNativeVertex();
    n3 = c.CreateNativeVertex().Gid();
    out.n1 = n1.Gid();
    out.n2 = n2.Gid();
    out.last_vertex = n3;
    ASSERT_TRUE(*c.AddLabel(n1, e.Label("N")));
    ASSERT_TRUE(*c.AddLabel(n1, e.Label("X")));
    ASSERT_TRUE(c.SetProperty(n1, e.Prop("a"), ms::PropertyValue(1)).has_value());

    auto s4 = e.Stub(e.base.v4);
    auto s3 = e.Stub(e.base.v3);
    auto native = c.CreateNativeEdge(n1, n2, e.EdgeType("T"));
    ASSERT_TRUE(native.has_value());
    n1_to_n2 = native->Gid();
    auto from_stub = c.CreateNativeEdge(s4, n1, e.EdgeType("T"));
    ASSERT_TRUE(from_stub.has_value());
    ASSERT_TRUE(c.SetProperty(*from_stub, e.Prop("w"), ms::PropertyValue(33)).has_value());
    ASSERT_TRUE(c.CreateNativeEdge(n2, s3, e.EdgeType("T")).has_value());
    auto stubs = c.CreateNativeEdge(s4, s3, e.EdgeType("T"));
    ASSERT_TRUE(stubs.has_value());
    between_stubs = stubs->Gid();
    out.last_edge = between_stubs;
  });

  e.Txn([&](mv::BranchContext &c, auto &diff) {
    auto edge = c.FindDiffEdge(n1_to_n2, ms::View::NEW);
    ASSERT_TRUE(edge.has_value());
    ASSERT_TRUE(c.SetProperty(*edge, e.Prop("w"), ms::PropertyValue(7)).has_value());
    auto n1 = diff.FindVertex(out.n1, ms::View::NEW);
    ASSERT_TRUE(n1.has_value());
    ASSERT_TRUE(*c.RemoveLabel(*n1, e.Label("X")));
    auto stubs = c.FindDiffEdge(between_stubs, e.base.v4, ms::View::NEW);
    ASSERT_TRUE(stubs.has_value());
    ASSERT_TRUE(diff.DeleteEdge(&*stubs).has_value());
    auto vertex = diff.FindVertex(n3, ms::View::NEW);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(diff.DeleteVertex(&*vertex).has_value());

    auto v1 = e.Fork(e.base.v1);
    ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue(9)).has_value());
    ASSERT_TRUE(*c.RemoveLabel(v1, e.Label("L")));
  });
  return out;
}

// Changes a replayed native vertex without allocating a gid, and a fork vertex.
void WriteAfterRestart(Env &e, ms::Gid native, int64_t value) {
  e.Txn([&](mv::BranchContext &c, auto &diff) {
    auto vertex = diff.FindVertex(native, ms::View::NEW);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(c.SetProperty(*vertex, e.Prop("a"), ms::PropertyValue(value)).has_value());
    auto v2 = e.Fork(e.base.v2);
    ASSERT_TRUE(c.SetProperty(v2, e.Prop("q"), ms::PropertyValue(value)).has_value());
  });
}
}  // namespace

TEST_F(VersioningBranchRecoveryTest, NativeAndForkWritesReplay) {
  NativeWorkload workload{};
  auto const r = RunRoundTrip([&](Env &e) { workload = RunNativeWorkload(e); });
  ASSERT_EQ(r.replayed.vertices.size(), 6u) << "4 fork vertices, 3 native, one of them deleted";
  const auto *n1 = r.replayed.Find(ms::Gid::FromUint(workload.n1.AsUint()));
  ASSERT_NE(n1, nullptr);
  EXPECT_EQ(Joined(n1->labels), "N,");
  EXPECT_EQ(Joined(r.replayed.At(r.base.v1).labels), "M,");
  EXPECT_EQ(r.replayed.At(r.base.v1).props.at("p"), "9");
}

TEST_F(VersioningBranchRecoveryTest, WritesAcrossTwoRestartsReplayTheWholeChain) {
  Base base{};
  NativeWorkload workload{};
  uint64_t number = 0;
  Captured after_first;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b");
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    workload = RunNativeWorkload(env);
    after_first = Capture(db, *ctx, base);
  }
  const auto first_run_files = WalFiles(number).size();
  ASSERT_GE(first_run_files, 1u);

  Captured after_second;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    bool built = false;
    auto ctx = Checkout(db, "b", &built);
    ASSERT_NE(ctx, nullptr);
    EXPECT_FALSE(built);
    EXPECT_EQ(Capture(db, *ctx, base).Render(), after_first.Render());

    Env env{db, *ctx, base};
    WriteAfterRestart(env, workload.n2, 5);
    after_second = Capture(db, *ctx, base);
    ASSERT_NE(after_second.Render(), after_first.Render());
  }
  EXPECT_GT(WalFiles(number).size(), first_run_files) << "the second run continues the WAL in a new file";

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  EXPECT_EQ(Capture(db, *ctx, base).Render(), after_second.Render());
}

TEST_F(VersioningBranchRecoveryTest, TwoBranchesRecoverIndependently) {
  Base base{};
  Captured b_before;
  Captured c_before;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    ASSERT_TRUE(db.version_store()->CreateBranch("c").has_value());
    auto b = Checkout(db, "b");
    auto c = Checkout(db, "c");
    ASSERT_NE(b, nullptr);
    ASSERT_NE(c, nullptr);
    Env env_b{db, *b, base};
    Env env_c{db, *c, base};
    RunNativeWorkload(env_b);
    env_c.Txn([&](mv::BranchContext &ctx, auto &) {
      auto native = ctx.CreateNativeVertex();
      ASSERT_TRUE(ctx.SetProperty(native, env_c.Prop("p"), ms::PropertyValue(100)).has_value());
      auto stub = env_c.Stub(base.v2);
      ASSERT_TRUE(ctx.CreateNativeEdge(stub, native, env_c.EdgeType("T")).has_value());
      auto v1 = env_c.Fork(base.v1);
      ASSERT_TRUE(ctx.SetProperty(v1, env_c.Prop("p"), ms::PropertyValue(2)).has_value());
    });
    b_before = Capture(db, *b, base);
    c_before = Capture(db, *c, base);
  }
  ASSERT_NE(b_before.Render(), c_before.Render());

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_EQ(db.version_store()->List().size(), 2u);
  auto b = Checkout(db, "b");
  auto c = Checkout(db, "c");
  ASSERT_NE(b, nullptr);
  ASSERT_NE(c, nullptr);
  EXPECT_EQ(Capture(db, *b, base).Render(), b_before.Render());
  EXPECT_EQ(Capture(db, *c, base).Render(), c_before.Render());
}

// With auto index creation on, a branch write using a new label or edge type must not make the diff engine create an
// index of its own: that index is a metadata delta in the branch WAL, which the replay refuses.
TEST_F(VersioningBranchRecoveryTest, BranchWritesWithAutoIndexCreationOnReplayAfterARestart) {
  const auto r = RunRoundTrip(
      [](Env &e) {
        Mem(e.db)->GetAsyncIndexer().CompleteRemaining();
        e.Txn([&](mv::BranchContext &c, auto &) {
          auto from = c.CreateNativeVertex();
          auto to = c.CreateNativeVertex();
          ASSERT_TRUE(*c.AddLabel(from, e.Label("Fresh")));
          ASSERT_TRUE(c.CreateNativeEdge(from, to, e.EdgeType("FRESH_T")).has_value());
          auto v1 = e.Fork(e.base.v1);
          ASSERT_TRUE(*c.AddLabel(v1, e.Label("FreshFork")));
        });
        e.ctx.diff_engine().GetAsyncIndexer().CompleteRemaining();
      },
      {.auto_index = true});
  EXPECT_TRUE(r.replayed.At(r.base.v1).labels.contains("FreshFork"));
}

TEST_F(VersioningBranchRecoveryTest, ABranchWithoutWritesIsStillBuiltOnFirstCheckout) {
  Base base{};
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto acc = db.Access(ms::WRITE);
    auto v1 = acc->FindVertex(base.v1, ms::View::OLD);
    ASSERT_TRUE(v1.has_value());
    ASSERT_TRUE(v1->SetProperty(db.storage()->NameToProperty("p"), ms::PropertyValue(50)).has_value());
    Commit(*acc);
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  bool built = false;
  auto ctx = Checkout(db, "b", &built);
  ASSERT_NE(ctx, nullptr);
  EXPECT_TRUE(built);
  const auto captured = Capture(db, *ctx, base);
  ASSERT_NE(captured.Find(base.v1), nullptr);
  EXPECT_EQ(captured.Find(base.v1)->props.at("p"), "1") << "the branch still reads main at its fork";
}

// A build with a branch number owns that directory's files whether or not floors were recorded: nothing may move
// them aside into main's backup directory.
TEST_F(VersioningBranchRecoveryTest, ABuildOverAnExistingBranchDirectoryKeepsItsFiles) {
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  CreateBase(db);
  const auto info = db.version_store()->CreateBranch("b");
  ASSERT_TRUE(info.has_value());
  const auto stray = BranchWalDir(info->number) / "stray";
  std::filesystem::create_directories(stray.parent_path());
  std::ofstream{stray} << "kept";

  auto built = mv::BranchCore::Build(*Mem(db), info->fork_ts, info->number);
  ASSERT_TRUE(built.has_value());
  EXPECT_TRUE(std::filesystem::exists(stray));
  EXPECT_FALSE(std::filesystem::exists(kStorageDirectory / "branches" / std::to_string(info->number) / ".backup"));
}

// ---- Gids after a restart

// A native object takes its gid from main's counters, which a restart rebuilds from main's own data: without the
// raise past every replayed native gid, a create on main or on the branch gets the gid of a replayed object.
TEST_F(VersioningBranchRecoveryTest, NewObjectsAfterARestartTakeNoReplayedGid) {
  Base base{};
  NativeWorkload workload{};
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    workload = RunNativeWorkload(env);
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  Env env{db, *ctx, base};

  const auto main_vertex = NewMainVertex(db);
  ASSERT_TRUE(main_vertex.has_value());
  EXPECT_GT(main_vertex->AsUint(), workload.last_vertex.AsUint());
  uint64_t main_edge = 0;
  {
    auto acc = db.Access(ms::WRITE);
    auto from = acc->FindVertex(base.v1, ms::View::OLD);
    auto to = acc->FindVertex(base.v3, ms::View::OLD);
    ASSERT_TRUE(from.has_value() && to.has_value());
    const auto edge = acc->CreateEdge(&*from, &*to, env.EdgeType("T"));
    ASSERT_TRUE(edge.has_value());
    main_edge = edge->Gid().AsUint();
    Commit(*acc);
  }
  EXPECT_GT(main_edge, workload.last_edge.AsUint());

  uint64_t branch_vertex = 0;
  uint64_t branch_edge = 0;
  env.Txn([&](mv::BranchContext &c, auto &) {
    auto native = c.CreateNativeVertex();
    branch_vertex = native.Gid().AsUint();
    auto stub = env.Stub(base.v2);
    auto edge = c.CreateNativeEdge(stub, native, env.EdgeType("T"));
    ASSERT_TRUE(edge.has_value());
    branch_edge = edge->Gid().AsUint();
  });
  EXPECT_GT(branch_vertex, workload.last_vertex.AsUint());
  EXPECT_NE(branch_vertex, main_vertex->AsUint());
  EXPECT_GT(branch_edge, workload.last_edge.AsUint());
  EXPECT_NE(branch_edge, main_edge);
}

// Versioning off: the branches are hidden, but main keeps running over them and must stay clear of their natives.
TEST_F(VersioningBranchRecoveryTest, MainIdsWhileHiddenClearTheBranchNativeIds) {
  Base base{};
  NativeWorkload workload{};
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    workload = RunNativeWorkload(env);
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true, {.versioning = false})};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  EXPECT_TRUE(db.version_store()->List().empty());
  const auto vertex = NewMainVertex(db);
  ASSERT_TRUE(vertex.has_value());
  EXPECT_GT(vertex->AsUint(), workload.last_vertex.AsUint());
  auto acc = db.Access(ms::WRITE);
  auto from = acc->FindVertex(base.v1, ms::View::OLD);
  auto to = acc->FindVertex(base.v3, ms::View::OLD);
  ASSERT_TRUE(from.has_value() && to.has_value());
  const auto edge = acc->CreateEdge(&*from, &*to, db.storage()->NameToEdgeType("T"));
  ASSERT_TRUE(edge.has_value());
  EXPECT_GT(edge->Gid().AsUint(), workload.last_edge.AsUint());
  acc->Abort();
}

// A fork taken with no main commit after it sits above the durable timestamp; commits made while the branch is hidden
// must still land above the fork point or they would show up in the branch.
TEST_F(VersioningBranchRecoveryTest, MainCommitsWhileHiddenStayOutOfTheBranch) {
  Base base{};
  uint64_t fork_ts = 0;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    for (int i = 0; i < 10; ++i) db.Access(ms::READ)->Abort();
    const auto info = db.version_store()->CreateBranch("b");
    ASSERT_TRUE(info.has_value());
    fork_ts = info->fork_ts;
  }

  ms::Gid hidden_run_vertex;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true, {.versioning = false})};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto vertex = NewMainVertex(db);
    ASSERT_TRUE(vertex.has_value());
    hidden_run_vertex = *vertex;
    EXPECT_GE(Mem(db)->repl_storage_state_.commit_ts_info_.load().ldt_, fork_ts)
        << "a commit made while hidden must be stamped at or above the fork point";
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto info = db.version_store()->Get("b");
  ASSERT_TRUE(info.has_value());
  auto branch = Mem(db)->HistoricalAccess(info->fork_ts);
  ASSERT_TRUE(branch.has_value());
  EXPECT_FALSE((*branch)->FindVertex(hidden_run_vertex, ms::View::OLD).has_value());
  EXPECT_TRUE((*branch)->FindVertex(base.v1, ms::View::OLD).has_value());
  EXPECT_TRUE(db.Access(ms::READ)->FindVertex(hidden_run_vertex, ms::View::OLD).has_value());
}

// ---- Refusals

namespace {
// Removes the first WAL file of `number`'s chain.
void RemoveFirstWalFile(uint64_t number) {
  auto files = WalFiles(number);
  ASSERT_EQ(files.size(), 2U);
  const auto first = std::ranges::find_if(
      files, [](const auto &path) { return memgraph::storage::durability::ReadWalInfo(path).seq_num == 0; });
  ASSERT_NE(first, files.end());
  std::filesystem::remove(*first);
}

// A branch "b" with a native workload, written to in two lifetimes so its WAL is a chain of two files.
struct TwoFileBranch {
  uint64_t number{0};
  Base base{};
  NativeWorkload workload{};
};

TwoFileBranch CreateTwoFileBranch(std::string_view name) {
  TwoFileBranch out;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    out.base = CreateBase(db);
    auto info = db.version_store()->CreateBranch(std::string{name});
    EXPECT_TRUE(info.has_value());
    if (!info) return out;
    out.number = info->number;
    auto ctx = Checkout(db, name);
    EXPECT_NE(ctx, nullptr);
    if (ctx == nullptr) return out;
    Env env{db, *ctx, out.base};
    out.workload = RunNativeWorkload(env);
  }
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto ctx = Checkout(db, name);
  EXPECT_NE(ctx, nullptr);
  if (ctx == nullptr) return out;
  Env env{db, *ctx, out.base};
  WriteAfterRestart(env, out.workload.n1, 2);
  return out;
}
}  // namespace

// A branch whose WAL does not replay makes the instance refuse to start, with a message that names the branch, the
// error and the files, and nothing on disk is touched.
TEST_F(VersioningBranchRecoveryTest, AMissingWalFileRefusesStartupAndLeavesEveryFileAsItIs) {
  const auto branch = CreateTwoFileBranch("b");
  ASSERT_NO_FATAL_FAILURE(RemoveFirstWalFile(branch.number));
  const auto before = DurableFiles();

  const auto message = StartupRefusal();
  ASSERT_FALSE(message.empty()) << "a branch that cannot be replayed must stop the startup";
  EXPECT_THAT(message, HasSubstr("'b'"));
  EXPECT_THAT(message, HasSubstr("missing prefix WAL files"));
  EXPECT_THAT(message, HasSubstr((kStorageDirectory / "branches" / std::to_string(branch.number)).string()));
  EXPECT_THAT(message, HasSubstr(BranchWalDir(branch.number).string()));
  EXPECT_THAT(message, HasSubstr((kStorageDirectory / "branches" / "ledger").string()));
  EXPECT_EQ(DurableFiles(), before) << "a refused startup changes no file";
  EXPECT_EQ(StartupRefusal(), message) << "and refuses the same way again";
  EXPECT_EQ(DurableFiles(), before);
}

// The steps in the message are the way out: followed to the letter, the instance starts, main and the other branch
// are as they were, the moved log is kept, and the branch comes back as it was forked.
TEST_F(VersioningBranchRecoveryTest, FollowingTheRefusalStepsStartsTheInstanceWithTheOtherBranchesIntact) {
  Base base{};
  NativeWorkload workload{};
  uint64_t bad = 0;
  Captured good_before;
  std::string main_before;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    const auto b = db.version_store()->CreateBranch("bad");
    ASSERT_TRUE(b.has_value());
    bad = b->number;
    ASSERT_TRUE(db.version_store()->CreateBranch("good").has_value());
    auto bad_ctx = Checkout(db, "bad");
    auto good_ctx = Checkout(db, "good");
    ASSERT_NE(bad_ctx, nullptr);
    ASSERT_NE(good_ctx, nullptr);
    Env bad_env{db, *bad_ctx, base};
    Env good_env{db, *good_ctx, base};
    workload = RunNativeWorkload(bad_env);
    RunNativeWorkload(good_env);
    good_before = Capture(db, *good_ctx, base);
    main_before = DumpMain(db);
  }
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto ctx = Checkout(db, "bad");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    WriteAfterRestart(env, workload.n1, 2);
  }
  ASSERT_NO_FATAL_FAILURE(RemoveFirstWalFile(bad));
  const auto bad_log = FileContents(BranchWalDir(bad));
  const auto before = DurableFiles();

  const auto message = StartupRefusal();
  ASSERT_FALSE(message.empty());
  EXPECT_THAT(message, HasSubstr("'bad'"));
  EXPECT_THAT(message, ::testing::Not(HasSubstr("'good'"))) << "only the branch that fails is named";
  EXPECT_EQ(DurableFiles(), before);

  ASSERT_NO_FATAL_FAILURE(FollowMoveSteps(message));
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto *store = db.version_store();
  EXPECT_EQ(DumpMain(db), main_before);
  EXPECT_EQ(store->List().size(), 2U);
  EXPECT_EQ(FileContents(BranchWalDir(bad).string() + ".damaged"), bad_log) << "the moved log is kept byte for byte";

  bool built = false;
  auto good = Checkout(db, "good", &built);
  ASSERT_NE(good, nullptr);
  EXPECT_FALSE(built) << "the good branch was recovered at startup";
  EXPECT_EQ(Capture(db, *good, base).Render(), good_before.Render());
  good.reset();

  auto fresh = Checkout(db, "bad", &built);
  ASSERT_NE(fresh, nullptr);
  EXPECT_TRUE(built);
  EXPECT_EQ(Capture(db, *fresh, base).vertices.size(), 4U) << "the branch reads main at its fork, without its writes";
  fresh.reset();

  EXPECT_NE(NewMainVertex(db), std::nullopt);
  ASSERT_EQ(store->DropBranch("bad"), mv::DropResult::kDropped);
  EXPECT_TRUE(store->Exists("good"));
}

// A branch is only replayed over the main history it forked from: a fork vertex that history does not have is a
// refusal, never a skipped record.
TEST_F(VersioningBranchRecoveryTest, ABranchWhoseMainHistoryLacksAForkObjectIsRefused) {
  uint64_t number = 0;
  std::pair<uint64_t, uint64_t> floors{};
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b");
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto v4 = env.Fork(base.v4);
      ASSERT_TRUE(*c.AddLabel(v4, env.Label("X")));
    });
    floors = {ctx->core()->vertex_gid_floor(), ctx->core()->edge_gid_floor()};
  }

  // Another main whose history has only three vertices: the branch's fork vertex (gid 3) is not in it.
  memgraph::dbms::Database other{MakeConfig(/*recover_on_startup=*/false, {}, kOtherStorageDirectory)};
  const memgraph::memory::DbArenaScope arena_scope{&other.Arena()};
  {
    auto acc = other.Access(ms::WRITE);
    for (int i = 0; i < 3; ++i) acc->CreateVertex();
    Commit(*acc);
  }
  auto info = other.version_store()->CreateBranch("o");
  ASSERT_TRUE(info.has_value());
  auto core = mv::BranchCore::Build(
      *Mem(other),
      info->fork_ts,
      info->number,
      mv::BranchCore::RecordedFloors{.vertex_gid_floor = floors.first, .edge_gid_floor = floors.second});
  ASSERT_TRUE(core.has_value());
  const auto replayed = mv::ReplayBranchWal(*core, BranchWalDir(number));
  ASSERT_FALSE(replayed.has_value());
  EXPECT_THAT(replayed.error(), HasSubstr("is not in the branch"));
}

// Records are routed by the gid floors; floors that disagree with the log make a record land on the wrong side, and
// that refuses the startup too. Moving the log aside also brings that branch back.
TEST_F(VersioningBranchRecoveryTest, ABranchWhoseRecordedFloorsDisagreeWithItsLogRefusesStartup) {
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto base = CreateBase(db);
    const auto info = db.version_store()->CreateBranch("b");
    ASSERT_TRUE(info.has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto n1 = c.CreateNativeVertex();
      auto n2 = c.CreateNativeVertex();
      ASSERT_TRUE(c.CreateNativeEdge(n1, n2, env.EdgeType("T")).has_value());
    });
  }
  const mv::BranchLedger ledger{kStorageDirectory / "branches" / "ledger"};
  auto contents = ledger.Read();
  ASSERT_TRUE(contents.has_value() && contents->has_value());
  for (auto &branch : (*contents)->branches) {
    *branch.info.vertex_gid_floor += 1000;
    *branch.info.edge_gid_floor += 1000;
  }
  ASSERT_TRUE(ledger.Write(**contents).has_value());
  const auto before = DurableFiles();

  const auto message = StartupRefusal();
  ASSERT_FALSE(message.empty());
  EXPECT_THAT(message, HasSubstr("'b'"));
  EXPECT_THAT(message, HasSubstr("a gid of the fork state"));
  EXPECT_EQ(DurableFiles(), before);

  ASSERT_NO_FATAL_FAILURE(FollowMoveSteps(message));
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  EXPECT_TRUE(db.version_store()->Exists("b"));
}
