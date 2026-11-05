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

// A branch comes back after a restart by replaying its diff-engine WAL: a record on a fork object is written again
// through the BranchContext write resolver, a native one is the diff engine's stock operation. Each case runs a
// workload, restarts, replays, and compares what a session reads and the branch's bookkeeping.
#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
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

struct RoundTrip {
  Captured live;
  Captured replayed;
  Base base;
};

// Runs `workload` on a durable branch, restarts the database, replays the branch WAL, and requires everything the
// session reads and everything the branch's bookkeeping says to be the same as before, and main untouched.
RoundTrip RunRoundTrip(const std::function<void(Env &)> &workload, Options options = {}) {
  RoundTrip out;
  std::string main_before;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false, options)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    out.base = CreateBase(db, options.properties_on_edges);
    EXPECT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto ctx = Checkout(db, "b");
    EXPECT_NE(ctx, nullptr);
    if (ctx == nullptr) return out;
    Env env{db, *ctx, out.base};
    workload(env);
    if (::testing::Test::HasFatalFailure()) return out;
    out.live = Capture(db, *ctx, out.base);
    main_before = DumpMain(db);
  }
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true, options)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  bool built = false;
  auto ctx = Checkout(db, "b", &built);
  EXPECT_NE(ctx, nullptr);
  EXPECT_FALSE(built) << "startup recovery registered the core";
  if (ctx == nullptr) return out;
  out.replayed = Capture(db, *ctx, out.base);
  EXPECT_EQ(out.replayed.Render(), out.live.Render());
  EXPECT_EQ(DumpMain(db), main_before) << "branch writes must not reach main";
  return out;
}

std::string Joined(const std::set<std::string> &names) {
  std::string joined;
  for (const auto &name : names) joined += name + ",";
  return joined;
}
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

TEST_F(VersioningBranchRecoveryTest, ReAddingALabelRemovedByAnEarlierTransactionAndAddingAnotherReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.Fork(e.base.v1);
      ASSERT_TRUE(*c.RemoveLabel(v1, e.Label("L")));
    });
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.Fork(e.base.v1);
      ASSERT_TRUE(*c.AddLabel(v1, e.Label("L")));
      ASSERT_TRUE(*c.AddLabel(v1, e.Label("X")));
    });
  });
  EXPECT_EQ(Joined(r.replayed.At(r.base.v1).labels), "L,M,X,");
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
  for (const bool properties_on_edges : {true, false}) {
    SCOPED_TRACE(properties_on_edges ? "properties on edges" : "no edge properties");
    std::filesystem::remove_all(kStorageDirectory);
    auto const r = RunRoundTrip(
        [](Env &e) {
          e.Txn([&](mv::BranchContext &c, auto &) {
            auto edge = e.ForkEdge(e.base.e1, e.base.v1);
            ASSERT_TRUE(*c.DeleteForkEdge(edge));
          });
        },
        {.properties_on_edges = properties_on_edges});
    EXPECT_TRUE(r.replayed.At(r.base.v1).out.empty());
    EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("deletes v=0 e=1"));
    EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("filter e" + std::to_string(r.base.e1.AsUint()) + " deleted=1"));
  }
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

// Every native record kind across two transactions, with fork-object writes around them.
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
    // Timestamps the diff engine hands out without a WAL record: after the restart its clock must still move past the
    // logged commits, or the next run's file sorts before this one.
    for (int i = 0; i < 50; ++i) ctx->diff_engine().Access(ms::READ)->Abort();
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
    const auto replayed = Capture(db, *ctx, base);
    EXPECT_EQ(replayed.Render(), after_first.Render());
    EXPECT_EQ(replayed.vertices.size(), 6u) << "4 fork vertices, 3 native, one of them deleted";
    EXPECT_EQ(Joined(replayed.At(workload.n1).labels), "N,");
    EXPECT_EQ(Joined(replayed.At(base.v1).labels), "M,");
    EXPECT_EQ(replayed.At(base.v1).props.at("p"), "9");

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

namespace {
mv::BranchInfo LedgerInfoOf(std::string_view name) {
  const auto contents = mv::BranchLedger{kStorageDirectory / "branches" / "ledger"}.Read();
  EXPECT_TRUE(contents.has_value() && contents->has_value());
  if (!contents.has_value() || !contents->has_value()) return {};
  const auto branch = std::ranges::find((*contents)->branches, name, &mv::LedgerBranch::name);
  EXPECT_NE(branch, (*contents)->branches.end());
  return branch == (*contents)->branches.end() ? mv::BranchInfo{} : branch->info;
}

void ExpectMainClearOfTheLedgersGids(memgraph::dbms::Database &db, const mv::BranchInfo &ledger_info) {
  EXPECT_GE(Mem(db)->vertex_id_.load(), mv::GidsHeldBelow(ledger_info, mv::GidKind::kVertex));
  EXPECT_GE(Mem(db)->edge_id_.load(), mv::GidsHeldBelow(ledger_info, mv::GidKind::kEdge));
}
}  // namespace

// A native object takes its gid from main's counters, which a restart rebuilds from main's own data: without the
// raise to the ledger's ceilings, a create on main or on the branch gets the gid of a replayed object.
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

  // The recovered branch's new native gids are reserved in the ledger too, so a crash now keeps main clear of them.
  const auto ledger_info = LedgerInfoOf("b");
  EXPECT_GT(mv::GidsHeldBelow(ledger_info, mv::GidKind::kVertex), branch_vertex);
  EXPECT_GT(mv::GidsHeldBelow(ledger_info, mv::GidKind::kEdge), branch_edge);
}

namespace {
// What a crash that hit right after the branch took its last native gids leaves of the ledger.
struct CrashedBranch {
  std::string ledger;
  uint64_t last_vertex{0};
  uint64_t last_edge{0};
};

std::string ReadLedgerFile() {
  std::ifstream in{kStorageDirectory / "branches" / "ledger", std::ios::binary};
  return std::string{std::istreambuf_iterator<char>{in}, {}};
}

// A branch takes more native gids than one reserved block holds. The ledger is read right after each of the last
// creates, which is where a crash would have left it; the files of the clean close that follows are otherwise kept.
CrashedBranch TakeMoreGidsThanOneBlock() {
  constexpr uint64_t kCreates = mv::kGidReserveChunk + 16;
  CrashedBranch out;
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto base = CreateBase(db);
  EXPECT_TRUE(db.version_store()->CreateBranch("b").has_value());
  auto ctx = Checkout(db, "b");
  EXPECT_NE(ctx, nullptr);
  if (ctx == nullptr) return out;
  Env env{db, *ctx, base};
  env.Txn([&](mv::BranchContext &c, auto &) {
    auto first = c.CreateNativeVertex();
    auto second = c.CreateNativeVertex();
    for (uint64_t i = 0; i < kCreates; ++i) {
      auto vertex = c.CreateNativeVertex();
      out.last_vertex = vertex.Gid().AsUint();
      const auto edge = c.CreateNativeEdge(first, second, env.EdgeType("T"));
      EXPECT_TRUE(edge.has_value());
      out.last_edge = edge->Gid().AsUint();
      if (i + 8 >= kCreates) out.ledger = ReadLedgerFile();
    }
  });
  return out;
}

class GidReservationTest : public VersioningBranchRecoveryTest, public ::testing::WithParamInterface<bool> {};
}  // namespace

// A crash after the branch took gids from more than one reserved block: the ledger holds every block handed out, so
// main, with versioning on or off and without reading the branch's WAL, never reuses a native gid of the branch.
TEST_P(GidReservationTest, MainStaysClearOfTheGidsABranchTookBeforeACrash) {
  const auto crashed = TakeMoreGidsThanOneBlock();
  ASSERT_FALSE(crashed.ledger.empty());
  {
    std::ofstream out{kStorageDirectory / "branches" / "ledger", std::ios::binary | std::ios::trunc};
    out.write(crashed.ledger.data(), static_cast<std::streamsize>(crashed.ledger.size()));
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true, {.versioning = GetParam()})};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto vertex = NewMainVertex(db);
  ASSERT_TRUE(vertex.has_value());
  EXPECT_GT(vertex->AsUint(), crashed.last_vertex);
  auto acc = db.Access(ms::WRITE);
  auto from = acc->CreateVertex();
  auto to = acc->CreateVertex();
  const auto edge = acc->CreateEdge(&from, &to, db.storage()->NameToEdgeType("T"));
  ASSERT_TRUE(edge.has_value());
  EXPECT_GT(edge->Gid().AsUint(), crashed.last_edge);
  acc->Abort();
}

INSTANTIATE_TEST_SUITE_P(Versioning, GidReservationTest, ::testing::Bool(),
                         [](const auto &info) { return info.param ? "On" : "Off"; });

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

// ---- Branches that cannot be recovered are set aside

namespace {
class WarningCapture {
 public:
  WarningCapture() : sink_(std::make_shared<Sink>()), previous_(spdlog::default_logger()) {
    auto logger = std::make_shared<spdlog::logger>("branch_recovery_test", sink_);
    logger->set_level(spdlog::level::warn);
    spdlog::set_default_logger(std::move(logger));
  }

  ~WarningCapture() { spdlog::set_default_logger(previous_); }

  WarningCapture(const WarningCapture &) = delete;
  WarningCapture &operator=(const WarningCapture &) = delete;

  std::string Warnings() const {
    std::string out;
    for (const auto &message : sink_->messages) {
      if (message.starts_with("Graph Versioning:")) out += message + "\n";
    }
    return out;
  }

 private:
  struct Sink : spdlog::sinks::base_sink<std::mutex> {
    std::vector<std::string> messages;

   protected:
    void sink_it_(const spdlog::details::log_msg &msg) override {
      messages.emplace_back(msg.payload.data(), msg.payload.size());
    }

    void flush_() override {}
  };

  std::shared_ptr<Sink> sink_;
  std::shared_ptr<spdlog::logger> previous_;
};

void RemoveFirstWalFile(uint64_t number) {
  auto files = WalFiles(number);
  ASSERT_EQ(files.size(), 2U);
  const auto first = std::ranges::find_if(
      files, [](const auto &path) { return memgraph::storage::durability::ReadWalInfo(path).seq_num == 0; });
  ASSERT_NE(first, files.end());
  std::filesystem::remove(*first);
}

// A branch with a native workload, written to in two lifetimes so its WAL is a chain of two files.
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

// A branch whose WAL does not replay is set aside as unavailable with the reason and main starts; the start warns what
// to do, no file changes, and main stays clear of the gids the branch may hold.
TEST_F(VersioningBranchRecoveryTest, AMissingWalFileSetsTheBranchAsideAndStartsTheInstance) {
  const auto branch = CreateTwoFileBranch("b");
  ASSERT_NO_FATAL_FAILURE(RemoveFirstWalFile(branch.number));
  const auto before = FileContents(kStorageDirectory / "branches");
  const auto ledger_info = LedgerInfoOf("b");
  ASSERT_GT(mv::GidsHeldBelow(ledger_info, mv::GidKind::kVertex), 0U);

  for (int start = 0; start < 2; ++start) {
    SCOPED_TRACE("start " + std::to_string(start));
    WarningCapture capture;
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto info = db.version_store()->Get("b");
    ASSERT_TRUE(info.has_value());
    ASSERT_TRUE(info->unavailable_reason.has_value()) << "a branch that cannot be replayed is set aside";
    EXPECT_THAT(*info->unavailable_reason, HasSubstr("missing prefix WAL files"));
    EXPECT_EQ(DumpMain(db).empty(), false);

    const auto warnings = capture.Warnings();
    EXPECT_THAT(warnings, HasSubstr("'b'"));
    EXPECT_THAT(warnings, HasSubstr("unavailable"));
    EXPECT_THAT(warnings, HasSubstr("missing prefix WAL files"));
    EXPECT_THAT(warnings, HasSubstr((kStorageDirectory / "branches" / std::to_string(branch.number)).string()));
    EXPECT_THAT(warnings, HasSubstr(BranchWalDir(branch.number).string()));
    EXPECT_THAT(warnings, HasSubstr((kStorageDirectory / "branches" / "ledger").string()));
    EXPECT_THAT(warnings, HasSubstr("restore its files"));
    EXPECT_THAT(warnings, HasSubstr("DROP BRANCH 'b'"));

    EXPECT_THAT(db.version_store()->TryAcquireCheckout("b").error(), HasSubstr("is unavailable"));
    ExpectMainClearOfTheLedgersGids(db, ledger_info);
  }
  EXPECT_EQ(FileContents(kStorageDirectory / "branches"), before) << "setting a branch aside changes no file";
}

// A branch set aside costs main and the other branches nothing, and has both ways out: DROP BRANCH removes it, and
// restoring its file and restarting brings it back as it was.
TEST_F(VersioningBranchRecoveryTest, AnUnavailableBranchLeavesMainAndTheOtherBranchIntactAndCanBeDroppedOrRestored) {
  Base base{};
  NativeWorkload workload{};
  uint64_t bad = 0;
  Captured good_before;
  std::string bad_before;
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
    bad_before = Capture(db, *ctx, base).Render();
  }
  const auto bad_log = FileContents(BranchWalDir(bad));
  ASSERT_NO_FATAL_FAILURE(RemoveFirstWalFile(bad));
  const auto ledger_info = LedgerInfoOf("bad");

  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *store = db.version_store();
    EXPECT_TRUE(store->Get("bad")->unavailable_reason.has_value());
    EXPECT_FALSE(store->Get("good")->unavailable_reason.has_value());
    EXPECT_EQ(DumpMain(db), main_before);
    bool built = false;
    auto good = Checkout(db, "good", &built);
    ASSERT_NE(good, nullptr);
    EXPECT_FALSE(built) << "the good branch was recovered at startup";
    EXPECT_EQ(Capture(db, *good, base).Render(), good_before.Render());
    good.reset();

    const auto fresh = NewMainVertex(db);
    ASSERT_TRUE(fresh.has_value());
    EXPECT_GT(fresh->AsUint(), workload.last_vertex.AsUint()) << "main takes no gid the branch holds";
    ExpectMainClearOfTheLedgersGids(db, ledger_info);
  }

  // The lost file comes back: the branch is as it was.
  for (const auto &[relative, bytes] : bad_log) {
    const auto path = BranchWalDir(bad) / std::filesystem::path{relative}.filename();
    if (!std::filesystem::exists(path)) std::ofstream{path, std::ios::binary} << bytes;
  }
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    EXPECT_FALSE(db.version_store()->Get("bad")->unavailable_reason.has_value());
    bool built = false;
    auto ctx = Checkout(db, "bad", &built);
    ASSERT_NE(ctx, nullptr);
    EXPECT_FALSE(built);
    EXPECT_EQ(Capture(db, *ctx, base).Render(), bad_before);
  }

  // Or it is dropped: the files go, the name is free, the other branch stays.
  ASSERT_NO_FATAL_FAILURE(RemoveFirstWalFile(bad));
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *store = db.version_store();
    ASSERT_TRUE(store->Get("bad")->unavailable_reason.has_value());
    ASSERT_EQ(store->DropBranch("bad"), mv::DropResult::kDropped);
    EXPECT_FALSE(std::filesystem::exists(kStorageDirectory / "branches" / std::to_string(bad)));
  }
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  EXPECT_FALSE(db.version_store()->Exists("bad"));
  EXPECT_TRUE(db.version_store()->Exists("good"));
  EXPECT_TRUE(db.version_store()->CreateBranch("bad").has_value());
}

// A branch is only replayed over the main history it forked from: a fork vertex that history does not have is damage,
// never a skipped record.
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

namespace {
// Runs `workload` on a fresh durable branch "b" and closes the database, leaving the branch's WAL behind.
void WriteBranch(const std::function<void(Env &)> &workload) {
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto base = CreateBase(db);
  ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  Env env{db, *ctx, base};
  workload(env);
}

// Overwrites the byte at `offset` of `file` and returns what was there.
char OverwriteByte(const std::filesystem::path &file, uint64_t offset, char value) {
  std::fstream stream{file, std::ios::in | std::ios::out | std::ios::binary};
  char old{};
  stream.seekg(static_cast<std::streamoff>(offset));
  stream.get(old);
  stream.seekp(static_cast<std::streamoff>(offset));
  stream.put(value);
  EXPECT_TRUE(stream.good()) << file;
  return old;
}

// A workload and the substring of the error its log must replay with.
struct RefusalCase {
  std::string_view expected;
  std::function<void(Env &)> workload;
};
}  // namespace

// Records are routed by the gid floors; floors that disagree with the log make a create record land on the fork side,
// and that sets the branch aside too, with the instance up.
TEST_F(VersioningBranchRecoveryTest, ABranchWhoseRecordedFloorsDisagreeWithItsLogIsSetAside) {
  const std::vector<RefusalCase> cases{
      {"creates vertex",
       [](Env &e) {
         e.Txn([&](mv::BranchContext &c, auto &) {
           auto n1 = c.CreateNativeVertex();
           auto n2 = c.CreateNativeVertex();
           ASSERT_TRUE(c.CreateNativeEdge(n1, n2, e.EdgeType("T")).has_value());
         });
       }},
      // An edge between two fork vertices' stubs: the log's only create record is the edge's.
      {"creates edge",
       [](Env &e) {
         e.Txn([&](mv::BranchContext &c, auto &) {
           auto s4 = e.Stub(e.base.v4);
           auto s3 = e.Stub(e.base.v3);
           ASSERT_TRUE(c.CreateNativeEdge(s4, s3, e.EdgeType("T")).has_value());
         });
       }},
  };
  for (const auto &[expected, workload] : cases) {
    SCOPED_TRACE(expected);
    std::filesystem::remove_all(kStorageDirectory);
    WriteBranch(workload);
    const mv::BranchLedger ledger{kStorageDirectory / "branches" / "ledger"};
    auto contents = ledger.Read();
    ASSERT_TRUE(contents.has_value() && contents->has_value());
    for (auto &branch : (*contents)->branches) {
      *branch.info.vertex_gid_floor += 1000;
      *branch.info.edge_gid_floor += 1000;
    }
    ASSERT_TRUE(ledger.Write(**contents).has_value());
    const auto before = FileContents(kStorageDirectory / "branches");

    {
      memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
      const auto info = db.version_store()->Get("b");
      ASSERT_TRUE(info.has_value());
      ASSERT_TRUE(info->unavailable_reason.has_value());
      EXPECT_THAT(*info->unavailable_reason, HasSubstr(std::string{expected} + " "));
      EXPECT_THAT(*info->unavailable_reason, HasSubstr("a gid of the fork state"));
    }
    EXPECT_EQ(FileContents(kStorageDirectory / "branches"), before);
  }
}

// A record the branch already holds does not apply again: replaying a log over its own result is refused.
TEST_F(VersioningBranchRecoveryTest, ReplayingALogOverItsOwnResultIsRefused) {
  const std::vector<RefusalCase> cases{
      {"Fork edge",
       [](Env &e) {
         e.Txn([&](mv::BranchContext &c, auto &) {
           auto edge = e.ForkEdge(e.base.e1, e.base.v1);
           ASSERT_TRUE(*c.DeleteForkEdge(edge));
         });
       }},
      {"Fork vertex",
       [](Env &e) { e.Txn([&](mv::BranchContext &c, auto &) { ASSERT_TRUE(*c.DeleteForkVertex(e.base.v4)); }); }},
      {"already exists in the branch",
       [](Env &e) { e.Txn([&](mv::BranchContext &c, auto &) { c.CreateNativeVertex(); }); }},
      // The log's only create record is the edge's, between two fork vertices' stubs.
      {"Edge",
       [](Env &e) {
         e.Txn([&](mv::BranchContext &c, auto &) {
           auto s4 = e.Stub(e.base.v4);
           auto s3 = e.Stub(e.base.v3);
           ASSERT_TRUE(c.CreateNativeEdge(s4, s3, e.EdgeType("T")).has_value());
         });
       }},
  };
  for (const auto &[expected, workload] : cases) {
    SCOPED_TRACE(expected);
    std::filesystem::remove_all(kStorageDirectory);
    WriteBranch(workload);
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto info = db.version_store()->Get("b");
    ASSERT_TRUE(info.has_value() && info->vertex_gid_floor && info->edge_gid_floor);
    auto core = mv::BranchCore::Build(*Mem(db),
                                      info->fork_ts,
                                      info->number,
                                      mv::BranchCore::RecordedFloors{.vertex_gid_floor = *info->vertex_gid_floor,
                                                                     .edge_gid_floor = *info->edge_gid_floor});
    ASSERT_TRUE(core.has_value());
    ASSERT_TRUE(mv::ReplayBranchWal(*core, BranchWalDir(info->number)).has_value());
    const auto again = mv::ReplayBranchWal(*core, BranchWalDir(info->number));
    ASSERT_FALSE(again.has_value());
    EXPECT_THAT(again.error(), HasSubstr(expected));
  }
}

namespace {
uint64_t CreateWrittenBranch() {
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto base = CreateBase(db);
  const auto info = db.version_store()->CreateBranch("b");
  EXPECT_TRUE(info.has_value());
  if (!info) return 0;
  auto ctx = Checkout(db, "b");
  EXPECT_NE(ctx, nullptr);
  if (ctx == nullptr) return 0;
  Env env{db, *ctx, base};
  env.Txn([&](mv::BranchContext &c, auto &) { [[maybe_unused]] auto vertex = c.CreateNativeVertex(); });
  return info->number;
}

struct Started {
  bool listed{false};
  std::optional<std::string> reason;
  std::string warnings;
};

Started StartedOver() {
  WarningCapture capture;
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  Started out;
  const auto info = db.version_store()->Get("b");
  out.listed = info.has_value();
  if (info) out.reason = info->unavailable_reason;
  out.warnings = capture.Warnings();
  return out;
}
}  // namespace

// ---- Main's recovered history short of a fork

namespace {
// The parameter: whether main takes a write while it runs without recovery. Such a commit is stamped above the fork, so
// main's last durable timestamp no longer says anything about its missing history.
class MainShortOfTheForkTest : public VersioningBranchRecoveryTest, public ::testing::WithParamInterface<bool> {};
}  // namespace

// Main restarted with versioning and recovery off starts empty while the branch stays hidden; with both back on, main's
// history ends before the fork, so the branch is set aside with that reason, its recorded fork untouched, main starts.
TEST_P(MainShortOfTheForkTest, AMainThatRecoveredLessThanTheBranchForkedFromSetsTheBranchAside) {
  Base base{};
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    env.Txn([&](mv::BranchContext &c, auto &) { [[maybe_unused]] auto vertex = c.CreateNativeVertex(); });
  }
  const auto recorded = LedgerInfoOf("b");
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false, {.versioning = false})};
    ASSERT_NE(db.version_store(), nullptr);
    EXPECT_FALSE(db.version_store()->Empty());
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    if (GetParam()) ASSERT_TRUE(NewMainVertex(db).has_value());
  }

  WarningCapture capture;
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto info = db.version_store()->Get("b");
  ASSERT_TRUE(info.has_value());
  ASSERT_TRUE(info->unavailable_reason.has_value()) << "the branch came back as available without its fork base";
  EXPECT_THAT(*info->unavailable_reason, HasSubstr("before the fork"));
  EXPECT_THAT(capture.Warnings(), HasSubstr("unavailable"));
  EXPECT_EQ(LedgerInfoOf("b").recorded_fork_ts, recorded.recorded_fork_ts) << "the recorded fork is never rewritten";
  EXPECT_THAT(db.version_store()->TryAcquireCheckout("b").error(), HasSubstr("is unavailable"));
  EXPECT_FALSE(db.Access(ms::READ)->FindVertex(base.v1, ms::View::OLD).has_value()) << "main is what it recovered";
}

INSTANTIATE_TEST_SUITE_P(Versioning, MainShortOfTheForkTest, ::testing::Bool(),
                         [](const auto &info) { return info.param ? "AfterAWriteWhileHidden" : "NoWrite"; });

// Transactions that commit nothing also take timestamps, so a fork can sit above main's last durable commit; that is no
// shortfall, nor is a later fork after main commits replayed from its WAL tail.
TEST_F(VersioningBranchRecoveryTest, AForkAboveMainsLastCommitIsNotAShortfall) {
  Base base{};
  ms::Gid later_vertex;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    for (int i = 0; i < 10; ++i) db.Access(ms::READ)->Abort();
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    const auto vertex = NewMainVertex(db);
    ASSERT_TRUE(vertex.has_value());
    later_vertex = *vertex;
    ASSERT_TRUE(db.version_store()->CreateBranch("c").has_value());
  }
  for (const bool versioning : {true, false, true}) {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true, {.versioning = versioning})};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    if (!versioning) continue;
    for (const auto *name : {"b", "c"}) {
      const auto info = db.version_store()->Get(name);
      ASSERT_TRUE(info.has_value()) << name;
      EXPECT_FALSE(info->unavailable_reason.has_value()) << name << ": " << info->unavailable_reason.value_or("");
      auto branch = Mem(db)->HistoricalAccess(info->fork_ts);
      ASSERT_TRUE(branch.has_value()) << name;
      EXPECT_TRUE((*branch)->FindVertex(base.v1, ms::View::OLD).has_value()) << name;
      EXPECT_EQ((*branch)->FindVertex(later_vertex, ms::View::OLD).has_value(), std::string_view{name} == "c")
          << name << " sees main's commit between the forks only if it forked after it";
    }
  }
}

// ---- Replay cost

// Reading a main vertex as of the fork walks all of main's later history of it, so replay must read each fork vertex
// once, not once per record: `kBranchTransactions` writes to v1 still read it once.
TEST_F(VersioningBranchRecoveryTest, ReplayReadsAForkVertexOnceNotOncePerTransaction) {
  constexpr int kBranchTransactions = 200;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto base = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    for (int i = 0; i < kBranchTransactions; ++i) {
      env.Txn([&](mv::BranchContext &c, auto &) {
        auto fork = env.Fork(base.v1);
        EXPECT_TRUE(c.SetProperty(fork, env.Prop("p"), ms::PropertyValue(i)).has_value());
        auto from = env.Stub(base.v1);
        auto to = c.CreateNativeVertex();
        EXPECT_TRUE(c.CreateNativeEdge(from, to, env.EdgeType("T")).has_value());
      });
    }
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  EXPECT_EQ(ctx->core()->ForkVertexReads(), 1U) << "one fork vertex is replayed, however many records name it";
}

// ---- Damaged or missing branch files

// While versioning is off a branch's WAL is never read at startup, however large it is: the branches are only hidden. A
// WAL that no longer decodes costs the start nothing and says nothing; versioning on would set the branch aside.
TEST_F(VersioningBranchRecoveryTest, HiddenStartupNeverReadsTheBranchWal) {
  const auto number = CreateWrittenBranch();
  const auto files = WalFiles(number);
  ASSERT_FALSE(files.empty());
  for (const auto &file : files) {
    const auto offset = memgraph::storage::durability::ReadWalInfo(file).offset_deltas;
    const auto size = std::filesystem::file_size(file);
    ASSERT_GT(size, offset);
    std::fstream out{file, std::ios::in | std::ios::out | std::ios::binary};
    out.seekp(static_cast<std::streamoff>(offset));
    const std::string junk(size - offset, '\xff');
    out.write(junk.data(), static_cast<std::streamsize>(junk.size()));
  }
  const auto before = FileContents(kStorageDirectory / "branches");

  WarningCapture capture;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true, {.versioning = false})};
    ASSERT_NE(db.version_store(), nullptr);
    EXPECT_FALSE(db.version_store()->Empty());
  }
  EXPECT_EQ(capture.Warnings(), "");
  EXPECT_EQ(FileContents(kStorageDirectory / "branches"), before);
  memgraph::dbms::Database on{MakeConfig(/*recover_on_startup=*/true)};
  EXPECT_TRUE(on.version_store()->Get("b")->unavailable_reason.has_value()) << "versioning on does read it, and sets "
                                                                               "the branch aside";
}

// A crash while the first transaction is written tears the only file, and main skips such a file as well: with one
// file in the directory, none of it readable, nothing can be told from a crash. The instance starts and says the
// writes are lost.
TEST_F(VersioningBranchRecoveryTest, AZeroLengthBranchWalStartsTheInstanceAndWarns) {
  const auto number = CreateWrittenBranch();
  const auto files = WalFiles(number);
  ASSERT_EQ(files.size(), 1U);
  std::filesystem::resize_file(files.front(), 0);

  const auto started = StartedOver();
  ASSERT_TRUE(started.listed);
  EXPECT_FALSE(started.reason.has_value());
  EXPECT_THAT(started.warnings, HasSubstr(BranchWalDir(number).string()));
  EXPECT_THAT(started.warnings, HasSubstr("lost"));
}

// Several files, none readable, are not a crash's work: the branch is set aside instead of coming back without them.
TEST_F(VersioningBranchRecoveryTest, SeveralBranchWalFilesNoneOfThemReadableSetTheBranchAside) {
  const auto branch = CreateTwoFileBranch("b");
  const auto files = WalFiles(branch.number);
  ASSERT_EQ(files.size(), 2U);
  for (const auto &file : files) std::filesystem::resize_file(file, 0);

  const auto started = StartedOver();
  ASSERT_TRUE(started.listed);
  ASSERT_TRUE(started.reason.has_value());
  EXPECT_THAT(*started.reason, HasSubstr("none of the 2 files"));
  EXPECT_THAT(*started.reason, HasSubstr(BranchWalDir(branch.number).string()));
}

// A used branch's directory or log directory missing, or a path of the wrong kind there whether the branch wrote or
// not: the instance starts, main is whole, the branch is unavailable with a reason naming the path, DROP BRANCH removes
// it, and the name is free again.
TEST_F(VersioningBranchRecoveryTest, AMissingOrWrongKindOfBranchDirectoryOrLogSetsTheBranchAside) {
  struct Variant {
    const char *what;
    bool written;
    // The path the damage is done to, relative to the branch directory: "" is the directory itself.
    const char *damaged;
    std::function<void(const std::filesystem::path &branch_dir)> damage;
    const char *reason_part;
  };

  const auto replace_with_file = [](const auto &dir) {
    std::filesystem::remove_all(dir);
    std::ofstream{dir} << "x";
  };
  const std::vector<Variant> variants{
      {"an empty file in place of the log directory",
       true,
       "wal",
       [](const auto &dir) {
         std::filesystem::remove_all(dir / "wal");
         std::ofstream{dir / "wal"};
       },
       "is not a directory"},
      {"a dangling link in place of the log directory",
       true,
       "wal",
       [](const auto &dir) {
         std::filesystem::remove_all(dir / "wal");
         std::filesystem::create_directory_symlink(dir / "nowhere", dir / "wal");
       },
       "link to a path that does not exist"},
      {"a file in place of the branch directory", true, "", replace_with_file, "is not a directory"},
      {"a file in place of the directory of a branch that never wrote",
       false,
       "",
       replace_with_file,
       "is not a directory"},
      {"a missing branch directory", true, "", [](const auto &dir) { std::filesystem::remove_all(dir); }, "is missing"},
      {"a missing log directory",
       true,
       "wal",
       [](const auto &dir) { std::filesystem::remove_all(dir / "wal"); },
       "is missing"},
  };
  for (const auto &variant : variants) {
    SCOPED_TRACE(variant.what);
    std::filesystem::remove_all(kStorageDirectory);
    uint64_t number = 0;
    if (variant.written) {
      number = CreateWrittenBranch();
    } else {
      memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
      const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
      CreateBase(db);
      number = db.version_store()->CreateBranch("b")->number;
    }
    const auto branch_dir = kStorageDirectory / "branches" / std::to_string(number);
    variant.damage(branch_dir);
    const auto damaged = std::string{variant.damaged}.empty() ? branch_dir : branch_dir / variant.damaged;

    const auto started = StartedOver();
    ASSERT_TRUE(started.listed);
    ASSERT_TRUE(started.reason.has_value());
    EXPECT_THAT(*started.reason, HasSubstr(variant.reason_part));
    EXPECT_THAT(*started.reason, HasSubstr(damaged.string()));
    EXPECT_THAT(started.warnings, HasSubstr(variant.reason_part));
    EXPECT_THAT(started.warnings, HasSubstr("DROP BRANCH 'b'"));

    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    EXPECT_EQ(db.Access(ms::READ)->ApproximateVertexCount(), 4U) << "main starts whole";
    ASSERT_EQ(db.version_store()->DropBranch("b"), mv::DropResult::kDropped);
    EXPECT_EQ(std::filesystem::symlink_status(branch_dir).type(), std::filesystem::file_type::not_found);
    EXPECT_TRUE(db.version_store()->CreateBranch("b").has_value()) << "the name is free again";
  }
}

// Without the ledger no branch is restored; the directories left behind say there were some, and none of their numbers
// is handed to a new branch.
TEST_F(VersioningBranchRecoveryTest, ADeletedLedgerWithBranchDirectoriesStartsTheInstanceAndWarns) {
  std::vector<uint64_t> numbers;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto base = CreateBase(db);
    for (const auto *name : {"b", "c"}) {
      const auto info = db.version_store()->CreateBranch(name);
      ASSERT_TRUE(info.has_value());
      numbers.push_back(info->number);
      auto ctx = Checkout(db, name);
      ASSERT_NE(ctx, nullptr);
      Env env{db, *ctx, base};
      env.Txn([&](mv::BranchContext &c, auto &) { [[maybe_unused]] auto vertex = c.CreateNativeVertex(); });
    }
  }
  ASSERT_EQ(numbers.size(), 2U);
  const auto ledger = kStorageDirectory / "branches" / "ledger";
  ASSERT_TRUE(std::filesystem::remove(ledger));

  const auto started = StartedOver();
  EXPECT_FALSE(started.listed);
  EXPECT_THAT(started.warnings, HasSubstr(ledger.string()));
  EXPECT_THAT(started.warnings,
              HasSubstr(fmt::format("[{}, {}]", std::min(numbers[0], numbers[1]), std::max(numbers[0], numbers[1]))));

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const auto fresh = db.version_store()->CreateBranch("d");
  ASSERT_TRUE(fresh.has_value());
  EXPECT_GT(fresh->number, std::max(numbers[0], numbers[1])) << "a leftover directory's number is reused";
}

// Nothing is missing when a branch was never checked out (no directory yet) or was checked out and never written (an
// empty WAL directory), and a database that never had branches has nothing to say.
TEST_F(VersioningBranchRecoveryTest, BranchesThatNeverWroteAndDatabasesWithoutBranchesStartWithoutWarnings) {
  EXPECT_THAT(StartedOver().warnings, ::testing::IsEmpty());
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("idle").has_value());
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    ASSERT_NE(Checkout(db, "b"), nullptr);
  }
  EXPECT_THAT(StartedOver().warnings, ::testing::IsEmpty());
}

namespace {
// The branch's only WAL file, finalized at close, after a transaction that sets a fork vertex's property.
std::filesystem::path WriteOneFinalizedFile() {
  std::filesystem::path file;
  WriteBranch([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.Fork(e.base.v1);
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue(7)).has_value());
    });
  });
  for (const auto &entry : std::filesystem::directory_iterator(kStorageDirectory / "branches")) {
    if (!entry.is_directory()) continue;
    const auto files = WalFiles(std::stoull(entry.path().filename().string()));
    EXPECT_EQ(files.size(), 1u);
    if (files.size() == 1) file = files.front();
  }
  if (!file.empty()) {
    EXPECT_TRUE(memgraph::storage::durability::ReadWalHeader(file).summary.has_value()) << "the file is finalized";
  }
  return file;
}
}  // namespace

// FinalizeWal renames a file only after its summary is synced, so a summary that does not verify in a file named as
// finalized is damage, and skipping the file would drop committed transactions. A file still named as being written
// may have been torn while it was finalized, and is skipped as main skips it.
TEST_F(VersioningBranchRecoveryTest, ADamagedSummarySetsTheBranchAsideOnlyInAFileNamedFinalized) {
  const auto file = WriteOneFinalizedFile();
  ASSERT_FALSE(file.empty());
  constexpr uint64_t kSummaryBytes = 4 * (sizeof(memgraph::storage::durability::Marker) + sizeof(uint64_t));
  const auto offset = memgraph::storage::durability::ReadWalHeader(file).offset_deltas - kSummaryBytes + 1;
  OverwriteByte(file, offset, static_cast<char>(OverwriteByte(file, offset, 0) ^ 0xFF));
  auto started = StartedOver();
  ASSERT_TRUE(started.reason.has_value()) << "a finalized file that fails its summary is not skipped";
  EXPECT_THAT(*started.reason, HasSubstr("summary"));

  const auto name = file.filename().string();
  ASSERT_NE(name.find("_from_"), std::string::npos);
  std::filesystem::rename(file, file.parent_path() / (name.substr(0, name.find("_from_")) + "_current"));
  started = StartedOver();
  EXPECT_FALSE(started.reason.has_value()) << *started.reason;
}

// A copy of a WAL file repeats its sequence number; replaying both would apply its transactions twice.
TEST_F(VersioningBranchRecoveryTest, TwoWalFilesWithOneSequenceNumberSetTheBranchAside) {
  const auto file = WriteOneFinalizedFile();
  ASSERT_FALSE(file.empty());
  std::filesystem::copy_file(file, file.string() + "_copy");
  const auto started = StartedOver();
  ASSERT_TRUE(started.reason.has_value());
  EXPECT_THAT(*started.reason, HasSubstr("sequence numbers 0 and 0"));
}

// Main verifies every transaction's CRC, an uncommitted one's too: a commit flag damaged to "not committed" would
// otherwise skip a committed transaction. The file is finalized, so nothing scans it before the replay.
TEST_F(VersioningBranchRecoveryTest, ACommitFlagThatFailsItsTransactionCrcSetsTheBranchAside) {
  namespace dur = memgraph::storage::durability;
  const auto file = WriteOneFinalizedFile();
  ASSERT_FALSE(file.empty());
  // The first transaction opens the deltas: SECTION_DELTA, its timestamp, DELTA_TRANSACTION_START, TYPE_BOOL, the flag.
  constexpr uint64_t kTimestamp = sizeof(dur::Marker) + sizeof(uint64_t);
  constexpr uint64_t kFlagValue = sizeof(dur::Marker) + kTimestamp + 2 * sizeof(dur::Marker);
  const auto offset = dur::ReadWalHeader(file).offset_deltas + kFlagValue;
  ASSERT_EQ(OverwriteByte(file, offset, static_cast<char>(dur::Marker::VALUE_FALSE)),
            static_cast<char>(dur::Marker::VALUE_TRUE));
  const auto started = StartedOver();
  ASSERT_TRUE(started.reason.has_value());
  EXPECT_THAT(*started.reason, HasSubstr("CRC"));
}
