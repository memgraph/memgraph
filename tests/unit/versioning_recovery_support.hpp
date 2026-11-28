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

// What the branch-recovery tests share: a durable database with a base graph and branches, and a dump of what a session
// reads on a branch together with its bookkeeping.
#pragma once

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
#include "kvstore/kvstore.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "tests/versioning_ledger_kv.hpp"
#include "utils/scheduler.hpp"
#include "utils/uuid.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/branch_recovery.hpp"
#include "versioning/version_store.hpp"

import memgraph.storage.property_value;

namespace memgraph::tests::versioning_recovery {

namespace ms = memgraph::storage;
namespace mv = memgraph::versioning;

inline const std::filesystem::path kStorageDirectory{std::filesystem::temp_directory_path() /
                                                     "versioning_branch_recovery"};
inline const std::filesystem::path kOtherStorageDirectory{std::filesystem::temp_directory_path() /
                                                          "versioning_branch_recovery_other"};
inline const std::filesystem::path kKvDirectory{std::filesystem::temp_directory_path() /
                                                "versioning_branch_recovery_kv"};

// One DBMS durability KVStore holds the branch ledgers; each database has its own uuid, hence its own ledger key.
inline const memgraph::utils::UUID kDbUuid;
inline const memgraph::utils::UUID kOtherDbUuid;

inline memgraph::tests::LedgerKv ledger_kv{kKvDirectory};

struct Options {
  bool properties_on_edges{true};
  bool auto_index{false};
};

inline ms::Config MakeConfig(bool recover_on_startup, Options options = {},
                             const std::filesystem::path &directory = kStorageDirectory) {
  ms::Config config{
      .durability = {.storage_directory = directory,
                     .recover_on_startup = recover_on_startup,
                     .snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                     // Effectively disable periodic snapshotting so recovery is WAL-only.
                     .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)}},
      .experimental_versioning = true,
  };
  config.salient.items.properties_on_edges = options.properties_on_edges;
  config.salient.items.enable_label_index_auto_creation = options.auto_index;
  config.salient.items.enable_edge_type_index_auto_creation = options.auto_index;
  config.salient.uuid = directory == kOtherStorageDirectory ? kOtherDbUuid : kDbUuid;
  return config;
}

// A Database wired to the test's ledger store.
struct TestDatabase : memgraph::dbms::Database {
  explicit TestDatabase(ms::Config config) : memgraph::dbms::Database(std::move(config), ledger_kv.Get()) {}
};

inline auto *Mem(memgraph::dbms::Database &db) { return static_cast<ms::InMemoryStorage *>(db.storage()); }

inline void Commit(ms::Storage::Accessor &acc) {
  ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

inline std::filesystem::path BranchWalDir(uint64_t id, const std::filesystem::path &directory = kStorageDirectory) {
  return directory / "branches" / std::to_string(id) / "wal";
}

inline std::vector<std::filesystem::path> WalFiles(uint64_t id) {
  std::vector<std::filesystem::path> files;
  if (!std::filesystem::exists(BranchWalDir(id))) return files;
  for (const auto &entry : std::filesystem::directory_iterator(BranchWalDir(id))) {
    if (entry.is_regular_file()) files.push_back(entry.path());
  }
  std::ranges::sort(files);
  return files;
}

// v1(:L:M {p:1, q:5}) -e1[:T {w:1}]-> v2(:M);  v3(:L) -e2[:T {w:2}]-> v1;  v4 -e3[:T {w:3}]-> v4.
struct Base {
  ms::Gid v1, v2, v3, v4, e1, e2, e3;
};

inline Base CreateBase(memgraph::dbms::Database &db, bool properties_on_edges = true) {
  auto *st = db.storage();
  auto acc = db.Access(ms::WRITE);
  auto v1 = acc->CreateVertex();
  auto v2 = acc->CreateVertex();
  auto v3 = acc->CreateVertex();
  auto v4 = acc->CreateVertex();
  EXPECT_TRUE(v1.AddLabel(st->NameToLabel("L")).has_value());
  EXPECT_TRUE(v1.AddLabel(st->NameToLabel("M")).has_value());
  EXPECT_TRUE(v1.SetProperty(st->NameToProperty("p"), ms::PropertyValue(1)).has_value());
  EXPECT_TRUE(v1.SetProperty(st->NameToProperty("q"), ms::PropertyValue(5)).has_value());
  EXPECT_TRUE(v2.AddLabel(st->NameToLabel("M")).has_value());
  EXPECT_TRUE(v3.AddLabel(st->NameToLabel("L")).has_value());
  auto e1 = acc->CreateEdge(&v1, &v2, st->NameToEdgeType("T"));
  auto e2 = acc->CreateEdge(&v3, &v1, st->NameToEdgeType("T"));
  auto e3 = acc->CreateEdge(&v4, &v4, st->NameToEdgeType("T"));
  EXPECT_TRUE(e1.has_value() && e2.has_value() && e3.has_value());
  if (properties_on_edges) {
    EXPECT_TRUE(e1->SetProperty(st->NameToProperty("w"), ms::PropertyValue(1)).has_value());
    EXPECT_TRUE(e2->SetProperty(st->NameToProperty("w"), ms::PropertyValue(2)).has_value());
    EXPECT_TRUE(e3->SetProperty(st->NameToProperty("w"), ms::PropertyValue(3)).has_value());
  }
  Base base{v1.Gid(), v2.Gid(), v3.Gid(), v4.Gid(), e1->Gid(), e2->Gid(), e3->Gid()};
  Commit(*acc);
  return base;
}

// What a session reads of the branch, as plain data by names, plus what the branch's bookkeeping says about it.
struct EdgeDump {
  uint64_t gid;
  std::string type;
  uint64_t to;
  std::map<std::string, std::string> props;
};

struct VertexDump {
  uint64_t gid;
  std::set<std::string> labels;
  std::map<std::string, std::string> props;
  std::vector<EdgeDump> out;
};

struct Captured {
  std::vector<VertexDump> vertices;
  std::string bookkeeping;

  const VertexDump *Find(ms::Gid gid) const {
    const auto it = std::ranges::find(vertices, gid.AsUint(), &VertexDump::gid);
    return it == vertices.end() ? nullptr : &*it;
  }

  // The vertex a test expects to exist; a missing one fails the test and yields an empty dump.
  const VertexDump &At(ms::Gid gid) const {
    static const VertexDump kMissing{};
    const auto *found = Find(gid);
    if (found == nullptr) ADD_FAILURE() << "vertex " << gid.AsUint() << " is not in the branch";
    return found != nullptr ? *found : kMissing;
  }

  std::string Render() const {
    std::ostringstream out;
    for (const auto &v : vertices) {
      out << "v" << v.gid << " [";
      for (const auto &label : v.labels) out << label << ",";
      out << "] {";
      for (const auto &[k, value] : v.props) out << k << "=" << value << ";";
      out << "}\n";
      for (const auto &e : v.out) {
        out << "  e" << e.gid << " " << e.type << " -> v" << e.to << " {";
        for (const auto &[k, value] : e.props) out << k << "=" << value << ";";
        out << "}\n";
      }
    }
    out << bookkeeping;
    return out.str();
  }
};

inline std::string Show(const ms::PropertyValue &value) {
  if (value.IsNull()) return "null";
  if (value.IsBool()) return value.ValueBool() ? "true" : "false";
  if (value.IsInt()) return std::to_string(value.ValueInt());
  return "?";
}

inline std::map<std::string, std::string> Named(ms::Storage &st,
                                                const std::map<ms::PropertyId, ms::PropertyValue> &props) {
  std::map<std::string, std::string> named;
  for (const auto &[pid, value] : props) named.emplace(st.PropertyToName(pid), Show(value));
  return named;
}

// Base-delete counts, touched gates of every name the workloads use, and change filters of every base object. Replay
// records only logged changes, so it must leave the same traces as the live run.
inline std::string Bookkeeping(memgraph::dbms::Database &db, mv::BranchCore &core, const Base &base) {
  auto &st = *db.storage();
  std::ostringstream out;
  const auto deletes = core.BaseDeletes();
  out << "deletes v=" << deletes.vertices << " e=" << deletes.edges << "\n";
  for (const auto *name : {"L", "M", "X", "NewL", "N"}) {
    out << "label " << name << ":" << core.MayHaveLabelTouched(st.NameToLabel(name)) << "\n";
  }
  for (const auto *name : {"p", "q", "w", "a"}) {
    out << "prop " << name << ":" << core.MayHavePropertyTouched(st.NameToProperty(name)) << "\n";
  }
  for (const auto gid : {base.v1, base.v2, base.v3, base.v4}) {
    out << "filter v" << gid.AsUint() << " label=" << core.MayHaveLabelChange(gid)
        << " prop=" << core.MayHavePropertyChange(gid)
        << " edge_out=" << core.MayHaveEdgeChange(gid, ms::EdgeDirection::OUT)
        << " edge_in=" << core.MayHaveEdgeChange(gid, ms::EdgeDirection::IN)
        << " deleted=" << core.MayHaveVertexDeleted(gid) << "\n";
  }
  for (const auto gid : {base.e1, base.e2, base.e3}) {
    out << "filter e" << gid.AsUint() << " deleted=" << core.MayHaveBaseEdgeDeleted(gid)
        << " prop=" << core.MayHaveBaseEdgePropertyChange(gid) << "\n";
  }
  return out.str();
}

inline Captured Capture(memgraph::dbms::Database &db, mv::BranchContext &ctx, const Base &base) {
  auto &st = *db.storage();
  Captured captured;
  auto diff_acc = ctx.diff_engine().Access(ms::READ);
  EXPECT_TRUE(ctx.BeginTransaction(diff_acc.get()).has_value());
  for (auto v : ctx.Vertices(ms::View::NEW)) {
    VertexDump d;
    d.gid = v.Gid().AsUint();
    for (const auto label : *ctx.Labels(v, ms::View::NEW)) d.labels.insert(st.LabelToName(label));
    d.props = Named(st, *ctx.Properties(v, ms::View::NEW));
    auto edges = ctx.OutEdges(v, ms::View::NEW);
    EXPECT_TRUE(edges.has_value());
    for (const auto &e : *edges) {
      d.out.push_back({.gid = e.Gid().AsUint(),
                       .type = st.EdgeTypeToName(e.EdgeType()),
                       .to = e.ToVertex().Gid().AsUint(),
                       .props = Named(st, *ctx.Properties(e, ms::View::NEW))});
    }
    std::ranges::sort(d.out, {}, &EdgeDump::gid);
    captured.vertices.push_back(std::move(d));
  }
  ctx.EndTransaction();
  std::ranges::sort(captured.vertices, {}, &VertexDump::gid);
  captured.bookkeeping = Bookkeeping(db, *ctx.core(), base);
  return captured;
}

// Main as a reader sees it: a branch's writes must never reach it.
inline std::string DumpMain(memgraph::dbms::Database &db) {
  auto &st = *db.storage();
  std::ostringstream out;
  auto acc = db.Access(ms::READ);
  for (auto v : acc->Vertices(ms::View::OLD)) {
    out << "v" << v.Gid().AsUint() << " [";
    for (const auto label : *v.Labels(ms::View::OLD)) out << st.LabelToName(label) << ",";
    out << "] {";
    for (const auto &[k, value] : Named(st, *v.Properties(ms::View::OLD))) out << k << "=" << value << ";";
    out << "}\n";
    for (const auto &e : v.OutEdges(ms::View::OLD)->edges) {
      out << "  e" << e.Gid().AsUint() << " -> v" << e.ToVertex().Gid().AsUint() << "\n";
    }
  }
  return out.str();
}

// Attaches to `name`: the registry's core, built on first use from the branch's recorded fork.
inline std::unique_ptr<mv::BranchContext> Checkout(memgraph::dbms::Database &db, std::string_view name,
                                                   bool *built = nullptr) {
  auto branch = db.version_store()->Checkout(name);
  EXPECT_TRUE(branch.has_value());
  if (!branch) return nullptr;
  const auto &info = (*branch)->info();
  const auto fork_ts = (*branch)->fork_ts();
  auto core = (*branch)->GetOrBuildCore([&]() -> std::expected<std::shared_ptr<mv::BranchCore>, std::string> {
    if (built != nullptr) *built = true;
    auto fresh = mv::BranchCore::Build(*Mem(db), info.fork, fork_ts, info.id);
    if (!fresh) return std::unexpected(fresh.error().message);
    return std::move(*fresh);
  });
  EXPECT_TRUE(core.has_value());
  if (!core) return nullptr;
  return std::make_unique<mv::BranchContext>(*core);
}

// Builds and registers `name`'s core from its recorded fork with its WAL replayed, so a later Checkout attaches to it.
// `floor_shift` moves the recorded gid floors, to feed the replay a misread log.
inline std::expected<mv::NativeGidEnds, std::string> Recover(memgraph::dbms::Database &db, std::string_view name,
                                                             int64_t floor_shift = 0) {
  auto branch = db.version_store()->Checkout(name);
  if (!branch) return std::unexpected(branch.error());
  const auto &info = (*branch)->info();
  const auto fork_ts = (*branch)->fork_ts();
  mv::NativeGidEnds ends;
  auto core = (*branch)->GetOrBuildCore([&]() -> std::expected<std::shared_ptr<mv::BranchCore>, std::string> {
    auto fork = info.fork;
    fork.vertex_gid_floor += floor_shift;
    fork.edge_gid_floor += floor_shift;
    auto built = mv::BranchCore::Build(*Mem(db), fork, fork_ts, info.id);
    if (!built) return std::unexpected(built.error().message);
    auto replayed = mv::ReplayBranchWal(*built, BranchWalDir(info.id));
    if (!replayed) return std::unexpected(replayed.error());
    ends = *replayed;
    return std::move(*built);
  });
  if (!core) return std::unexpected(core.error());
  return ends;
}

struct Env {
  memgraph::dbms::Database &db;
  mv::BranchContext &ctx;
  Base base;

  ms::LabelId Label(std::string_view name) { return db.storage()->NameToLabel(name); }

  ms::PropertyId Prop(std::string_view name) { return db.storage()->NameToProperty(name); }

  ms::EdgeTypeId EdgeType(std::string_view name) { return db.storage()->NameToEdgeType(name); }

  ms::VertexAccessor BaseVertex(ms::Gid gid) {
    auto base_vertex = ctx.FindBaseVertex(gid);
    EXPECT_TRUE(base_vertex.has_value());
    return std::move(*base_vertex);
  }

  ms::EdgeAccessor BaseEdge(ms::Gid edge, ms::Gid from) {
    auto base_edge = ctx.FindBaseEdge(edge, from);
    EXPECT_TRUE(base_edge.has_value());
    return std::move(*base_edge);
  }

  ms::VertexAccessor Overlay(ms::Gid gid) {
    auto overlay = ctx.EnsureDiffVertex(gid);
    EXPECT_TRUE(overlay.has_value());
    return std::move(*overlay);
  }

  template <typename Body>
  void Txn(Body &&body) {
    auto diff_acc = ctx.diff_engine().Access(ms::WRITE);
    ASSERT_TRUE(ctx.BeginTransaction(diff_acc.get()).has_value());
    body(ctx, *diff_acc);
    ASSERT_TRUE(ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx.EndTransaction();
  }
};

class RecoveryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ledger_kv.Reset();
    std::filesystem::remove_all(kStorageDirectory);
    std::filesystem::remove_all(kOtherStorageDirectory);
  }

  void TearDown() override {
    ledger_kv.Reset();
    std::filesystem::remove_all(kStorageDirectory);
    std::filesystem::remove_all(kOtherStorageDirectory);
  }
};

struct RoundTrip {
  Captured live;
  Captured replayed;
  Base base;
};

// Runs `workload` on a durable branch, restarts the database, replays the branch WAL, and requires everything the
// session reads and everything the branch's bookkeeping says to be the same as before, and main untouched.
inline RoundTrip RunRoundTrip(const std::function<void(Env &)> &workload, Options options = {}) {
  RoundTrip out;
  std::string main_before;
  {
    TestDatabase db{MakeConfig(/*recover_on_startup=*/false, options)};
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
  TestDatabase db{MakeConfig(/*recover_on_startup=*/true, options)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto recovered = Recover(db, "b");
  EXPECT_TRUE(recovered.has_value()) << recovered.error();
  if (!recovered) return out;
  bool built = false;
  auto ctx = Checkout(db, "b", &built);
  EXPECT_NE(ctx, nullptr);
  EXPECT_FALSE(built) << "recovery registered the core";
  if (ctx == nullptr) return out;
  out.replayed = Capture(db, *ctx, out.base);
  EXPECT_EQ(out.replayed.Render(), out.live.Render());
  EXPECT_EQ(DumpMain(db), main_before) << "branch writes must not reach main";
  return out;
}

}  // namespace memgraph::tests::versioning_recovery
