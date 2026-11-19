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

// A branch comes back after a restart by replaying its diff-engine WAL: a record on a base object is written again
// through the BranchContext write resolver, a native one is the diff engine's stock operation. Each case runs a
// workload, restarts, replays, and compares what a session reads and the branch's bookkeeping.
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
#include "kvstore/kvstore.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/scheduler.hpp"
#include "utils/uuid.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/branch_recovery.hpp"
#include "versioning/version_store.hpp"

import memgraph.storage.property_value;

namespace ms = memgraph::storage;
namespace mv = memgraph::versioning;

namespace {
using ::testing::HasSubstr;

const std::filesystem::path kStorageDirectory{std::filesystem::temp_directory_path() / "versioning_branch_recovery"};
const std::filesystem::path kOtherStorageDirectory{std::filesystem::temp_directory_path() /
                                                   "versioning_branch_recovery_other"};
const std::filesystem::path kKvDirectory{std::filesystem::temp_directory_path() / "versioning_branch_recovery_kv"};

// One DBMS durability KVStore holds the branch ledgers; each database has its own uuid, hence its own ledger key.
const memgraph::utils::UUID kDbUuid;
const memgraph::utils::UUID kOtherDbUuid;

std::unique_ptr<memgraph::kvstore::KVStore> &KvSlot() {
  static std::unique_ptr<memgraph::kvstore::KVStore> kv;
  return kv;
}

memgraph::kvstore::KVStore *Kv() {
  auto &kv = KvSlot();
  if (!kv) kv = std::make_unique<memgraph::kvstore::KVStore>(kKvDirectory);
  return kv.get();
}

void ResetKv() {
  KvSlot().reset();
  std::filesystem::remove_all(kKvDirectory);
}

struct Options {
  bool properties_on_edges{true};
  bool auto_index{false};
};

ms::Config MakeConfig(bool recover_on_startup, Options options = {},
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
  explicit TestDatabase(ms::Config config) : memgraph::dbms::Database(std::move(config), Kv()) {}
};

auto *Mem(memgraph::dbms::Database &db) { return static_cast<ms::InMemoryStorage *>(db.storage()); }

void Commit(ms::Storage::Accessor &acc) {
  ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

std::filesystem::path BranchWalDir(uint64_t number, const std::filesystem::path &directory = kStorageDirectory) {
  return directory / "branches" / std::to_string(number) / "wal";
}

std::vector<std::filesystem::path> WalFiles(uint64_t number) {
  std::vector<std::filesystem::path> files;
  if (!std::filesystem::exists(BranchWalDir(number))) return files;
  for (const auto &entry : std::filesystem::directory_iterator(BranchWalDir(number))) {
    if (entry.is_regular_file()) files.push_back(entry.path());
  }
  std::ranges::sort(files);
  return files;
}

// v1(:L:M {p:1, q:5}) -e1[:T {w:1}]-> v2(:M);  v3(:L) -e2[:T {w:2}]-> v1;  v4 -e3[:T {w:3}]-> v4.
struct Base {
  ms::Gid v1, v2, v3, v4, e1, e2, e3;
};

Base CreateBase(memgraph::dbms::Database &db, bool properties_on_edges = true) {
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

std::string Show(const ms::PropertyValue &value) {
  if (value.IsNull()) return "null";
  if (value.IsBool()) return value.ValueBool() ? "true" : "false";
  if (value.IsInt()) return std::to_string(value.ValueInt());
  return "?";
}

std::map<std::string, std::string> Named(ms::Storage &st, const std::map<ms::PropertyId, ms::PropertyValue> &props) {
  std::map<std::string, std::string> named;
  for (const auto &[pid, value] : props) named.emplace(st.PropertyToName(pid), Show(value));
  return named;
}

// Base-delete counts, touched gates of every name the workloads use, and change filters of every base object. Replay
// records only logged changes, so it must leave the same traces as the live run.
std::string Bookkeeping(memgraph::dbms::Database &db, mv::BranchCore &core, const Base &base) {
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
        << " prop=" << core.MayHavePropertyChange(gid) << " edge=" << core.MayHaveEdgeChange(gid)
        << " deleted=" << core.MayHaveVertexDeleted(gid) << "\n";
  }
  for (const auto gid : {base.e1, base.e2, base.e3}) {
    out << "filter e" << gid.AsUint() << " deleted=" << core.MayHaveBaseEdgeDeleted(gid)
        << " prop=" << core.MayHaveBaseEdgePropertyChange(gid) << "\n";
  }
  return out.str();
}

Captured Capture(memgraph::dbms::Database &db, mv::BranchContext &ctx, const Base &base) {
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
std::string DumpMain(memgraph::dbms::Database &db) {
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
std::unique_ptr<mv::BranchContext> Checkout(memgraph::dbms::Database &db, std::string_view name,
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
std::expected<mv::NativeGidEnds, std::string> Recover(memgraph::dbms::Database &db, std::string_view name,
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

class VersioningBranchRecoveryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ResetKv();
    std::filesystem::remove_all(kStorageDirectory);
    std::filesystem::remove_all(kOtherStorageDirectory);
  }

  void TearDown() override {
    ResetKv();
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
RoundTrip RunRoundTrip(const std::function<void(Env &)> &workload, Options options = {}) {
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

std::string Joined(const std::set<std::string> &names) {
  std::string joined;
  for (const auto &name : names) joined += name + ",";
  return joined;
}
}  // namespace

// ---- Properties of base vertices

TEST_F(VersioningBranchRecoveryTest, RemovingABasePropertyReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.BaseVertex(e.base.v1);
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue()).has_value());
    });
  });
  ASSERT_NE(r.replayed.Find(r.base.v1), nullptr);
  EXPECT_EQ(r.replayed.At(r.base.v1).props.size(), 1u);
  EXPECT_EQ(r.replayed.At(r.base.v1).props.at("q"), "5");
}

TEST_F(VersioningBranchRecoveryTest, RemovingThenSettingABasePropertyInOneTransactionReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.BaseVertex(e.base.v1);
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue()).has_value());
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue(7)).has_value());
    });
  });
  EXPECT_EQ(r.replayed.At(r.base.v1).props.at("p"), "7");
}

TEST_F(VersioningBranchRecoveryTest, SettingThenRemovingABasePropertyReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.BaseVertex(e.base.v1);
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue(5)).has_value());
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue()).has_value());
    });
  });
  EXPECT_EQ(r.replayed.At(r.base.v1).props.count("p"), 0u);
  EXPECT_EQ(r.replayed.At(r.base.v1).props.at("q"), "5");
}

TEST_F(VersioningBranchRecoveryTest, SettingThenRemovingAPropertyTheBaseLacksReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v3 = e.BaseVertex(e.base.v3);
      ASSERT_TRUE(c.SetProperty(v3, e.Prop("a"), ms::PropertyValue(1)).has_value());
      ASSERT_TRUE(c.SetProperty(v3, e.Prop("a"), ms::PropertyValue()).has_value());
    });
  });
  EXPECT_TRUE(r.replayed.At(r.base.v3).props.empty());
}

TEST_F(VersioningBranchRecoveryTest, APropertyRemovedInOneTransactionAndSetInANextReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.BaseVertex(e.base.v1);
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue()).has_value());
    });
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.BaseVertex(e.base.v1);
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue(9)).has_value());
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("q"), ms::PropertyValue(6)).has_value());
    });
  });
  EXPECT_EQ(r.replayed.At(r.base.v1).props.at("p"), "9");
  EXPECT_EQ(r.replayed.At(r.base.v1).props.at("q"), "6");
}

// ---- Labels of base vertices

TEST_F(VersioningBranchRecoveryTest, RemovingAndReAddingABaseLabelInOneTransactionReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.BaseVertex(e.base.v1);
      ASSERT_TRUE(*c.RemoveLabel(v1, e.Label("L")));
      ASSERT_TRUE(*c.AddLabel(v1, e.Label("L")));
    });
  });
  EXPECT_EQ(Joined(r.replayed.At(r.base.v1).labels), "L,M,");
}

TEST_F(VersioningBranchRecoveryTest, ReAddingALabelRemovedByAnEarlierTransactionAndAddingAnotherReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.BaseVertex(e.base.v1);
      ASSERT_TRUE(*c.RemoveLabel(v1, e.Label("L")));
    });
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.BaseVertex(e.base.v1);
      ASSERT_TRUE(*c.AddLabel(v1, e.Label("L")));
      ASSERT_TRUE(*c.AddLabel(v1, e.Label("X")));
    });
  });
  EXPECT_EQ(Joined(r.replayed.At(r.base.v1).labels), "L,M,X,");
}

TEST_F(VersioningBranchRecoveryTest, AddingThenRemovingALabelTheBaseLacksReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.BaseVertex(e.base.v1);
      ASSERT_TRUE(*c.AddLabel(v1, e.Label("NewL")));
      ASSERT_TRUE(*c.RemoveLabel(v1, e.Label("NewL")));
    });
  });
  EXPECT_EQ(Joined(r.replayed.At(r.base.v1).labels), "L,M,");
}

// ---- Deletes

TEST_F(VersioningBranchRecoveryTest, DeletingABaseVertexAfterWritesReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v3 = e.BaseVertex(e.base.v3);
      ASSERT_TRUE(*c.AddLabel(v3, e.Label("X")));
      ASSERT_TRUE(c.SetProperty(v3, e.Prop("a"), ms::PropertyValue(2)).has_value());
      ASSERT_TRUE(*c.DeleteBaseVertex(e.base.v3, true));
    });
  });
  EXPECT_EQ(r.replayed.Find(r.base.v3), nullptr);
  EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("deletes v=1 e=1"));
}

TEST_F(VersioningBranchRecoveryTest, DetachDeletingABaseVertexReplaysItsHiddenEdgesAndCounts) {
  auto const r = RunRoundTrip(
      [](Env &e) { e.Txn([&](mv::BranchContext &c, auto &) { ASSERT_TRUE(*c.DeleteBaseVertex(e.base.v1, true)); }); });
  EXPECT_EQ(r.replayed.Find(r.base.v1), nullptr);
  EXPECT_TRUE(r.replayed.At(r.base.v3).out.empty()) << "e2 is hidden with its endpoint";
  EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("deletes v=1 e=2"));
}

TEST_F(VersioningBranchRecoveryTest, DeletingABaseEdgeReplays) {
  for (const bool properties_on_edges : {true, false}) {
    SCOPED_TRACE(properties_on_edges ? "properties on edges" : "no edge properties");
    ResetKv();
    std::filesystem::remove_all(kStorageDirectory);
    auto const r = RunRoundTrip(
        [](Env &e) {
          e.Txn([&](mv::BranchContext &c, auto &) {
            auto edge = e.BaseEdge(e.base.e1, e.base.v1);
            ASSERT_TRUE(*c.DeleteBaseEdge(edge));
          });
        },
        {.properties_on_edges = properties_on_edges});
    EXPECT_TRUE(r.replayed.At(r.base.v1).out.empty());
    EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("deletes v=0 e=1"));
    EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("filter e" + std::to_string(r.base.e1.AsUint()) + " deleted=1"));
  }
}

// The WAL puts a base vertex's delete before the records of its transaction's edges, so each of these orders needs
// the delete applied last.
TEST_F(VersioningBranchRecoveryTest, ABaseEdgeDeletedThenItsEndpointInOneTransactionReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto edge = e.BaseEdge(e.base.e1, e.base.v1);
      ASSERT_TRUE(*c.DeleteBaseEdge(edge));
      ASSERT_TRUE(*c.DeleteBaseVertex(e.base.v1, true));
    });
  });
  EXPECT_EQ(r.replayed.Find(r.base.v1), nullptr);
  EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("deletes v=1 e=2")) << "e1 counted once, e2 with the vertex";
  EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("filter e" + std::to_string(r.base.e1.AsUint()) + " deleted=1"))
      << "the edge's own marker must exist, not only the vertex's";
}

TEST_F(VersioningBranchRecoveryTest, ABaseEdgeDeletedThenItsEndpointInALaterTransactionReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto edge = e.BaseEdge(e.base.e1, e.base.v1);
      ASSERT_TRUE(*c.DeleteBaseEdge(edge));
    });
    e.Txn([&](mv::BranchContext &c, auto &) { ASSERT_TRUE(*c.DeleteBaseVertex(e.base.v1, true)); });
  });
  EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("deletes v=1 e=2"));
}

TEST_F(VersioningBranchRecoveryTest, ANativeEdgeCreatedThenDetachDeletedWithItsBaseEndpointReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &diff) {
      auto overlay = e.Overlay(e.base.v2);
      auto native = c.CreateNativeVertex();
      auto edge = c.CreateNativeEdge(overlay, native, e.EdgeType("N"));
      ASSERT_TRUE(edge.has_value());
      ASSERT_TRUE(diff.DeleteEdge(&*edge).has_value());
      ASSERT_TRUE(*c.DeleteBaseVertex(e.base.v2, true));
    });
  });
  EXPECT_EQ(r.replayed.Find(r.base.v2), nullptr);
}

TEST_F(VersioningBranchRecoveryTest, DetachDeletingABaseVertexWithACommittedNativeEdgeReplays) {
  uint64_t native_edge = 0;
  auto const r = RunRoundTrip([&](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto overlay = e.Overlay(e.base.v2);
      auto native = c.CreateNativeVertex();
      auto edge = c.CreateNativeEdge(overlay, native, e.EdgeType("N"));
      ASSERT_TRUE(edge.has_value());
      native_edge = edge->Gid().AsUint();
    });
    e.Txn([&](mv::BranchContext &c, auto &diff) {
      auto edge = c.FindDiffEdge(ms::Gid::FromUint(native_edge), e.base.v2, ms::View::NEW);
      ASSERT_TRUE(edge.has_value());
      ASSERT_TRUE(diff.DeleteEdge(&*edge).has_value());
      ASSERT_TRUE(*c.DeleteBaseVertex(e.base.v2, true));
    });
  });
  EXPECT_EQ(r.replayed.Find(r.base.v2), nullptr);
  EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("deletes v=1 e=1")) << "e1 goes with v2";
}

TEST_F(VersioningBranchRecoveryTest, DetachDeletingABaseVertexWithNativeEdgesToNativeAndBaseVerticesReplays) {
  uint64_t to_native = 0, to_base = 0;
  auto const r = RunRoundTrip([&](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto a = e.Overlay(e.base.v2);
      auto b = e.Overlay(e.base.v3);
      auto native = c.CreateNativeVertex();
      auto first = c.CreateNativeEdge(a, native, e.EdgeType("N"));
      auto second = c.CreateNativeEdge(b, a, e.EdgeType("N"));
      ASSERT_TRUE(first.has_value() && second.has_value());
      to_native = first->Gid().AsUint();
      to_base = second->Gid().AsUint();
    });
    e.Txn([&](mv::BranchContext &c, auto &diff) {
      auto first = c.FindDiffEdge(ms::Gid::FromUint(to_native), e.base.v2, ms::View::NEW);
      auto second = c.FindDiffEdge(ms::Gid::FromUint(to_base), e.base.v3, ms::View::NEW);
      ASSERT_TRUE(first.has_value() && second.has_value());
      ASSERT_TRUE(diff.DeleteEdge(&*first).has_value());
      ASSERT_TRUE(diff.DeleteEdge(&*second).has_value());
      ASSERT_TRUE(*c.DeleteBaseVertex(e.base.v2, true));
    });
  });
  EXPECT_EQ(r.replayed.Find(r.base.v2), nullptr);
  ASSERT_NE(r.replayed.Find(r.base.v3), nullptr);
  for (auto const &edge : r.replayed.At(r.base.v3).out) EXPECT_NE(edge.gid, to_base);
}

// ---- Edges

TEST_F(VersioningBranchRecoveryTest, BaseEdgePropertyWritesReplay) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto e1 = e.BaseEdge(e.base.e1, e.base.v1);
      ASSERT_TRUE(c.SetProperty(e1, e.Prop("w"), ms::PropertyValue(5)).has_value());
      auto e2 = e.BaseEdge(e.base.e2, e.base.v3);
      ASSERT_TRUE(c.SetProperty(e2, e.Prop("a"), ms::PropertyValue(3)).has_value());
    });
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto e2 = e.BaseEdge(e.base.e2, e.base.v3);
      ASSERT_TRUE(c.SetProperty(e2, e.Prop("w"), ms::PropertyValue()).has_value());
    });
  });
  const auto &e1 = r.replayed.At(r.base.v1).out.at(0);
  EXPECT_EQ(e1.props.at("w"), "5");
  const auto &e2 = r.replayed.At(r.base.v3).out.at(0);
  EXPECT_EQ(e2.props.count("w"), 0u);
  EXPECT_EQ(e2.props.at("a"), "3");
}

// ---- Self-loops

TEST_F(VersioningBranchRecoveryTest, DeletingABaseSelfLoopReplays) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto loop = e.BaseEdge(e.base.e3, e.base.v4);
      ASSERT_TRUE(c.SetProperty(loop, e.Prop("w"), ms::PropertyValue(8)).has_value());
      ASSERT_TRUE(*c.DeleteBaseEdge(loop));
    });
  });
  EXPECT_TRUE(r.replayed.At(r.base.v4).out.empty());
  EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("deletes v=0 e=1"));
}

TEST_F(VersioningBranchRecoveryTest, DetachDeletingABaseVertexWithASelfLoopReplays) {
  auto const r = RunRoundTrip(
      [](Env &e) { e.Txn([&](mv::BranchContext &c, auto &) { ASSERT_TRUE(*c.DeleteBaseVertex(e.base.v4, true)); }); });
  EXPECT_EQ(r.replayed.Find(r.base.v4), nullptr);
  EXPECT_THAT(r.replayed.bookkeeping, HasSubstr("deletes v=1 e=1"));
}

// A removed base property stays removed, not the base's value again, after native writes around it.
TEST_F(VersioningBranchRecoveryTest, ARemovedBasePropertyStaysRemovedAcrossLaterRestartWrites) {
  auto const r = RunRoundTrip([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.BaseVertex(e.base.v1);
      ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue()).has_value());
      auto e1 = e.BaseEdge(e.base.e1, e.base.v1);
      ASSERT_TRUE(c.SetProperty(e1, e.Prop("w"), ms::PropertyValue()).has_value());
    });
  });
  EXPECT_EQ(r.replayed.At(r.base.v1).props.count("p"), 0u);
  EXPECT_EQ(r.replayed.At(r.base.v1).out.at(0).props.count("w"), 0u);
}

// ---- Native objects

namespace {
struct NativeWorkload {
  ms::Gid n1;
  ms::Gid n2;
};

// Every native record kind across two transactions, with base-object writes around them.
NativeWorkload RunNativeWorkload(Env &e) {
  NativeWorkload out{};
  ms::Gid n3;
  ms::Gid n1_to_n2;
  ms::Gid between_overlays;

  e.Txn([&](mv::BranchContext &c, auto &) {
    auto n1 = c.CreateNativeVertex();
    auto n2 = c.CreateNativeVertex();
    n3 = c.CreateNativeVertex().Gid();
    out = {n1.Gid(), n2.Gid()};
    ASSERT_TRUE(*c.AddLabel(n1, e.Label("N")));
    ASSERT_TRUE(*c.AddLabel(n1, e.Label("X")));
    ASSERT_TRUE(c.SetProperty(n1, e.Prop("a"), ms::PropertyValue(1)).has_value());

    auto s4 = e.Overlay(e.base.v4);
    auto s3 = e.Overlay(e.base.v3);
    auto native = c.CreateNativeEdge(n1, n2, e.EdgeType("T"));
    ASSERT_TRUE(native.has_value());
    n1_to_n2 = native->Gid();
    auto from_overlay = c.CreateNativeEdge(s4, n1, e.EdgeType("T"));
    ASSERT_TRUE(from_overlay.has_value());
    ASSERT_TRUE(c.SetProperty(*from_overlay, e.Prop("w"), ms::PropertyValue(33)).has_value());
    ASSERT_TRUE(c.CreateNativeEdge(n2, s3, e.EdgeType("T")).has_value());
    auto overlays = c.CreateNativeEdge(s4, s3, e.EdgeType("T"));
    ASSERT_TRUE(overlays.has_value());
    between_overlays = overlays->Gid();
  });

  e.Txn([&](mv::BranchContext &c, auto &diff) {
    auto edge = c.FindDiffEdge(n1_to_n2, ms::View::NEW);
    ASSERT_TRUE(edge.has_value());
    ASSERT_TRUE(c.SetProperty(*edge, e.Prop("w"), ms::PropertyValue(7)).has_value());
    auto n1 = diff.FindVertex(out.n1, ms::View::NEW);
    ASSERT_TRUE(n1.has_value());
    ASSERT_TRUE(*c.RemoveLabel(*n1, e.Label("X")));
    auto overlays = c.FindDiffEdge(between_overlays, e.base.v4, ms::View::NEW);
    ASSERT_TRUE(overlays.has_value());
    ASSERT_TRUE(diff.DeleteEdge(&*overlays).has_value());
    auto vertex = diff.FindVertex(n3, ms::View::NEW);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(diff.DeleteVertex(&*vertex).has_value());

    auto v1 = e.BaseVertex(e.base.v1);
    ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue(9)).has_value());
    ASSERT_TRUE(*c.RemoveLabel(v1, e.Label("L")));
  });
  return out;
}

// Changes a replayed native vertex without allocating a gid, and a base vertex.
void WriteAfterRestart(Env &e, ms::Gid native, int64_t value) {
  e.Txn([&](mv::BranchContext &c, auto &diff) {
    auto vertex = diff.FindVertex(native, ms::View::NEW);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(c.SetProperty(*vertex, e.Prop("a"), ms::PropertyValue(value)).has_value());
    auto v2 = e.BaseVertex(e.base.v2);
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
    TestDatabase db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b");
    ASSERT_TRUE(info.has_value());
    number = info->id;
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
    TestDatabase db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    ASSERT_TRUE(Recover(db, "b").has_value());
    bool built = false;
    auto ctx = Checkout(db, "b", &built);
    ASSERT_NE(ctx, nullptr);
    EXPECT_FALSE(built);
    const auto replayed = Capture(db, *ctx, base);
    EXPECT_EQ(replayed.Render(), after_first.Render());
    EXPECT_EQ(replayed.vertices.size(), 6u) << "4 base vertices, 3 native, one of them deleted";
    EXPECT_EQ(Joined(replayed.At(workload.n1).labels), "N,");
    EXPECT_EQ(Joined(replayed.At(base.v1).labels), "M,");
    EXPECT_EQ(replayed.At(base.v1).props.at("p"), "9");

    Env env{db, *ctx, base};
    WriteAfterRestart(env, workload.n2, 5);
    after_second = Capture(db, *ctx, base);
    ASSERT_NE(after_second.Render(), after_first.Render());
  }
  EXPECT_GT(WalFiles(number).size(), first_run_files) << "the second run continues the WAL in a new file";

  TestDatabase db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_TRUE(Recover(db, "b").has_value());
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  EXPECT_EQ(Capture(db, *ctx, base).Render(), after_second.Render());
}

TEST_F(VersioningBranchRecoveryTest, TwoBranchesRecoverIndependently) {
  Base base{};
  Captured b_before;
  Captured c_before;
  {
    TestDatabase db{MakeConfig(/*recover_on_startup=*/false)};
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
      auto overlay = env_c.Overlay(base.v2);
      ASSERT_TRUE(ctx.CreateNativeEdge(overlay, native, env_c.EdgeType("T")).has_value());
      auto v1 = env_c.BaseVertex(base.v1);
      ASSERT_TRUE(ctx.SetProperty(v1, env_c.Prop("p"), ms::PropertyValue(2)).has_value());
    });
    b_before = Capture(db, *b, base);
    c_before = Capture(db, *c, base);
  }
  ASSERT_NE(b_before.Render(), c_before.Render());

  TestDatabase db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_EQ(db.version_store()->List().size(), 2u);
  ASSERT_TRUE(Recover(db, "b").has_value());
  ASSERT_TRUE(Recover(db, "c").has_value());
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
          auto v1 = e.BaseVertex(e.base.v1);
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
    TestDatabase db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto acc = db.Access(ms::WRITE);
    auto v1 = acc->FindVertex(base.v1, ms::View::OLD);
    ASSERT_TRUE(v1.has_value());
    ASSERT_TRUE(v1->SetProperty(db.storage()->NameToProperty("p"), ms::PropertyValue(50)).has_value());
    Commit(*acc);
  }

  TestDatabase db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  bool built = false;
  auto ctx = Checkout(db, "b", &built);
  ASSERT_NE(ctx, nullptr);
  EXPECT_TRUE(built);
  const auto captured = Capture(db, *ctx, base);
  ASSERT_NE(captured.Find(base.v1), nullptr);
  EXPECT_EQ(captured.Find(base.v1)->props.at("p"), "1") << "the branch still reads main at its fork";
}

// A build with a branch number owns that directory's files: nothing may move them aside into main's backup directory.
TEST_F(VersioningBranchRecoveryTest, ABuildOverAnExistingBranchDirectoryKeepsItsFiles) {
  TestDatabase db{MakeConfig(/*recover_on_startup=*/false)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  CreateBase(db);
  const auto info = db.version_store()->CreateBranch("b");
  ASSERT_TRUE(info.has_value());
  const auto stray = BranchWalDir(info->id) / "stray";
  std::filesystem::create_directories(stray.parent_path());
  std::ofstream{stray} << "kept";

  auto built = mv::BranchCore::Build(*Mem(db), info->fork, db.version_store()->Get("b")->fork_ts, info->id);
  ASSERT_TRUE(built.has_value());
  EXPECT_TRUE(std::filesystem::exists(stray));
  EXPECT_FALSE(std::filesystem::exists(kStorageDirectory / "branches" / std::to_string(info->id) / ".backup"));
}

// ---- Refusals

TEST_F(VersioningBranchRecoveryTest, AMissingWalFileFailsTheReplayAndKeepsTheOtherFiles) {
  uint64_t number = 0;
  Base base{};
  NativeWorkload workload{};
  {
    TestDatabase db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b");
    ASSERT_TRUE(info.has_value());
    number = info->id;
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    workload = RunNativeWorkload(env);
  }
  {
    TestDatabase db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    ASSERT_TRUE(Recover(db, "b").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    WriteAfterRestart(env, workload.n1, 2);
  }

  auto files = WalFiles(number);
  ASSERT_EQ(files.size(), 2u);
  const auto first = std::ranges::find_if(
      files, [](const auto &path) { return memgraph::storage::durability::ReadWalInfo(path).seq_num == 0; });
  ASSERT_NE(first, files.end());
  std::filesystem::remove(*first);
  files.erase(first);

  TestDatabase db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto recovered = Recover(db, "b");
  ASSERT_FALSE(recovered.has_value());
  EXPECT_THAT(recovered.error(), HasSubstr("missing prefix WAL files"));
  EXPECT_EQ(WalFiles(number), files) << "a failed replay leaves the branch's files as they are";
}

// A branch is only replayed over the main history it forked from: a base vertex that history does not have is a
// refusal, never a skipped record.
TEST_F(VersioningBranchRecoveryTest, ABranchWhoseMainHistoryLacksABaseObjectIsRefused) {
  uint64_t number = 0;
  mv::ForkPoint fork_of_b;
  {
    TestDatabase db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b");
    ASSERT_TRUE(info.has_value());
    number = info->id;
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto v4 = env.BaseVertex(base.v4);
      ASSERT_TRUE(*c.AddLabel(v4, env.Label("X")));
    });
    fork_of_b = ctx->core()->fork();
  }

  // Another main whose history has only three vertices: the branch's base vertex (gid 3) is not in it.
  TestDatabase other{MakeConfig(/*recover_on_startup=*/false, {}, kOtherStorageDirectory)};
  const memgraph::memory::DbArenaScope arena_scope{&other.Arena()};
  {
    auto acc = other.Access(ms::WRITE);
    for (int i = 0; i < 3; ++i) acc->CreateVertex();
    Commit(*acc);
  }
  auto info = other.version_store()->CreateBranch("o");
  ASSERT_TRUE(info.has_value());
  auto core = mv::BranchCore::Build(*Mem(other), fork_of_b, other.version_store()->Get("o")->fork_ts, info->id);
  ASSERT_TRUE(core.has_value());
  const auto replayed = mv::ReplayBranchWal(*core, BranchWalDir(number));
  ASSERT_FALSE(replayed.has_value());
  EXPECT_THAT(replayed.error(), HasSubstr("is not in the branch"));
}

namespace {
// Runs `workload` on a fresh durable branch "b" and closes the database, leaving the branch's WAL behind.
void WriteBranch(const std::function<void(Env &)> &workload) {
  TestDatabase db{MakeConfig(/*recover_on_startup=*/false)};
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

// Records are routed by the gid floors; floors that disagree with the log make a create record land on the base side.
TEST_F(VersioningBranchRecoveryTest, ACreateRecordBelowTheRecordedFloorIsRefused) {
  const std::vector<RefusalCase> cases{
      {"creates vertex",
       [](Env &e) {
         e.Txn([&](mv::BranchContext &c, auto &) {
           auto n1 = c.CreateNativeVertex();
           auto n2 = c.CreateNativeVertex();
           ASSERT_TRUE(c.CreateNativeEdge(n1, n2, e.EdgeType("T")).has_value());
         });
       }},
      // An edge between two base vertices' overlays: the log's only create record is the edge's.
      {"creates edge",
       [](Env &e) {
         e.Txn([&](mv::BranchContext &c, auto &) {
           auto s4 = e.Overlay(e.base.v4);
           auto s3 = e.Overlay(e.base.v3);
           ASSERT_TRUE(c.CreateNativeEdge(s4, s3, e.EdgeType("T")).has_value());
         });
       }},
  };
  for (const auto &[expected, workload] : cases) {
    SCOPED_TRACE(expected);
    ResetKv();
    std::filesystem::remove_all(kStorageDirectory);
    WriteBranch(workload);
    TestDatabase db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto recovered = Recover(db, "b", /*floor_shift=*/1000);
    ASSERT_FALSE(recovered.has_value());
    EXPECT_THAT(recovered.error(), HasSubstr(std::string{expected} + " "));
    EXPECT_THAT(recovered.error(), HasSubstr("a gid of the base state"));
  }
}

// A record the branch already holds does not apply again: replaying a log over its own result is refused.
TEST_F(VersioningBranchRecoveryTest, ReplayingALogOverItsOwnResultIsRefused) {
  const std::vector<RefusalCase> cases{
      {"Base edge",
       [](Env &e) {
         e.Txn([&](mv::BranchContext &c, auto &) {
           auto edge = e.BaseEdge(e.base.e1, e.base.v1);
           ASSERT_TRUE(*c.DeleteBaseEdge(edge));
         });
       }},
      // The log's vertex delete follows the delete of its self-loop, which is what the second replay refuses first.
      {"Base edge 2 is already deleted",
       [](Env &e) { e.Txn([&](mv::BranchContext &c, auto &) { ASSERT_TRUE(*c.DeleteBaseVertex(e.base.v4, true)); }); }},
      {"already exists in the branch",
       [](Env &e) { e.Txn([&](mv::BranchContext &c, auto &) { c.CreateNativeVertex(); }); }},
      // The log's only create record is the edge's, between two base vertices' overlays.
      {"Edge",
       [](Env &e) {
         e.Txn([&](mv::BranchContext &c, auto &) {
           auto s4 = e.Overlay(e.base.v4);
           auto s3 = e.Overlay(e.base.v3);
           ASSERT_TRUE(c.CreateNativeEdge(s4, s3, e.EdgeType("T")).has_value());
         });
       }},
  };
  for (const auto &[expected, workload] : cases) {
    SCOPED_TRACE(expected);
    ResetKv();
    std::filesystem::remove_all(kStorageDirectory);
    WriteBranch(workload);
    TestDatabase db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto listing = db.version_store()->Get("b");
    ASSERT_TRUE(listing.has_value());
    const auto number = listing->info.id;
    auto core = mv::BranchCore::Build(*Mem(db), listing->info.fork, listing->fork_ts, number);
    ASSERT_TRUE(core.has_value());
    ASSERT_TRUE(mv::ReplayBranchWal(*core, BranchWalDir(number)).has_value());
    const auto again = mv::ReplayBranchWal(*core, BranchWalDir(number));
    ASSERT_FALSE(again.has_value());
    EXPECT_THAT(again.error(), HasSubstr(expected));
  }
}

namespace {
// The branch's only WAL file, finalized at close, after a transaction that sets a base vertex's property.
std::filesystem::path WriteOneFinalizedFile() {
  std::filesystem::path file;
  WriteBranch([](Env &e) {
    e.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = e.BaseVertex(e.base.v1);
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
TEST_F(VersioningBranchRecoveryTest, ADamagedSummaryFailsTheReplayOfAFileNamedFinalized) {
  const auto file = WriteOneFinalizedFile();
  ASSERT_FALSE(file.empty());
  constexpr uint64_t kSummaryBytes = 4 * (sizeof(memgraph::storage::durability::Marker) + sizeof(uint64_t));
  const auto offset = memgraph::storage::durability::ReadWalHeader(file).offset_deltas - kSummaryBytes + 1;
  OverwriteByte(file, offset, static_cast<char>(OverwriteByte(file, offset, 0) ^ 0xFF));
  {
    TestDatabase db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto recovered = Recover(db, "b");
    ASSERT_FALSE(recovered.has_value());
    EXPECT_THAT(recovered.error(), HasSubstr("summary"));
  }

  const auto name = file.filename().string();
  ASSERT_NE(name.find("_from_"), std::string::npos);
  std::filesystem::rename(file, file.parent_path() / (name.substr(0, name.find("_from_")) + "_current"));
  TestDatabase db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto recovered = Recover(db, "b");
  EXPECT_TRUE(recovered.has_value()) << recovered.error();
}

// Any read failure of a file named as finalized is damage, not just a summary one: the file always held a transaction,
// so skipping it would silently drop committed work. A file still named as being written keeps main's rule.
TEST_F(VersioningBranchRecoveryTest, AFinalizedFileThatCannotBeReadFailsTheReplay) {
  const std::vector<std::pair<std::string, std::function<void(const std::filesystem::path &)>>> damages{
      {"magic", [](const auto &file) { OverwriteByte(file, 0, static_cast<char>(OverwriteByte(file, 0, 0) ^ 0xFF)); }},
      {"zero length", [](const auto &file) { std::filesystem::resize_file(file, 0); }},
      {"truncated deltas",
       [](const auto &file) { std::filesystem::resize_file(file, std::filesystem::file_size(file) - 4); }},
  };
  for (const auto &[what, damage] : damages) {
    SCOPED_TRACE(what);
    ResetKv();
    std::filesystem::remove_all(kStorageDirectory);
    const auto file = WriteOneFinalizedFile();
    ASSERT_FALSE(file.empty());
    damage(file);
    TestDatabase db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    EXPECT_FALSE(Recover(db, "b").has_value());
  }
}

TEST_F(VersioningBranchRecoveryTest, AnUnreadableFileStillNamedAsBeingWrittenIsSkipped) {
  const auto file = WriteOneFinalizedFile();
  ASSERT_FALSE(file.empty());
  const auto name = file.filename().string();
  const auto current = file.parent_path() / (name.substr(0, name.find("_from_")) + "_current");
  std::filesystem::rename(file, current);
  std::filesystem::resize_file(current, 0);
  TestDatabase db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto recovered = Recover(db, "b");
  EXPECT_TRUE(recovered.has_value()) << recovered.error();
}

// A copy of a WAL file repeats its sequence number; replaying both would apply its transactions twice.
TEST_F(VersioningBranchRecoveryTest, TwoWalFilesWithOneSequenceNumberFailTheReplay) {
  const auto file = WriteOneFinalizedFile();
  ASSERT_FALSE(file.empty());
  {
    std::ifstream in{file, std::ios::binary};
    std::ofstream{file.string() + "_copy", std::ios::binary} << in.rdbuf();
  }
  TestDatabase db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto recovered = Recover(db, "b");
  ASSERT_FALSE(recovered.has_value());
  EXPECT_THAT(recovered.error(), HasSubstr("sequence numbers 0 and 0"));
}

// Main verifies every transaction's CRC, an uncommitted one's too: a commit flag damaged to "not committed" would
// otherwise skip a committed transaction. The file is finalized, so nothing scans it before the replay.
TEST_F(VersioningBranchRecoveryTest, ACommitFlagThatFailsItsTransactionCrcFailsTheReplay) {
  namespace dur = memgraph::storage::durability;
  const auto file = WriteOneFinalizedFile();
  ASSERT_FALSE(file.empty());
  // The first transaction opens the deltas: SECTION_DELTA, its timestamp, DELTA_TRANSACTION_START, TYPE_BOOL, the flag.
  constexpr uint64_t kTimestamp = sizeof(dur::Marker) + sizeof(uint64_t);
  constexpr uint64_t kFlagValue = sizeof(dur::Marker) + kTimestamp + 2 * sizeof(dur::Marker);
  const auto offset = dur::ReadWalHeader(file).offset_deltas + kFlagValue;
  ASSERT_EQ(OverwriteByte(file, offset, static_cast<char>(dur::Marker::VALUE_FALSE)),
            static_cast<char>(dur::Marker::VALUE_TRUE));
  TestDatabase db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto recovered = Recover(db, "b");
  ASSERT_FALSE(recovered.has_value());
  EXPECT_THAT(recovered.error(), HasSubstr("CRC"));
}
