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

// The branch's diff-engine WAL logs a fork stub as the user operation its markers stand for (REMOVE_LABEL,
// DELETE_VERTEX, the property's current value), never as a marker. The WAL files are decoded and checked here.
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "dbms/database.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/scheduler.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/version_store.hpp"

import memgraph.storage.property_value;

namespace ms = memgraph::storage;
namespace dur = memgraph::storage::durability;
namespace mv = memgraph::versioning;

namespace {
using ::testing::Contains;
using ::testing::Each;
using ::testing::ElementsAre;
using ::testing::UnorderedElementsAre;
using ::testing::UnorderedElementsAreArray;

const std::filesystem::path kStorageDirectory{std::filesystem::temp_directory_path() / "versioning_branch_wal"};

struct Options {
  bool wal{true};
  bool properties_on_edges{true};
  bool loop{false};  // main has a self-loop on v3
};

ms::Config MakeConfig(Options options = {}) {
  ms::Config config{
      .durability = {.storage_directory = kStorageDirectory,
                     .recover_on_startup = false,
                     .snapshot_wal_mode = options.wal
                                              ? ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL
                                              : ms::Config::Durability::SnapshotWalMode::DISABLED,
                     .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)}},
      // A database has a branch registry only with versioning on or branches to keep.
      .experimental_versioning = true,
  };
  config.salient.items.properties_on_edges = options.properties_on_edges;
  return config;
}

auto *Mem(memgraph::dbms::Database &db) { return static_cast<ms::InMemoryStorage *>(db.storage()); }

std::string Render(const ms::ExternalPropertyValue &value) {
  if (value.IsNull()) return "null";
  if (value.IsBool()) return value.ValueBool() ? "true" : "false";
  if (value.IsInt()) return std::to_string(value.ValueInt());
  return "?";
}

// One line per record, in the form a test states it: "add_label:<gid>:<name>", "set_prop:<gid>:<name>=<value>",
// "set_edge_prop:<gid>:<name>=<value>:<from>-><to>:<type>", "create_edge:<gid>:<type>:<from>-><to>".
std::string Render(const dur::WalDeltaData &delta) {
  return std::visit(
      [](const auto &d) -> std::string {
        using T = std::decay_t<decltype(d)>;
        if constexpr (std::is_same_v<T, dur::WalVertexCreate>) return "create_vertex:" + std::to_string(d.gid.AsUint());
        if constexpr (std::is_same_v<T, dur::WalVertexDelete>) return "delete_vertex:" + std::to_string(d.gid.AsUint());
        if constexpr (std::is_same_v<T, dur::WalVertexAddLabel>)
          return "add_label:" + std::to_string(d.gid.AsUint()) + ":" + d.label;
        if constexpr (std::is_same_v<T, dur::WalVertexRemoveLabel>)
          return "remove_label:" + std::to_string(d.gid.AsUint()) + ":" + d.label;
        if constexpr (std::is_same_v<T, dur::WalVertexSetProperty>)
          return "set_prop:" + std::to_string(d.gid.AsUint()) + ":" + d.property + "=" + Render(d.value);
        if constexpr (std::is_same_v<T, dur::WalEdgeSetProperty>)
          return "set_edge_prop:" + std::to_string(d.gid.AsUint()) + ":" + d.property + "=" + Render(d.value) + ":" +
                 std::to_string(d.from_gid ? d.from_gid->AsUint() : ~0ULL) + "->" +
                 std::to_string(d.to_gid ? d.to_gid->AsUint() : ~0ULL) + ":" + d.edge_type.value_or("?");
        if constexpr (std::is_same_v<T, dur::WalEdgeCreate>)
          return "create_edge:" + std::to_string(d.gid.AsUint()) + ":" + d.edge_type + ":" +
                 std::to_string(d.from_vertex.AsUint()) + "->" + std::to_string(d.to_vertex.AsUint());
        if constexpr (std::is_same_v<T, dur::WalEdgeDelete>)
          return "delete_edge:" + std::to_string(d.gid.AsUint()) + ":" + d.edge_type + ":" +
                 std::to_string(d.from_vertex.AsUint()) + "->" + std::to_string(d.to_vertex.AsUint());
        return "other";
      },
      delta.data_);
}

struct WalTxn {
  uint64_t timestamp;
  std::vector<std::string> records;
};

struct WalContents {
  std::vector<WalTxn> txns;
  uint64_t decoded_deltas{0};
  uint64_t summarized_deltas{0};  // what the files' headers state, i.e. what UpdateStats counted
};

// All transactions of the WAL files in `wal_dir`, oldest file first. Empty when the directory does not exist. Call only
// once the owning storage is gone: a live storage may still hold its newest file open.
WalContents ReadWal(const std::filesystem::path &wal_dir) {
  WalContents out;
  if (!std::filesystem::exists(wal_dir)) return out;
  std::vector<std::pair<uint64_t, std::filesystem::path>> ordered;
  for (auto const &entry : std::filesystem::directory_iterator(wal_dir)) {
    if (entry.is_regular_file()) ordered.emplace_back(dur::ReadWalInfo(entry.path()).from_timestamp, entry.path());
  }
  std::ranges::sort(ordered);

  for (auto const &[from_ts, file] : ordered) {
    auto const header = dur::ReadWalHeader(file);
    EXPECT_TRUE(header.summary.has_value()) << file << " is not finalized";
    if (header.summary) out.summarized_deltas += header.summary->num_deltas;
    dur::Decoder wal;
    wal.Initialize(file, dur::kWalMagic);
    wal.SetPosition(header.offset_deltas);
    std::optional<WalTxn> open;
    while (wal.GetPosition() < wal.GetSize()) {
      auto const timestamp = dur::ReadWalDeltaHeader(&wal);
      auto delta = dur::ReadWalDeltaData(&wal);
      ++out.decoded_deltas;
      if (std::holds_alternative<dur::WalTransactionStart>(delta.data_)) {
        EXPECT_FALSE(open.has_value()) << "a transaction started inside another";
        open = WalTxn{.timestamp = timestamp, .records = {}};
      } else if (std::holds_alternative<dur::WalTransactionEnd>(delta.data_)) {
        EXPECT_TRUE(open.has_value()) << "a transaction ended without starting";
        if (open) out.txns.push_back(std::move(*open));
        open.reset();
      } else if (open) {
        open->records.push_back(Render(delta));
      }
    }
    EXPECT_FALSE(open.has_value()) << "the WAL ends inside a transaction";
  }
  return out;
}

std::vector<WalTxn> ReadWalTxns(const std::filesystem::path &wal_dir) { return ReadWal(wal_dir).txns; }

std::filesystem::path BranchWalDir(uint64_t number) {
  return kStorageDirectory / "branches" / std::to_string(number) / "wal";
}

void Commit(ms::Storage::Accessor &acc) {
  ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// v1(:L {p: 1}) -e1[:T {w: 1}]-> v2, and v3, isolated unless `loop` gives it the self-loop `loop_edge`.
struct Base {
  ms::Gid v1, v2, v3, e1, loop_edge;
};

Base CreateBase(memgraph::dbms::Database &db, bool properties_on_edges = true, bool loop = false) {
  auto *storage = db.storage();
  auto acc = db.Access(ms::WRITE);
  auto v1 = acc->CreateVertex();
  auto v2 = acc->CreateVertex();
  auto v3 = acc->CreateVertex();
  EXPECT_TRUE(v1.AddLabel(storage->NameToLabel("L")).has_value());
  EXPECT_TRUE(v1.SetProperty(storage->NameToProperty("p"), ms::PropertyValue(1)).has_value());
  auto e1 = acc->CreateEdge(&v1, &v2, storage->NameToEdgeType("T"));
  EXPECT_TRUE(e1.has_value());
  if (properties_on_edges) {
    EXPECT_TRUE(e1->SetProperty(storage->NameToProperty("w"), ms::PropertyValue(1)).has_value());
  }
  Base base{.v1 = v1.Gid(), .v2 = v2.Gid(), .v3 = v3.Gid(), .e1 = e1->Gid()};
  if (loop) {
    auto loop_edge = acc->CreateEdge(&v3, &v3, storage->NameToEdgeType("T"));
    EXPECT_TRUE(loop_edge.has_value());
    base.loop_edge = loop_edge->Gid();
  }
  Commit(*acc);
  return base;
}

class VersioningBranchWalTest : public ::testing::Test {
 protected:
  void SetUp() override { std::filesystem::remove_all(kStorageDirectory); }

  void TearDown() override { std::filesystem::remove_all(kStorageDirectory); }
};

struct Fixture {
  explicit Fixture(Options options = {}) : db{MakeConfig(options)}, arena_scope{&db.Arena()} {
    base = CreateBase(db, options.properties_on_edges, options.loop);
    auto info = db.version_store()->CreateBranch("b");
    EXPECT_TRUE(info.has_value());
    number = info->number;
    auto built = mv::BranchContext::BuildFromFork(*Mem(db), info->fork_ts, info->number);
    EXPECT_TRUE(built.has_value());
    ctx = std::move(*built);
  }

  ms::LabelId Label(std::string_view name) { return db.storage()->NameToLabel(name); }

  ms::PropertyId Prop(std::string_view name) { return db.storage()->NameToProperty(name); }

  memgraph::dbms::Database db;
  memgraph::memory::DbArenaScope arena_scope;
  Base base{};
  uint64_t number{};
  std::unique_ptr<mv::BranchContext> ctx;
};

template <typename Body>
void BranchTxn(mv::BranchContext &ctx, Body &&body) {
  auto diff_acc = ctx.diff_engine().Access(ms::WRITE);
  ASSERT_TRUE(ctx.BeginTransaction(diff_acc.get()).has_value());
  body(ctx);
  ASSERT_TRUE(ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
  ctx.EndTransaction();
}

ms::VertexAccessor ForkVertex(mv::BranchContext &ctx, ms::Gid gid) {
  auto fork = ctx.FindForkVertex(gid);
  EXPECT_TRUE(fork.has_value());
  return std::move(*fork);
}

ms::EdgeAccessor ForkEdge(mv::BranchContext &ctx, ms::Gid edge, ms::Gid from) {
  auto fork = ctx.FindForkEdge(edge, from);
  EXPECT_TRUE(fork.has_value());
  return std::move(*fork);
}

std::string RawBytes(const std::filesystem::path &dir) {
  std::string all;
  if (!std::filesystem::exists(dir)) return all;
  for (auto const &entry : std::filesystem::recursive_directory_iterator(dir)) {
    if (!entry.is_regular_file()) continue;
    std::ifstream in{entry.path(), std::ios::binary};
    all.append(std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{});
  }
  return all;
}

struct Outcome {
  Base base{};
  uint64_t number{};
  WalContents wal;

  const std::vector<std::string> &Records() const {
    EXPECT_EQ(wal.txns.size(), 1u);
    static const std::vector<std::string> kNone;
    return wal.txns.empty() ? kNone : wal.txns[0].records;
  }
};

// What no branch WAL may ever hold: the marker-looking names of an earlier encoding. A test
// whose user wrote such a name says so with `user_wrote_marker_names`.
constexpr std::array kMarkerBytes = {
    std::string_view{"__mg_removed_"}, std::string_view{"__mg_deleted"}, std::string_view{"__mg_lit_"}};

// Runs `body` on a fresh durable branch of a database holding `CreateBase`, closes everything, and decodes the branch
// WAL. Every case also checks that the header's delta count equals the records and that the file holds no marker.
template <typename Body>
Outcome RunBranch(Body &&body, Options options = {}, bool user_wrote_marker_names = false) {
  Outcome out;
  {
    Fixture f{options};
    body(f);
    f.ctx.reset();
    out.base = f.base;
    out.number = f.number;
  }
  out.wal = ReadWal(BranchWalDir(out.number));
  EXPECT_EQ(out.wal.summarized_deltas, out.wal.decoded_deltas) << "num_deltas must equal the records written";
  if (!user_wrote_marker_names) {
    auto const bytes = RawBytes(BranchWalDir(out.number));
    for (auto const needle : kMarkerBytes) {
      EXPECT_EQ(bytes.find(needle), std::string::npos) << "the branch WAL holds the marker text " << needle;
    }
  }
  return out;
}

std::string Gs(ms::Gid gid) { return std::to_string(gid.AsUint()); }
}  // namespace

TEST_F(VersioningBranchWalTest, NativeAndStubWritesShareOneStockWalTransaction) {
  uint64_t native_gid = 0;
  auto const out = RunBranch([&](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto native = c.CreateNativeVertex();
      native_gid = native.Gid().AsUint();
      ASSERT_TRUE(native.SetProperty(f.Prop("q"), ms::PropertyValue(7)).has_value());
      auto fork = ForkVertex(c, f.base.v1);
      ASSERT_TRUE(c.SetProperty(fork, f.Prop("p"), ms::PropertyValue(2)).has_value());
    });
  });
  auto const native = std::to_string(native_gid);
  auto const v1 = Gs(out.base.v1);
  EXPECT_THAT(out.Records(),
              UnorderedElementsAre("create_vertex:" + native, "set_prop:" + native + ":q=7", "set_prop:" + v1 + ":p=2"))
      << "native deltas and stub deltas are one stock WAL transaction; a stub is created without a create record: its "
         "gid is the fork vertex's";
}

TEST_F(VersioningBranchWalTest, RemovingAForkPropertyIsTwoSetsToNull) {
  auto const out = RunBranch([](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto fork = ForkVertex(c, f.base.v1);
      ASSERT_TRUE(c.SetProperty(fork, f.Prop("p"), ms::PropertyValue()).has_value());
    });
  });
  auto const v1 = Gs(out.base.v1);
  EXPECT_THAT(out.Records(), ElementsAre("set_prop:" + v1 + ":p=null", "set_prop:" + v1 + ":p=null"))
      << "the marker delta and the value drop both restate p, which is now null";
}

TEST_F(VersioningBranchWalTest, RemovingAPropertyThenSettingItRestatesTheNewValueForEveryDelta) {
  auto const out = RunBranch([](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto fork = ForkVertex(c, f.base.v1);
      ASSERT_TRUE(c.SetProperty(fork, f.Prop("p"), ms::PropertyValue()).has_value());
      ASSERT_TRUE(c.SetProperty(fork, f.Prop("p"), ms::PropertyValue(7)).has_value());
    });
  });
  auto const v1 = Gs(out.base.v1);
  EXPECT_THAT(out.Records(), Each("set_prop:" + v1 + ":p=7"))
      << "a record carries the value at commit, not the delta's";
  EXPECT_EQ(out.Records().size(), 4u) << "marker on, value drop, value set, marker off";
}

TEST_F(VersioningBranchWalTest, SettingThenRemovingAForkPropertyEndsNullOnEveryRecord) {
  auto const out = RunBranch([](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto fork = ForkVertex(c, f.base.v1);
      ASSERT_TRUE(c.SetProperty(fork, f.Prop("p"), ms::PropertyValue(5)).has_value());
      ASSERT_TRUE(c.SetProperty(fork, f.Prop("p"), ms::PropertyValue()).has_value());
    });
  });
  EXPECT_THAT(out.Records(), Each("set_prop:" + Gs(out.base.v1) + ":p=null"));
  EXPECT_EQ(out.Records().size(), 3u) << "value set, marker on, value drop";
}

TEST_F(VersioningBranchWalTest, SettingThenRemovingAPropertyTheForkLacksLeavesNoMarker) {
  auto const out = RunBranch([](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto fork = ForkVertex(c, f.base.v2);
      ASSERT_TRUE(c.SetProperty(fork, f.Prop("z"), ms::PropertyValue(5)).has_value());
      ASSERT_TRUE(c.SetProperty(fork, f.Prop("z"), ms::PropertyValue()).has_value());
    });
  });
  auto const v2 = Gs(out.base.v2);
  EXPECT_THAT(out.Records(), ElementsAre("set_prop:" + v2 + ":z=null", "set_prop:" + v2 + ":z=null"));
}

TEST_F(VersioningBranchWalTest, AStubPropertyValueIsRecordedAsOfCommitAcrossTransactions) {
  auto const out = RunBranch([](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto fork = ForkVertex(c, f.base.v1);
      ASSERT_TRUE(c.SetProperty(fork, f.Prop("p"), ms::PropertyValue(3)).has_value());
    });
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto fork = ForkVertex(c, f.base.v1);
      ASSERT_TRUE(c.SetProperty(fork, f.Prop("p"), ms::PropertyValue()).has_value());
    });
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto fork = ForkVertex(c, f.base.v1);
      ASSERT_TRUE(c.SetProperty(fork, f.Prop("p"), ms::PropertyValue(5)).has_value());
    });
  });
  ASSERT_EQ(out.wal.txns.size(), 3u);
  EXPECT_LT(out.wal.txns[0].timestamp, out.wal.txns[1].timestamp) << "WAL order is commit order";
  EXPECT_LT(out.wal.txns[1].timestamp, out.wal.txns[2].timestamp);
  auto const v1 = Gs(out.base.v1);
  EXPECT_THAT(out.wal.txns[0].records, ElementsAre("set_prop:" + v1 + ":p=3"));
  EXPECT_THAT(out.wal.txns[1].records, Each("set_prop:" + v1 + ":p=null"));
  EXPECT_THAT(out.wal.txns[2].records, Each("set_prop:" + v1 + ":p=5")) << "set, then the marker turned off";
}

TEST_F(VersioningBranchWalTest, RemovingAForkLabelIsARemoveLabelAndReAddingItIsAnAddLabel) {
  auto const out = RunBranch([](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto fork = ForkVertex(c, f.base.v1);
      ASSERT_TRUE(*c.RemoveLabel(fork, f.Label("L")));
    });
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto fork = ForkVertex(c, f.base.v1);
      ASSERT_TRUE(*c.AddLabel(fork, f.Label("L"))) << "un-removing a fork label";
    });
  });
  ASSERT_EQ(out.wal.txns.size(), 2u);
  auto const v1 = Gs(out.base.v1);
  EXPECT_THAT(out.wal.txns[0].records, ElementsAre("remove_label:" + v1 + ":L")) << "the marker turns on: user REMOVE";
  EXPECT_THAT(out.wal.txns[1].records, ElementsAre("add_label:" + v1 + ":L")) << "the marker turns off: user ADD";
}

TEST_F(VersioningBranchWalTest, RemovingAndReAddingAForkLabelInOneTransactionKeepsTheOrder) {
  auto const out = RunBranch([](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto fork = ForkVertex(c, f.base.v1);
      ASSERT_TRUE(*c.RemoveLabel(fork, f.Label("L")));
      ASSERT_TRUE(*c.AddLabel(fork, f.Label("L")));
    });
  });
  auto const v1 = Gs(out.base.v1);
  EXPECT_THAT(out.Records(), ElementsAre("remove_label:" + v1 + ":L", "add_label:" + v1 + ":L"));
}

TEST_F(VersioningBranchWalTest, AddingThenRemovingALabelTheForkLacksIsPlainAddAndRemove) {
  auto const out = RunBranch([](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto fork = ForkVertex(c, f.base.v1);
      ASSERT_TRUE(*c.AddLabel(fork, f.Label("NewL")));
      ASSERT_TRUE(*c.RemoveLabel(fork, f.Label("NewL")));
    });
  });
  auto const v1 = Gs(out.base.v1);
  EXPECT_THAT(out.Records(), ElementsAre("add_label:" + v1 + ":NewL", "remove_label:" + v1 + ":NewL"));
}

TEST_F(VersioningBranchWalTest, DeletingAForkVertexAfterWritesEndsItsRecordsWithDeleteVertex) {
  auto const out = RunBranch([](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto fork = ForkVertex(c, f.base.v1);
      ASSERT_TRUE(*c.AddLabel(fork, f.Label("NewL")));
      ASSERT_TRUE(c.SetProperty(fork, f.Prop("p"), ms::PropertyValue(2)).has_value());
      auto deleted = c.DeleteForkVertex(f.base.v1);
      ASSERT_TRUE(deleted.has_value());
      ASSERT_TRUE(*deleted);
    });
  });
  auto const v1 = Gs(out.base.v1);
  EXPECT_THAT(out.Records(),
              ElementsAre("add_label:" + v1 + ":NewL",
                          "set_prop:" + v1 + ":p=2",
                          "delete_vertex:" + v1,
                          "delete_edge:" + Gs(out.base.e1) + ":T:" + v1 + "->" + Gs(out.base.v2)))
      << "the delete marker is the stub's newest delta; the edge's own marker follows in the edge pass";
}

// A fork vertex delete is a vertex-pass record, so it precedes the edge records of the same transaction.
TEST_F(VersioningBranchWalTest, AForkVertexDeleteIsEmittedBeforeTheNativeEdgeRecordsOfItsTransaction) {
  uint64_t native = 0;
  auto const out = RunBranch([&](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto stub = c.EnsureDiffVertex(f.base.v2);
      ASSERT_TRUE(stub.has_value());
      auto vertex = c.CreateNativeVertex();
      native = vertex.Gid().AsUint();
      ASSERT_TRUE(c.CreateNativeEdge(*stub, vertex, f.db.storage()->NameToEdgeType("N")).has_value());
      auto deleted = c.DeleteForkVertex(f.base.v2);
      ASSERT_TRUE(deleted.has_value());
      ASSERT_TRUE(*deleted);
    });
  });
  auto const &records = out.Records();
  auto const delete_at = std::ranges::find(records, "delete_vertex:" + Gs(out.base.v2)) - records.begin();
  auto const create_at =
      std::ranges::find_if(records, [](auto const &r) { return r.starts_with("create_edge:"); }) - records.begin();
  ASSERT_LT(delete_at, static_cast<std::ptrdiff_t>(records.size()));
  ASSERT_LT(create_at, static_cast<std::ptrdiff_t>(records.size()));
  EXPECT_LT(delete_at, create_at);
  EXPECT_THAT(records, Contains("create_vertex:" + std::to_string(native)));
}

TEST_F(VersioningBranchWalTest, AForkVertexDeleteIsEmittedBeforeTheDeleteOfACommittedNativeEdgeOnIt) {
  uint64_t native_edge = 0;
  auto const out = RunBranch([&](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto stub = c.EnsureDiffVertex(f.base.v2);
      ASSERT_TRUE(stub.has_value());
      auto vertex = c.CreateNativeVertex();
      auto edge = c.CreateNativeEdge(*stub, vertex, f.db.storage()->NameToEdgeType("N"));
      ASSERT_TRUE(edge.has_value());
      native_edge = edge->Gid().AsUint();
    });
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto edge = c.FindDiffEdge(ms::Gid::FromUint(native_edge), f.base.v2, ms::View::NEW);
      ASSERT_TRUE(edge.has_value());
      ASSERT_TRUE(c.current_diff_txn()->DeleteEdge(&*edge).has_value());
      auto deleted = c.DeleteForkVertex(f.base.v2);
      ASSERT_TRUE(deleted.has_value());
      ASSERT_TRUE(*deleted);
    });
  });
  ASSERT_EQ(out.wal.txns.size(), 2u);
  auto const &records = out.wal.txns[1].records;
  auto const delete_at = std::ranges::find(records, "delete_vertex:" + Gs(out.base.v2)) - records.begin();
  auto const edge_at =
      std::ranges::find_if(records, [](auto const &r) { return r.starts_with("delete_edge:"); }) - records.begin();
  ASSERT_LT(delete_at, static_cast<std::ptrdiff_t>(records.size()));
  ASSERT_LT(edge_at, static_cast<std::ptrdiff_t>(records.size()));
  EXPECT_LT(delete_at, edge_at);
}

TEST_F(VersioningBranchWalTest, AForkEdgeDeleteThenItsEndpointDeleteEmitsTheVertexFirstAndTheEdgeLater) {
  auto const out = RunBranch([](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto edge = ForkEdge(c, f.base.e1, f.base.v1);
      auto deleted_edge = c.DeleteForkEdge(edge);
      ASSERT_TRUE(deleted_edge.has_value());
      ASSERT_TRUE(*deleted_edge);
      auto deleted = c.DeleteForkVertex(f.base.v1);
      ASSERT_TRUE(deleted.has_value());
      ASSERT_TRUE(*deleted);
    });
  });
  auto const tail = ":T:" + Gs(out.base.v1) + "->" + Gs(out.base.v2);
  EXPECT_THAT(out.Records(), ElementsAre("delete_vertex:" + Gs(out.base.v1), "delete_edge:" + Gs(out.base.e1) + tail));
}

TEST_F(VersioningBranchWalTest, ForkEdgePropertyWritesCarryEndpointsAndType) {
  auto const out = RunBranch([](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto edge = ForkEdge(c, f.base.e1, f.base.v1);
      ASSERT_TRUE(c.SetProperty(edge, f.Prop("w"), ms::PropertyValue()).has_value());
    });
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto edge = ForkEdge(c, f.base.e1, f.base.v1);
      ASSERT_TRUE(c.SetProperty(edge, f.Prop("w"), ms::PropertyValue(5)).has_value());
    });
  });
  ASSERT_EQ(out.wal.txns.size(), 2u);
  auto const tail = ":" + Gs(out.base.v1) + "->" + Gs(out.base.v2) + ":T";
  auto const e1 = Gs(out.base.e1);
  // Each record carries the stub's value of the stripped key at commit: a removal leaves null on the marker and the
  // value drop alike, and re-setting over it leaves 5 on the value set and the marker clear alike.
  auto const removed = "set_edge_prop:" + e1 + ":w=null" + tail;
  auto const set = "set_edge_prop:" + e1 + ":w=5" + tail;
  EXPECT_THAT(out.wal.txns[0].records, ElementsAre(removed, removed)) << "marker on, value drop";
  EXPECT_THAT(out.wal.txns[1].records, ElementsAre(set, set)) << "value set, marker off";
}

// A fork edge delete is one DELETE_EDGE and nothing else: stub adjacency markers (a self-loop's included) are never
// logged. A fork vertex delete is one DELETE_VERTEX plus one DELETE_EDGE per hidden fork edge.
TEST_F(VersioningBranchWalTest, DeletedForkEdgesLogOnlyTheirDeleteEdgeRecords) {
  auto const out = RunBranch(
      [](Fixture &f) {
        BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
          auto deleted = c.DeleteForkEdge(ForkEdge(c, f.base.loop_edge, f.base.v3));
          ASSERT_TRUE(deleted.has_value());
          ASSERT_TRUE(*deleted);
        });
        BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
          auto deleted = c.DeleteForkVertex(f.base.v1);
          ASSERT_TRUE(deleted.has_value());
          ASSERT_TRUE(*deleted);
        });
      },
      {.loop = true});
  ASSERT_EQ(out.wal.txns.size(), 2u);
  EXPECT_THAT(out.wal.txns[0].records,
              ElementsAre("delete_edge:" + Gs(out.base.loop_edge) + ":T:" + Gs(out.base.v3) + "->" + Gs(out.base.v3)));
  EXPECT_THAT(out.wal.txns[1].records,
              ElementsAre("delete_vertex:" + Gs(out.base.v1),
                          "delete_edge:" + Gs(out.base.e1) + ":T:" + Gs(out.base.v1) + "->" + Gs(out.base.v2)));
}

TEST_F(VersioningBranchWalTest, DeletingAForkEdgeWorksWhenMainHasNoEdgeProperties) {
  auto const out = RunBranch(
      [](Fixture &f) {
        BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
          auto edge = ForkEdge(c, f.base.e1, f.base.v1);
          auto deleted = c.DeleteForkEdge(edge);
          ASSERT_TRUE(deleted.has_value());
          ASSERT_TRUE(*deleted);
        });
      },
      {.properties_on_edges = false});
  EXPECT_THAT(out.Records(),
              ElementsAre("delete_edge:" + Gs(out.base.e1) + ":T:" + Gs(out.base.v1) + "->" + Gs(out.base.v2)))
      << "the edge stub is a heap edge whatever main stores, and its endpoints are the fork vertices";
}

// Names that spell what an encoding of markers might have used are the user's own names and are written as such.
TEST_F(VersioningBranchWalTest, UserNamesThatLookLikeMarkersAreWrittenVerbatim) {
  auto const nul_name = std::string{"a\0b", 3};
  auto const high_name = std::string{"\xff\xfe_x"};
  std::vector<std::string> names = {"__mg_removed_x", "__mg_deleted", "__mg_lit_x", "__mg_id__", nul_name, high_name};
  auto const out = RunBranch(
      [&](Fixture &f) {
        BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
          auto fork = ForkVertex(c, f.base.v1);
          for (auto const &name : names) {
            auto const label = f.Label(name);
            auto const prop = f.Prop(name);
            ASSERT_FALSE(ms::IsReservedId(label));
            ASSERT_FALSE(ms::IsReservedId(prop));
            ASSERT_TRUE(*c.AddLabel(fork, label));
            ASSERT_TRUE(c.SetProperty(fork, prop, ms::PropertyValue(1)).has_value());
          }
          ASSERT_TRUE(*c.RemoveLabel(fork, f.Label("L")));
          // A real marker next to the lookalikes.
          ASSERT_TRUE(c.SetProperty(fork, f.Prop("p"), ms::PropertyValue()).has_value());
        });
      },
      {},
      /*user_wrote_marker_names=*/true);

  auto const v1 = Gs(out.base.v1);
  std::vector<std::string> expected = {"remove_label:" + v1 + ":L"};
  for (auto const &name : names) {
    expected.push_back("add_label:" + v1 + ":" + name);
    expected.push_back("set_prop:" + v1 + ":" + name + "=1");
  }
  expected.push_back("set_prop:" + v1 + ":p=null");
  expected.push_back("set_prop:" + v1 + ":p=null");
  EXPECT_THAT(out.Records(), UnorderedElementsAreArray(expected));
}

// Main never holds a reserved id, so the hook in the encoder never fires for it: its WAL is the stock one, with the
// records of exactly what was written, whatever branches exist.
TEST_F(VersioningBranchWalTest, MainsOwnWalIsTheStockEncodingWhileABranchIsWrittenTo) {
  uint64_t number = 0;
  Base base{};
  {
    Fixture f;
    number = f.number;
    base = f.base;
    auto *storage = f.db.storage();
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto fork = ForkVertex(c, f.base.v1);
      ASSERT_TRUE(*c.RemoveLabel(fork, f.Label("L")));
      ASSERT_TRUE(c.SetProperty(fork, f.Prop("p"), ms::PropertyValue()).has_value());
      auto deleted = c.DeleteForkVertex(f.base.v3);
      ASSERT_TRUE(deleted.has_value());
    });
    auto acc = f.db.Access(ms::WRITE);
    auto v = acc->FindVertex(f.base.v1, ms::View::NEW);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->RemoveLabel(storage->NameToLabel("L")).has_value());
    ASSERT_TRUE(v->SetProperty(storage->NameToProperty("p"), ms::PropertyValue()).has_value());
    auto deleted_vertex = acc->FindVertex(f.base.v3, ms::View::NEW);
    ASSERT_TRUE(deleted_vertex.has_value());
    ASSERT_TRUE(acc->DeleteVertex(&*deleted_vertex).has_value());
    Commit(*acc);
  }
  auto const main_wal = ReadWalTxns(kStorageDirectory / "wal");
  ASSERT_GE(main_wal.size(), 2u);
  auto const &last = main_wal.back().records;
  EXPECT_THAT(last,
              UnorderedElementsAre(::testing::HasSubstr("remove_label:"),
                                   ::testing::HasSubstr(":p=null"),
                                   ::testing::HasSubstr("delete_vertex:")));
  auto const bytes = RawBytes(kStorageDirectory / "wal");
  for (auto const needle : kMarkerBytes) EXPECT_EQ(bytes.find(needle), std::string::npos) << needle;

  // The branch's own record of the same writes, including the delete of an isolated fork vertex.
  auto const branch_wal = ReadWal(BranchWalDir(number));
  ASSERT_EQ(branch_wal.txns.size(), 1u);
  auto const v1 = Gs(base.v1);
  EXPECT_THAT(branch_wal.txns[0].records,
              UnorderedElementsAre("remove_label:" + v1 + ":L",
                                   "set_prop:" + v1 + ":p=null",
                                   "set_prop:" + v1 + ":p=null",
                                   "delete_vertex:" + Gs(base.v3)));
  EXPECT_EQ(branch_wal.summarized_deltas, branch_wal.decoded_deltas);
}

TEST_F(VersioningBranchWalTest, EnumMirrorStaysOutOfTheBranchWal) {
  uint64_t number = 0;
  {
    memgraph::dbms::Database db{MakeConfig()};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    {
      auto unique = db.UniqueAccess();
      ASSERT_TRUE(unique->CreateEnum("Color", std::vector<std::string>{"red", "green"}).has_value());
      Commit(*unique);
    }
    auto const base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b");
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = mv::BranchContext::BuildFromFork(*Mem(db), info->fork_ts, info->number);
    ASSERT_TRUE(ctx.has_value());
    BranchTxn(**ctx, [&](mv::BranchContext &c) {
      auto fork = ForkVertex(c, base.v1);
      ASSERT_TRUE(c.SetProperty(fork, db.storage()->NameToProperty("p"), ms::PropertyValue(2)).has_value());
    });
    ctx->reset();
  }

  auto const txns = ReadWalTxns(BranchWalDir(number));
  ASSERT_EQ(txns.size(), 1u) << "the build's enum-mirror commit is rebuilt every time, so it is not in the WAL";
  EXPECT_EQ(txns[0].records.size(), 1u);
}

TEST_F(VersioningBranchWalTest, RolledBackTransactionWritesNothing) {
  uint64_t number = 0;
  ms::Gid v1;
  {
    Fixture f;
    number = f.number;
    v1 = f.base.v1;
    auto const p = f.Prop("p");
    {
      auto diff_acc = f.ctx->diff_engine().Access(ms::WRITE);
      ASSERT_TRUE(f.ctx->BeginTransaction(diff_acc.get()).has_value());
      static_cast<void>(f.ctx->CreateNativeVertex());
      auto fork = ForkVertex(*f.ctx, f.base.v1);
      ASSERT_TRUE(f.ctx->SetProperty(fork, p, ms::PropertyValue(9)).has_value());
      f.ctx->EndTransaction();  // no commit: the diff transaction aborts
    }
    {
      auto diff_acc = f.ctx->diff_engine().Access(ms::WRITE);
      ASSERT_TRUE(f.ctx->BeginTransaction(diff_acc.get()).has_value());
      auto fork = ForkVertex(*f.ctx, f.base.v1);
      ASSERT_TRUE(f.ctx->SetProperty(fork, p, ms::PropertyValue(9)).has_value());
      f.ctx->EndTransaction();
    }
    // Positive control: the branch WAL is live, so the aborted blocks above provably wrote nothing.
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto fork = ForkVertex(c, f.base.v1);
      ASSERT_TRUE(c.SetProperty(fork, p, ms::PropertyValue(4)).has_value());
    });
    f.ctx.reset();
  }

  auto const txns = ReadWalTxns(BranchWalDir(number));
  ASSERT_EQ(txns.size(), 1u) << "only the committed control transaction is in the WAL";
  EXPECT_THAT(txns[0].records, ElementsAre("set_prop:" + Gs(v1) + ":p=4"));
}

TEST_F(VersioningBranchWalTest, BranchOfAMainWithoutWalWritesNoBranchWal) {
  constexpr uint64_t kNumber = 7;
  {
    memgraph::dbms::Database db{MakeConfig({.wal = false})};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto const base = CreateBase(db);
    auto *storage = Mem(db);
    auto const fork_ts = storage->RegisterForkPin();
    {
      auto ctx = mv::BranchContext::BuildFromFork(*storage, fork_ts, kNumber);
      ASSERT_TRUE(ctx.has_value());
      auto const p = db.storage()->NameToProperty("p");
      BranchTxn(**ctx, [&](mv::BranchContext &c) {
        static_cast<void>(c.CreateNativeVertex());
        auto fork = ForkVertex(c, base.v1);
        ASSERT_TRUE(c.SetProperty(fork, p, ms::PropertyValue(2)).has_value());
      });
      BranchTxn(**ctx, [&](mv::BranchContext &c) {
        auto fork = ForkVertex(c, base.v1);
        ASSERT_TRUE(c.SetProperty(fork, p, ms::PropertyValue(3)).has_value());
      });
    }
    storage->ReleaseForkPin(fork_ts);
  }

  auto const branch_dir = kStorageDirectory / "branches" / std::to_string(kNumber);
  if (std::filesystem::exists(branch_dir)) {
    for (auto const &entry : std::filesystem::recursive_directory_iterator(branch_dir)) {
      EXPECT_FALSE(entry.is_regular_file()) << "branch durability off, yet " << entry.path() << " exists";
    }
  }
}
