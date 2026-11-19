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

// A durable branch logs through its diff engine's stock WAL. An overlay's changes are stock records (labels, property
// values, a delete mark as DELETE_VERTEX); a deleted base edge is a DELETE_EDGE written from its edge overlay's delete
// mark. The WAL files are decoded and checked here.
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "dbms/database.hpp"
#include "kvstore/kvstore.hpp"
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
using ::testing::ElementsAre;
using ::testing::UnorderedElementsAre;

const std::filesystem::path kStorageDirectory{std::filesystem::temp_directory_path() / "versioning_branch_wal"};
// The DBMS durability KVStore that versioning requires for the branch ledger.
const std::filesystem::path kKvDirectory{std::filesystem::temp_directory_path() / "versioning_branch_wal_kv"};

struct Options {
  bool properties_on_edges{true};
  bool loop{false};  // main has a self-loop on v3
};

ms::Config MakeConfig(Options options = {}) {
  ms::Config config{
      .durability = {.storage_directory = kStorageDirectory,
                     .recover_on_startup = false,
                     .snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
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
  void SetUp() override {
    std::filesystem::remove_all(kStorageDirectory);
    std::filesystem::remove_all(kKvDirectory);
  }

  void TearDown() override {
    std::filesystem::remove_all(kStorageDirectory);
    std::filesystem::remove_all(kKvDirectory);
  }
};

struct Fixture {
  explicit Fixture(Options options = {}) : kv{kKvDirectory}, db{MakeConfig(options), &kv}, arena_scope{&db.Arena()} {
    base = CreateBase(db, options.properties_on_edges, options.loop);
    EXPECT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto const listing = db.version_store()->Get("b");
    EXPECT_TRUE(listing.has_value());
    number = listing->info.id;
    auto built = mv::BranchContext::BuildFromFork(*Mem(db), listing->fork_ts, number);
    EXPECT_TRUE(built.has_value());
    ctx = std::move(*built);
  }

  ms::LabelId Label(std::string_view name) { return db.storage()->NameToLabel(name); }

  ms::PropertyId Prop(std::string_view name) { return db.storage()->NameToProperty(name); }

  memgraph::kvstore::KVStore kv;  // declared before `db` so it outlives it
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

ms::VertexAccessor BaseVertex(mv::BranchContext &ctx, ms::Gid gid) {
  auto base_vertex = ctx.FindBaseVertex(gid);
  EXPECT_TRUE(base_vertex.has_value());
  return std::move(*base_vertex);
}

ms::EdgeAccessor BaseEdge(mv::BranchContext &ctx, ms::Gid edge, ms::Gid from) {
  auto base_edge = ctx.FindBaseEdge(edge, from);
  EXPECT_TRUE(base_edge.has_value());
  return std::move(*base_edge);
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

// Runs `body` on a fresh durable branch of a database holding `CreateBase`, closes everything, and decodes the branch
// WAL. Every case also checks that the header's delta count equals the records.
template <typename Body>
Outcome RunBranch(Body &&body, Options options = {}) {
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
  return out;
}

std::string Gs(ms::Gid gid) { return std::to_string(gid.AsUint()); }
}  // namespace

TEST_F(VersioningBranchWalTest, NativeWritesAndOverlayWritesShareOneStockWalTransaction) {
  uint64_t native_gid = 0;
  auto const out = RunBranch([&](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto native = c.CreateNativeVertex();
      native_gid = native.Gid().AsUint();
      ASSERT_TRUE(native.AddLabel(f.Label("N")).has_value());
      ASSERT_TRUE(native.SetProperty(f.Prop("q"), ms::PropertyValue(7)).has_value());
      auto base_vertex = BaseVertex(c, f.base.v1);
      ASSERT_TRUE(c.SetProperty(base_vertex, f.Prop("p"), ms::PropertyValue(2)).has_value());
    });
  });
  auto const native = std::to_string(native_gid);
  EXPECT_THAT(out.Records(),
              UnorderedElementsAre("create_vertex:" + native,
                                   "add_label:" + native + ":N",
                                   "set_prop:" + native + ":q=7",
                                   "set_prop:" + Gs(out.base.v1) + ":p=2"))
      << "an overlay is written without a create record: its gid is the base vertex's";
}

TEST_F(VersioningBranchWalTest, NativeEdgeOnAnOverlayIsAStockCreateEdge) {
  uint64_t native_edge = 0;
  uint64_t native_vertex = 0;
  auto const out = RunBranch([&](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto overlay = c.EnsureDiffVertex(f.base.v2);
      ASSERT_TRUE(overlay.has_value());
      auto vertex = c.CreateNativeVertex();
      native_vertex = vertex.Gid().AsUint();
      auto edge = c.CreateNativeEdge(*overlay, vertex, f.db.storage()->NameToEdgeType("N"));
      ASSERT_TRUE(edge.has_value());
      native_edge = edge->Gid().AsUint();
    });
  });
  EXPECT_THAT(out.Records(),
              ElementsAre("create_vertex:" + std::to_string(native_vertex),
                          "create_edge:" + std::to_string(native_edge) + ":N:" + Gs(out.base.v2) + "->" +
                              std::to_string(native_vertex)));
}

TEST_F(VersioningBranchWalTest, OverlayPropertyRemovalIsAStockSetToNull) {
  auto const out = RunBranch([](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto base_vertex = BaseVertex(c, f.base.v1);
      ASSERT_TRUE(c.SetProperty(base_vertex, f.Prop("p"), ms::PropertyValue()).has_value());
    });
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto base_vertex = BaseVertex(c, f.base.v1);
      ASSERT_TRUE(c.SetProperty(base_vertex, f.Prop("p"), ms::PropertyValue(5)).has_value());
      ASSERT_TRUE(c.SetProperty(base_vertex, f.Prop("p"), ms::PropertyValue()).has_value());
    });
  });
  ASSERT_EQ(out.wal.txns.size(), 2u);
  auto const v1 = Gs(out.base.v1);
  EXPECT_THAT(out.wal.txns[0].records, ElementsAre("set_prop:" + v1 + ":p=null"));
  EXPECT_THAT(out.wal.txns[1].records, ElementsAre("set_prop:" + v1 + ":p=null", "set_prop:" + v1 + ":p=null"))
      << "a record restates the value at commit";
}

TEST_F(VersioningBranchWalTest, OverlayLabelAddAndRemoveAreStockRecordsInOrder) {
  auto const out = RunBranch([](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto base_vertex = BaseVertex(c, f.base.v1);
      ASSERT_TRUE(*c.RemoveLabel(base_vertex, f.Label("L")));
      ASSERT_TRUE(*c.AddLabel(base_vertex, f.Label("NewL")));
    });
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto base_vertex = BaseVertex(c, f.base.v1);
      ASSERT_TRUE(*c.AddLabel(base_vertex, f.Label("L")));
    });
  });
  ASSERT_EQ(out.wal.txns.size(), 2u);
  auto const v1 = Gs(out.base.v1);
  EXPECT_THAT(out.wal.txns[0].records, ElementsAre("remove_label:" + v1 + ":L", "add_label:" + v1 + ":NewL"));
  EXPECT_THAT(out.wal.txns[1].records, ElementsAre("add_label:" + v1 + ":L"));
}

TEST_F(VersioningBranchWalTest, VertexMarkDeletedIsDeleteVertexAfterItsBaseEdgesAreDeleted) {
  auto const out = RunBranch([](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto base_vertex = BaseVertex(c, f.base.v2);
      ASSERT_TRUE(c.SetProperty(base_vertex, f.Prop("p"), ms::PropertyValue(2)).has_value());
      auto deleted = c.DeleteBaseVertex(f.base.v2, true);
      ASSERT_TRUE(deleted.has_value());
      ASSERT_TRUE(*deleted);
    });
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto deleted = c.DeleteBaseVertex(f.base.v3, false);
      ASSERT_TRUE(deleted.has_value());
      ASSERT_TRUE(*deleted);
    });
  });
  ASSERT_EQ(out.wal.txns.size(), 2u);
  EXPECT_THAT(out.wal.txns[0].records,
              ElementsAre("set_prop:" + Gs(out.base.v2) + ":p=2",
                          "delete_edge:" + Gs(out.base.e1) + ":T:" + Gs(out.base.v1) + "->" + Gs(out.base.v2),
                          "delete_vertex:" + Gs(out.base.v2)))
      << "the incident base edge goes first, as a replay needs";
  EXPECT_THAT(out.wal.txns[1].records, ElementsAre("delete_vertex:" + Gs(out.base.v3)));
}

TEST_F(VersioningBranchWalTest, BaseEdgeDeleteIsExactlyOneDeleteEdge) {
  auto const out = RunBranch(
      [](Fixture &f) {
        BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
          auto deleted = c.DeleteBaseEdge(BaseEdge(c, f.base.e1, f.base.v1));
          ASSERT_TRUE(deleted.has_value());
          ASSERT_TRUE(*deleted);
        });
        BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
          auto deleted = c.DeleteBaseEdge(BaseEdge(c, f.base.loop_edge, f.base.v3));
          ASSERT_TRUE(deleted.has_value());
          ASSERT_TRUE(*deleted);
        });
      },
      {.loop = true});
  ASSERT_EQ(out.wal.txns.size(), 2u);
  EXPECT_THAT(out.wal.txns[0].records,
              ElementsAre("delete_edge:" + Gs(out.base.e1) + ":T:" + Gs(out.base.v1) + "->" + Gs(out.base.v2)));
  EXPECT_THAT(out.wal.txns[1].records,
              ElementsAre("delete_edge:" + Gs(out.base.loop_edge) + ":T:" + Gs(out.base.v3) + "->" + Gs(out.base.v3)));
}

TEST_F(VersioningBranchWalTest, NativeEdgeDeleteIsExactlyOneDeleteEdge) {
  uint64_t from = 0;
  uint64_t to = 0;
  uint64_t edge = 0;
  auto const out = RunBranch([&](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto a = c.CreateNativeVertex();
      auto b = c.CreateNativeVertex();
      auto created = c.CreateNativeEdge(a, b, f.db.storage()->NameToEdgeType("N"));
      ASSERT_TRUE(created.has_value());
      from = a.Gid().AsUint();
      to = b.Gid().AsUint();
      edge = created->Gid().AsUint();
    });
    auto diff_acc = f.ctx->diff_engine().Access(ms::WRITE);
    ASSERT_TRUE(f.ctx->BeginTransaction(diff_acc.get()).has_value());
    auto a = diff_acc->FindVertex(ms::Gid::FromUint(from), ms::View::NEW);
    ASSERT_TRUE(a.has_value());
    auto edges = a->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.has_value());
    ASSERT_EQ(edges->edges.size(), 1u);
    auto native_edge = edges->edges[0];
    ASSERT_TRUE(diff_acc->DeleteEdge(&native_edge).has_value());
    ASSERT_TRUE(f.ctx->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    f.ctx->EndTransaction();
  });
  ASSERT_EQ(out.wal.txns.size(), 2u);
  EXPECT_THAT(
      out.wal.txns[1].records,
      ElementsAre("delete_edge:" + std::to_string(edge) + ":N:" + std::to_string(from) + "->" + std::to_string(to)));
}

TEST_F(VersioningBranchWalTest, BaseEdgeDeleteWorksWhenMainHasNoEdgeProperties) {
  auto const out = RunBranch(
      [](Fixture &f) {
        BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
          auto deleted = c.DeleteBaseEdge(BaseEdge(c, f.base.e1, f.base.v1));
          ASSERT_TRUE(deleted.has_value());
          ASSERT_TRUE(*deleted);
        });
      },
      {.properties_on_edges = false});
  EXPECT_THAT(out.Records(),
              ElementsAre("delete_edge:" + Gs(out.base.e1) + ":T:" + Gs(out.base.v1) + "->" + Gs(out.base.v2)))
      << "the edge overlay is a heap edge whatever main stores";
}

TEST_F(VersioningBranchWalTest, BaseEdgePropertyWritesCarryEndpointsAndTypeAndPrecedeTheEdgeDelete) {
  auto const out = RunBranch([](Fixture &f) {
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto edge = BaseEdge(c, f.base.e1, f.base.v1);
      ASSERT_TRUE(c.SetProperty(edge, f.Prop("w"), ms::PropertyValue()).has_value());
    });
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto edge = BaseEdge(c, f.base.e1, f.base.v1);
      ASSERT_TRUE(c.SetProperty(edge, f.Prop("w"), ms::PropertyValue(5)).has_value());
      auto deleted = c.DeleteBaseEdge(edge);
      ASSERT_TRUE(deleted.has_value());
      ASSERT_TRUE(*deleted);
    });
  });
  ASSERT_EQ(out.wal.txns.size(), 2u);
  auto const e1 = Gs(out.base.e1);
  auto const tail = Gs(out.base.v1) + "->" + Gs(out.base.v2);
  EXPECT_THAT(out.wal.txns[0].records, ElementsAre("set_edge_prop:" + e1 + ":w=null:" + tail + ":T"));
  EXPECT_THAT(out.wal.txns[1].records,
              ElementsAre("set_edge_prop:" + e1 + ":w=5:" + tail + ":T", "delete_edge:" + e1 + ":T:" + tail))
      << "a replay must set the property before it deletes the edge";
}

// A main workload of edge creates and deletes must be what it always was, whatever a branch writes beside it.
TEST_F(VersioningBranchWalTest, MainsOwnWalIsUnchangedWhileABranchIsWrittenTo) {
  uint64_t number = 0;
  Base base{};
  ms::Gid created_edge;
  {
    Fixture f;
    number = f.number;
    base = f.base;
    auto *storage = f.db.storage();
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto deleted = c.DeleteBaseEdge(BaseEdge(c, f.base.e1, f.base.v1));
      ASSERT_TRUE(deleted.has_value());
    });
    {
      auto acc = f.db.Access(ms::WRITE);
      auto v1 = acc->FindVertex(f.base.v1, ms::View::NEW);
      auto v3 = acc->FindVertex(f.base.v3, ms::View::NEW);
      ASSERT_TRUE(v1.has_value() && v3.has_value());
      auto edge = acc->CreateEdge(&*v1, &*v3, storage->NameToEdgeType("M"));
      ASSERT_TRUE(edge.has_value());
      created_edge = edge->Gid();
      Commit(*acc);
    }
    auto acc = f.db.Access(ms::WRITE);
    auto v1 = acc->FindVertex(f.base.v1, ms::View::NEW);
    ASSERT_TRUE(v1.has_value());
    auto edges = v1->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.has_value());
    for (auto edge : edges->edges) {
      if (edge.Gid() == base.e1) ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
    }
    Commit(*acc);
  }
  auto const main_wal = ReadWal(kStorageDirectory / "wal");
  ASSERT_GE(main_wal.txns.size(), 3u);
  auto const &create = main_wal.txns[main_wal.txns.size() - 2].records;
  EXPECT_THAT(create, ElementsAre("create_edge:" + Gs(created_edge) + ":M:" + Gs(base.v1) + "->" + Gs(base.v3)));
  EXPECT_THAT(main_wal.txns.back().records,
              ElementsAre("delete_edge:" + Gs(base.e1) + ":T:" + Gs(base.v1) + "->" + Gs(base.v2)));
  EXPECT_EQ(main_wal.summarized_deltas, main_wal.decoded_deltas);

  auto const branch_wal = ReadWal(BranchWalDir(number));
  ASSERT_EQ(branch_wal.txns.size(), 1u);
  EXPECT_THAT(branch_wal.txns[0].records,
              ElementsAre("delete_edge:" + Gs(base.e1) + ":T:" + Gs(base.v1) + "->" + Gs(base.v2)));
}

TEST_F(VersioningBranchWalTest, EnumMirrorStaysOutOfTheBranchWal) {
  uint64_t number = 0;
  {
    memgraph::kvstore::KVStore kv{kKvDirectory};
    memgraph::dbms::Database db{MakeConfig(), &kv};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    {
      auto unique = db.UniqueAccess();
      ASSERT_TRUE(unique->CreateEnum("Color", std::vector<std::string>{"red", "green"}).has_value());
      Commit(*unique);
    }
    auto const base = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto const listing = db.version_store()->Get("b");
    ASSERT_TRUE(listing.has_value());
    number = listing->info.id;
    auto ctx = mv::BranchContext::BuildFromFork(*Mem(db), listing->fork_ts, number);
    ASSERT_TRUE(ctx.has_value());
    BranchTxn(**ctx, [&](mv::BranchContext &c) {
      auto base_vertex = BaseVertex(c, base.v1);
      ASSERT_TRUE(c.SetProperty(base_vertex, db.storage()->NameToProperty("p"), ms::PropertyValue(2)).has_value());
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
      auto base_vertex = BaseVertex(*f.ctx, f.base.v1);
      ASSERT_TRUE(f.ctx->SetProperty(base_vertex, p, ms::PropertyValue(9)).has_value());
      ASSERT_TRUE(f.ctx->DeleteBaseEdge(BaseEdge(*f.ctx, f.base.e1, f.base.v1)).has_value());
      f.ctx->EndTransaction();  // no commit: the diff transaction aborts
    }
    // Positive control: the branch WAL is live, so the aborted block above provably wrote nothing.
    BranchTxn(*f.ctx, [&](mv::BranchContext &c) {
      auto base_vertex = BaseVertex(c, f.base.v1);
      ASSERT_TRUE(c.SetProperty(base_vertex, p, ms::PropertyValue(4)).has_value());
    });
    f.ctx.reset();
  }

  auto const txns = ReadWalTxns(BranchWalDir(number));
  ASSERT_EQ(txns.size(), 1u) << "only the committed control transaction is in the WAL";
  EXPECT_THAT(txns[0].records, ElementsAre("set_prop:" + Gs(v1) + ":p=4"));
}
