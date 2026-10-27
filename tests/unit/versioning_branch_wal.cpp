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

// A committed branch transaction is durable in the branch's own diff-engine WAL: native-object deltas plus one
// OVERLAY_PUT per overlay key written, inside the same WAL transaction. The WAL files are decoded and checked here.
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "dbms/database.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/scheduler.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/version_store.hpp"
#include "versioning/vertex_overlay_store.hpp"

import memgraph.storage.property_value;

namespace ms = memgraph::storage;
namespace dur = memgraph::storage::durability;
namespace mv = memgraph::versioning;

namespace {
const std::filesystem::path kStorageDirectory{std::filesystem::temp_directory_path() / "versioning_branch_wal"};

ms::Config MakeConfig(bool wal = true) {
  return ms::Config{
      .durability = {.storage_directory = kStorageDirectory,
                     .recover_on_startup = false,
                     .snapshot_wal_mode = wal ? ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL
                                              : ms::Config::Durability::SnapshotWalMode::DISABLED,
                     .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)}},
  };
}

auto *Mem(memgraph::dbms::Database &db) { return static_cast<ms::InMemoryStorage *>(db.storage()); }

// One WAL transaction: everything between its start and end markers.
struct WalTxn {
  uint64_t timestamp;
  std::vector<dur::WalDeltaData> deltas;

  template <typename T>
  std::vector<T> Of() const {
    std::vector<T> out;
    for (auto const &delta : deltas) {
      if (auto const *typed = std::get_if<T>(&delta.data_)) out.push_back(*typed);
    }
    return out;
  }

  bool EndsWithOverlayPut() const {
    return !deltas.empty() && std::holds_alternative<dur::WalOverlayPut>(deltas.back().data_);
  }
};

// All transactions of the WAL files in `wal_dir`, oldest file first. Empty when the directory does not exist. Call only
// once the owning storage is gone: a live storage may still hold its newest file open.
std::vector<WalTxn> ReadWalTxns(const std::filesystem::path &wal_dir) {
  std::vector<WalTxn> txns;
  if (!std::filesystem::exists(wal_dir)) return txns;
  std::vector<std::filesystem::path> files;
  for (auto const &entry : std::filesystem::directory_iterator(wal_dir)) {
    if (entry.is_regular_file()) files.push_back(entry.path());
  }
  std::vector<std::pair<uint64_t, std::filesystem::path>> ordered;
  for (auto const &file : files) ordered.emplace_back(dur::ReadWalInfo(file).from_timestamp, file);
  std::ranges::sort(ordered);

  for (auto const &[from_ts, file] : ordered) {
    auto const info = dur::ReadWalInfo(file);
    dur::Decoder wal;
    wal.Initialize(file, dur::kWalMagic);
    wal.SetPosition(info.offset_deltas);
    std::optional<WalTxn> open;
    for (uint64_t i = 0; i < info.num_deltas; ++i) {
      auto const timestamp = dur::ReadWalDeltaHeader(&wal);
      auto delta = dur::ReadWalDeltaData(&wal);
      if (std::holds_alternative<dur::WalTransactionStart>(delta.data_)) {
        EXPECT_FALSE(open.has_value()) << "a transaction started inside another";
        open = WalTxn{.timestamp = timestamp, .deltas = {}};
      } else if (std::holds_alternative<dur::WalTransactionEnd>(delta.data_)) {
        EXPECT_TRUE(open.has_value()) << "a transaction ended without starting";
        if (open) txns.push_back(std::move(*open));
        open.reset();
      } else if (open) {
        open->deltas.push_back(std::move(delta));
      }
    }
    EXPECT_FALSE(open.has_value()) << "the WAL ends inside a transaction";
  }
  return txns;
}

size_t CountOverlayPuts(const std::vector<WalTxn> &txns) {
  size_t count = 0;
  for (auto const &txn : txns) count += txn.Of<dur::WalOverlayPut>().size();
  return count;
}

std::filesystem::path BranchWalDir(uint64_t number) {
  return kStorageDirectory / "branches" / std::to_string(number) / "wal";
}

dur::WalOverlayPut Put(uint64_t key, bool deleted = false) {
  return dur::WalOverlayPut{.key = key,
                            .deleted = deleted,
                            .added_labels = {},
                            .removed_labels = {},
                            .removed_properties = {},
                            .properties = {},
                            .from_gid = 0,
                            .to_gid = 0};
}

ms::ExternalPropertyValue Int(int64_t value) { return ms::ExternalPropertyValue(value); }

void Commit(ms::Storage::Accessor &acc) {
  ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// v1(:L {p: 1}) -e1[:T {w: 1}]-> v2, and an isolated v3.
struct Base {
  ms::Gid v1, v2, v3, e1;
};

Base CreateBase(memgraph::dbms::Database &db) {
  auto *storage = db.storage();
  auto acc = db.Access(ms::WRITE);
  auto v1 = acc->CreateVertex();
  auto v2 = acc->CreateVertex();
  auto v3 = acc->CreateVertex();
  EXPECT_TRUE(v1.AddLabel(storage->NameToLabel("L")).has_value());
  EXPECT_TRUE(v1.SetProperty(storage->NameToProperty("p"), ms::PropertyValue(1)).has_value());
  auto e1 = acc->CreateEdge(&v1, &v2, storage->NameToEdgeType("T"));
  EXPECT_TRUE(e1.has_value());
  EXPECT_TRUE(e1->SetProperty(storage->NameToProperty("w"), ms::PropertyValue(1)).has_value());
  Base base{v1.Gid(), v2.Gid(), v3.Gid(), e1->Gid()};
  Commit(*acc);
  return base;
}

class VersioningBranchWalTest : public ::testing::Test {
 protected:
  void SetUp() override { std::filesystem::remove_all(kStorageDirectory); }

  void TearDown() override { std::filesystem::remove_all(kStorageDirectory); }
};

// Runs `body` as one branch transaction that commits; the context is destroyed (WAL finalized) by the caller.
template <typename Body>
void BranchTxn(mv::BranchContext &ctx, Body &&body) {
  auto diff_acc = ctx.diff_engine().Access(ms::WRITE);
  ASSERT_TRUE(ctx.BeginTransaction(diff_acc.get()).has_value());
  body(ctx);
  ASSERT_TRUE(ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
  ctx.EndTransaction();
}
}  // namespace

TEST_F(VersioningBranchWalTest, MixedTransactionIsOneWalTransactionWithNativeDeltasAndOverlayPut) {
  uint64_t number = 0;
  Base base{};
  {
    memgraph::dbms::Database db{MakeConfig()};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b", "main", std::nullopt);
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = mv::BranchContext::BuildFromFork(*Mem(db), info->fork_ts, info->number);
    ASSERT_TRUE(ctx.has_value());
    const auto p = db.storage()->NameToProperty("p");
    const auto q = db.storage()->NameToProperty("q");

    BranchTxn(**ctx, [&](mv::BranchContext &c) {
      auto native = c.CreateNativeVertex();
      ASSERT_TRUE(native.SetProperty(q, ms::PropertyValue(7)).has_value());
      ASSERT_TRUE(c.PrepareForkWrite(base.v1).has_value());
      ASSERT_TRUE(c.OverlaySetProperty(base.v1, p, ms::PropertyValue(2)).has_value());
    });
    ctx->reset();
  }

  auto const txns = ReadWalTxns(BranchWalDir(number));
  ASSERT_EQ(txns.size(), 1u) << "native deltas and the overlay record share one WAL transaction";
  EXPECT_EQ(txns[0].Of<dur::WalVertexCreate>().size(), 1u);
  EXPECT_EQ(txns[0].Of<dur::WalVertexSetProperty>().size(), 1u);
  ASSERT_TRUE(txns[0].EndsWithOverlayPut()) << "the overlay record follows the deltas, before the transaction end";
  auto expected = Put(base.v1.AsUint());
  expected.properties.emplace_back("p", Int(2));
  EXPECT_THAT(txns[0].Of<dur::WalOverlayPut>(), ::testing::ElementsAre(expected));

  auto const main_txns = ReadWalTxns(kStorageDirectory / "wal");
  EXPECT_FALSE(main_txns.empty()) << "main's own commits are in its WAL";
  EXPECT_EQ(CountOverlayPuts(main_txns), 0u);
}

TEST_F(VersioningBranchWalTest, OverlayOnlyTransactionWritesItsOwnWalTransaction) {
  uint64_t number = 0;
  Base base{};
  {
    memgraph::dbms::Database db{MakeConfig()};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b", "main", std::nullopt);
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = mv::BranchContext::BuildFromFork(*Mem(db), info->fork_ts, info->number);
    ASSERT_TRUE(ctx.has_value());
    const auto p = db.storage()->NameToProperty("p");
    const auto fork_label = db.storage()->NameToLabel("L");
    const auto new_label = db.storage()->NameToLabel("NewL");

    BranchTxn(**ctx, [&](mv::BranchContext &c) {
      ASSERT_TRUE(c.PrepareForkWrite(base.v1).has_value());
      ASSERT_TRUE(c.OverlayAddLabel(base.v1, new_label).has_value());
      ASSERT_TRUE(c.OverlayRemoveLabel(base.v1, fork_label).has_value());
      ASSERT_TRUE(c.OverlaySetProperty(base.v1, p, ms::PropertyValue(3)).has_value());
    });
    BranchTxn(**ctx, [&](mv::BranchContext &c) {
      ASSERT_TRUE(c.PrepareForkWrite(base.v1).has_value());
      ASSERT_TRUE(c.OverlaySetProperty(base.v1, p, ms::PropertyValue()).has_value());
    });
    ctx->reset();
  }

  auto const txns = ReadWalTxns(BranchWalDir(number));
  ASSERT_EQ(txns.size(), 2u);
  EXPECT_LT(txns[0].timestamp, txns[1].timestamp) << "WAL order is commit order";

  auto first = Put(base.v1.AsUint());
  first.added_labels = {"NewL"};
  first.removed_labels = {"L"};
  first.properties.emplace_back("p", Int(3));
  EXPECT_THAT(txns[0].Of<dur::WalOverlayPut>(), ::testing::ElementsAre(first));
  EXPECT_EQ(txns[0].deltas.size(), 1u) << "no native delta in an overlay-only transaction";

  auto second = Put(base.v1.AsUint());
  second.added_labels = {"NewL"};
  second.removed_labels = {"L"};
  second.removed_properties = {"p"};
  EXPECT_THAT(txns[1].Of<dur::WalOverlayPut>(), ::testing::ElementsAre(second))
      << "the record is the full overlay state, not the delta of the last statement";
  EXPECT_EQ(txns[1].deltas.size(), 1u);
}

TEST_F(VersioningBranchWalTest, ForkEdgePropertyWriteCarriesTheEdgeBitAndEndpoints) {
  uint64_t number = 0;
  Base base{};
  {
    memgraph::dbms::Database db{MakeConfig()};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b", "main", std::nullopt);
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = mv::BranchContext::BuildFromFork(*Mem(db), info->fork_ts, info->number);
    ASSERT_TRUE(ctx.has_value());
    const auto w = db.storage()->NameToProperty("w");

    BranchTxn(**ctx, [&](mv::BranchContext &c) {
      auto from = c.historical().FindVertex(base.v1, ms::View::OLD);
      ASSERT_TRUE(from.has_value());
      auto out = from->OutEdges(ms::View::OLD);
      ASSERT_TRUE(out.has_value());
      ASSERT_EQ(out->edges.size(), 1u);
      auto const fork_edge = out->edges[0];
      ASSERT_TRUE(c.PrepareForkEdgeWrite(fork_edge).has_value());
      ASSERT_TRUE(c.EdgeOverlaySetProperty(fork_edge, w, ms::PropertyValue(5)).has_value());
    });
    ctx->reset();
  }

  auto const txns = ReadWalTxns(BranchWalDir(number));
  ASSERT_EQ(txns.size(), 1u);
  auto expected = Put(mv::EdgeKey(base.e1.AsUint()));
  expected.properties.emplace_back("w", Int(5));
  expected.from_gid = base.v1.AsUint();
  expected.to_gid = base.v2.AsUint();
  EXPECT_THAT(txns[0].Of<dur::WalOverlayPut>(), ::testing::ElementsAre(expected));
}

TEST_F(VersioningBranchWalTest, ForkVertexDeleteWritesADeletedOverlayPut) {
  uint64_t number = 0;
  Base base{};
  {
    memgraph::dbms::Database db{MakeConfig()};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b", "main", std::nullopt);
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = mv::BranchContext::BuildFromFork(*Mem(db), info->fork_ts, info->number);
    ASSERT_TRUE(ctx.has_value());

    BranchTxn(**ctx, [&](mv::BranchContext &c) {
      auto deleted = c.OverlayMarkDeleted(base.v3);
      ASSERT_TRUE(deleted.has_value());
      ASSERT_TRUE(*deleted);
    });
    ctx->reset();
  }

  auto const txns = ReadWalTxns(BranchWalDir(number));
  ASSERT_EQ(txns.size(), 1u);
  EXPECT_THAT(txns[0].Of<dur::WalOverlayPut>(), ::testing::ElementsAre(Put(base.v3.AsUint(), /*deleted=*/true)));
}

TEST_F(VersioningBranchWalTest, RolledBackTransactionWritesNothing) {
  uint64_t number = 0;
  {
    memgraph::dbms::Database db{MakeConfig()};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto const base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b", "main", std::nullopt);
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = mv::BranchContext::BuildFromFork(*Mem(db), info->fork_ts, info->number);
    ASSERT_TRUE(ctx.has_value());
    const auto p = db.storage()->NameToProperty("p");

    {
      auto diff_acc = (*ctx)->diff_engine().Access(ms::WRITE);
      ASSERT_TRUE((*ctx)->BeginTransaction(diff_acc.get()).has_value());
      static_cast<void>((*ctx)->CreateNativeVertex());
      ASSERT_TRUE((*ctx)->PrepareForkWrite(base.v1).has_value());
      ASSERT_TRUE((*ctx)->OverlaySetProperty(base.v1, p, ms::PropertyValue(9)).has_value());
      (*ctx)->EndTransaction();  // no commit: the overlay and the diff transaction abort
    }
    {
      auto diff_acc = (*ctx)->diff_engine().Access(ms::WRITE);
      ASSERT_TRUE((*ctx)->BeginTransaction(diff_acc.get()).has_value());
      ASSERT_TRUE((*ctx)->PrepareForkWrite(base.v1).has_value());
      ASSERT_TRUE((*ctx)->OverlaySetProperty(base.v1, p, ms::PropertyValue(9)).has_value());
      (*ctx)->EndTransaction();
    }
    // Positive control: the branch WAL is live, so the aborted blocks above provably wrote nothing.
    BranchTxn(**ctx, [&](mv::BranchContext &c) {
      ASSERT_TRUE(c.PrepareForkWrite(base.v1).has_value());
      ASSERT_TRUE(c.OverlaySetProperty(base.v1, p, ms::PropertyValue(4)).has_value());
    });
    ctx->reset();
  }

  EXPECT_EQ(ReadWalTxns(BranchWalDir(number)).size(), 1u) << "only the committed control transaction is in the WAL";
}

TEST_F(VersioningBranchWalTest, BranchOfAMainWithoutWalWritesNoBranchWal) {
  constexpr uint64_t kNumber = 7;
  {
    memgraph::dbms::Database db{MakeConfig(/*wal=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto const base = CreateBase(db);
    auto *storage = Mem(db);
    auto const fork_ts = storage->RegisterForkPin();
    {
      auto ctx = mv::BranchContext::BuildFromFork(*storage, fork_ts, kNumber);
      ASSERT_TRUE(ctx.has_value());
      const auto p = db.storage()->NameToProperty("p");
      BranchTxn(**ctx, [&](mv::BranchContext &c) {
        static_cast<void>(c.CreateNativeVertex());
        ASSERT_TRUE(c.PrepareForkWrite(base.v1).has_value());
        ASSERT_TRUE(c.OverlaySetProperty(base.v1, p, ms::PropertyValue(2)).has_value());
      });
      BranchTxn(**ctx, [&](mv::BranchContext &c) {
        ASSERT_TRUE(c.PrepareForkWrite(base.v1).has_value());
        ASSERT_TRUE(c.OverlaySetProperty(base.v1, p, ms::PropertyValue(3)).has_value());
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
