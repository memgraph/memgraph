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

#include <malloc.h>

#include <atomic>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "versioning/vertex_overlay_store.hpp"

namespace ms = memgraph::storage;
using memgraph::versioning::OverlayState;
using memgraph::versioning::OverlayTransaction;
using memgraph::versioning::OverlayVersion;
using memgraph::versioning::VertexOverlayStore;
using ms::IsolationLevel;
using ms::View;

namespace {

constexpr auto kSI = IsolationLevel::SNAPSHOT_ISOLATION;
constexpr auto kRC = IsolationLevel::READ_COMMITTED;
constexpr auto kRU = IsolationLevel::READ_UNCOMMITTED;

ms::LabelId Label(uint32_t id) { return ms::LabelId::FromUint(id); }

ms::PropertyId Prop(uint32_t id) { return ms::PropertyId::FromUint(id); }

// Stands in for the engine: one clock for start and commit timestamps, the active set GC needs.
struct Env {
  VertexOverlayStore store;
  std::mutex mutex;
  uint64_t clock{1};
  uint64_t next_txn_id{ms::kTransactionInitialId};
  std::multiset<uint64_t> active;

  std::unique_ptr<OverlayTransaction> Begin(IsolationLevel isolation = kSI) {
    std::lock_guard guard{mutex};
    uint64_t const start = clock++;
    active.insert(start);
    return std::make_unique<OverlayTransaction>(start, next_txn_id++, isolation);
  }

  void Release(const OverlayTransaction &txn) {
    std::lock_guard guard{mutex};
    active.erase(active.find(txn.start_ts()));
  }

  // The commit timestamp is taken and stamped under the clock's lock, so no snapshot starts in between.
  uint64_t Commit(OverlayTransaction &txn) {
    std::lock_guard guard{mutex};
    uint64_t const ts = clock++;
    store.Commit(txn, ts);
    active.erase(active.find(txn.start_ts()));
    return ts;
  }

  void Abort(OverlayTransaction &txn) {
    store.Abort(txn);
    Release(txn);
  }

  void Gc() {
    uint64_t oldest = 0;
    uint64_t now = 0;
    {
      std::lock_guard guard{mutex};
      now = clock;
      oldest = active.empty() ? now : *active.begin();
    }
    store.CollectGarbage(oldest, now);
  }

  std::expected<bool, ms::Error> AddLabel(OverlayTransaction &txn, uint64_t gid, uint32_t label) {
    return store.Write(gid, txn, [&](OverlayState &state) {
      state.AddLabel(Label(label));
      return true;
    });
  }

  // Commits a one-label write of its own transaction.
  void CommitLabel(uint64_t gid, uint32_t label) {
    auto txn = Begin();
    ASSERT_TRUE(AddLabel(*txn, gid, label).has_value());
    Commit(*txn);
  }
};

std::vector<uint32_t> Added(const OverlayVersion *version) {
  std::vector<uint32_t> labels;
  for (auto const label : version->added_labels()) labels.push_back(label.AsUint());
  return labels;
}

using Labels = std::vector<uint32_t>;

}  // namespace

TEST(VersioningVertexOverlayStore, SnapshotIsolationSeesOnlyEarlierCommits) {
  Env env;
  auto before = env.Begin();
  auto writer = env.Begin();
  ASSERT_TRUE(env.AddLabel(*writer, 7, 1).has_value());
  auto during = env.Begin();

  EXPECT_EQ(env.store.Read(7, *during, View::NEW), nullptr);  // uncommitted
  ASSERT_NE(env.store.Read(7, *writer, View::NEW), nullptr);
  env.Commit(*writer);

  EXPECT_EQ(env.store.Read(7, *before, View::NEW), nullptr);  // started before the commit
  EXPECT_EQ(env.store.Read(7, *during, View::NEW), nullptr);
  auto after = env.Begin();
  auto const *version = env.store.Read(7, *after, View::NEW);
  ASSERT_NE(version, nullptr);
  EXPECT_EQ(Added(version), (Labels{1}));
  EXPECT_EQ(env.store.Read(8, *after, View::NEW), nullptr);  // no overlay: the fork vertex applies
}

TEST(VersioningVertexOverlayStore, ReadCommittedAndUncommittedIsolation) {
  Env env;
  auto rc = env.Begin(kRC);
  auto ru = env.Begin(kRU);
  auto writer = env.Begin();
  ASSERT_TRUE(env.AddLabel(*writer, 3, 5).has_value());

  EXPECT_EQ(env.store.Read(3, *rc, View::NEW), nullptr);
  ASSERT_NE(env.store.Read(3, *ru, View::NEW), nullptr);
  EXPECT_EQ(Added(env.store.Read(3, *ru, View::NEW)), (Labels{5}));

  env.Commit(*writer);
  auto const *seen = env.store.Read(3, *rc, View::NEW);  // started before the commit, reads the latest committed
  ASSERT_NE(seen, nullptr);
  EXPECT_EQ(Added(seen), (Labels{5}));
}

TEST(VersioningVertexOverlayStore, OwnWritesFollowCommandIdForOldAndNew) {
  Env env;
  env.CommitLabel(1, 9);
  auto txn = env.Begin();

  ASSERT_TRUE(env.AddLabel(*txn, 1, 1).has_value());
  EXPECT_EQ(Added(env.store.Read(1, *txn, View::NEW)), (Labels{1, 9}));
  EXPECT_EQ(Added(env.store.Read(1, *txn, View::OLD)), (Labels{9}));  // this command's write is not yet old

  ASSERT_TRUE(env.AddLabel(*txn, 1, 2).has_value());  // same command: replaces the head
  EXPECT_EQ(env.store.ChainLength(1), 2U);
  EXPECT_EQ(Added(env.store.Read(1, *txn, View::NEW)), (Labels{1, 2, 9}));
  EXPECT_EQ(Added(env.store.Read(1, *txn, View::OLD)), (Labels{9}));

  txn->AdvanceCommand();
  EXPECT_EQ(Added(env.store.Read(1, *txn, View::OLD)), (Labels{1, 2, 9}));
  ASSERT_TRUE(env.AddLabel(*txn, 1, 3).has_value());  // new command: adds a version
  EXPECT_EQ(env.store.ChainLength(1), 3U);
  EXPECT_EQ(Added(env.store.Read(1, *txn, View::NEW)), (Labels{1, 2, 3, 9}));
  EXPECT_EQ(Added(env.store.Read(1, *txn, View::OLD)), (Labels{1, 2, 9}));

  // A mutation that changes nothing writes nothing.
  auto const noop = env.store.Write(1, *txn, [](OverlayState &) { return false; });
  ASSERT_TRUE(noop.has_value());
  EXPECT_FALSE(*noop);
  EXPECT_EQ(env.store.ChainLength(1), 3U);
}

TEST(VersioningVertexOverlayStore, WriteWriteConflictsInBothOrderings) {
  for (auto const isolation : {kSI, kRC}) {
    Env env;
    // The first writer has not committed.
    auto first = env.Begin(isolation);
    auto second = env.Begin(isolation);
    ASSERT_TRUE(env.AddLabel(*first, 1, 1).has_value());
    auto const uncommitted = env.AddLabel(*second, 1, 2);
    ASSERT_FALSE(uncommitted.has_value());
    EXPECT_EQ(uncommitted.error(), ms::Error::SERIALIZATION_ERROR);
    EXPECT_TRUE(env.AddLabel(*second, 2, 2).has_value());  // another vertex is free

    // The first writer committed after the second started; the roles swap.
    env.Commit(*second);
    auto const late = env.AddLabel(*first, 2, 3);
    ASSERT_FALSE(late.has_value());
    EXPECT_EQ(late.error(), ms::Error::SERIALIZATION_ERROR);
    // A no-op still conflicts, as PrepareForWrite runs before anything else.
    EXPECT_FALSE(env.store.Write(2, *first, [](OverlayState &) { return false; }).has_value());
    env.Abort(*first);

    auto third = env.Begin(isolation);
    EXPECT_TRUE(env.AddLabel(*third, 2, 3).has_value());  // started after the commit
  }
}

TEST(VersioningVertexOverlayStore, AbortRestoresPreviousHeads) {
  Env env;
  env.CommitLabel(1, 9);
  auto txn = env.Begin();
  ASSERT_TRUE(env.AddLabel(*txn, 1, 1).has_value());
  txn->AdvanceCommand();
  ASSERT_TRUE(env.AddLabel(*txn, 1, 2).has_value());
  ASSERT_TRUE(env.AddLabel(*txn, 2, 4).has_value());
  EXPECT_EQ(env.store.ChainLength(1), 3U);
  EXPECT_EQ(env.store.size(), 2U);

  env.Abort(*txn);
  EXPECT_EQ(env.store.ChainLength(1), 1U);
  EXPECT_EQ(env.store.ChainLength(2), 0U);  // the chain that held only aborted versions is gone
  EXPECT_EQ(env.store.size(), 1U);
  auto reader = env.Begin();
  EXPECT_EQ(Added(env.store.Read(1, *reader, View::NEW)), (Labels{9}));
  EXPECT_EQ(env.store.Read(2, *reader, View::NEW), nullptr);
  EXPECT_TRUE(env.AddLabel(*reader, 1, 5).has_value());  // no leftover uncommitted head

  env.Release(*reader);
  env.Gc();
  EXPECT_EQ(env.store.live_versions(), 2U);  // the committed head and the reader's own version
}

TEST(VersioningVertexOverlayStore, GcCollapsesChainsBehindPinnedSnapshots) {
  Env env;
  env.CommitLabel(1, 1);
  auto pin_old = env.Begin();
  auto const *v1 = env.store.Read(1, *pin_old, View::NEW);
  ASSERT_NE(v1, nullptr);
  env.CommitLabel(1, 2);
  auto pin_mid = env.Begin();
  env.CommitLabel(1, 3);
  ASSERT_EQ(env.store.ChainLength(1), 3U);

  env.Gc();  // the oldest snapshot still needs the oldest version
  EXPECT_EQ(env.store.ChainLength(1), 3U);

  env.Release(*pin_old);
  env.Gc();  // the first version is unlinked but kept alive: its reader could still be walking it
  EXPECT_EQ(env.store.ChainLength(1), 2U);
  EXPECT_EQ(env.store.live_versions(), 3U);
  EXPECT_EQ(Added(v1), (Labels{1}));
  EXPECT_EQ(Added(env.store.Read(1, *pin_mid, View::NEW)), (Labels{1, 2}));

  env.Release(*pin_mid);
  env.Gc();
  EXPECT_EQ(env.store.ChainLength(1), 1U);
  EXPECT_EQ(env.store.live_versions(), 1U);
  auto reader = env.Begin();
  EXPECT_EQ(Added(env.store.Read(1, *reader, View::NEW)), (Labels{1, 2, 3}));
}

TEST(VersioningVertexOverlayStore, VersionsRoundTripEveryPart) {
  Env env;
  auto txn = env.Begin();
  auto const written = env.store.Write(1, *txn, [](OverlayState &state) {
    for (uint32_t label = 0; label < 300; ++label) state.AddLabel(Label(label));  // forces the extended form
    state.RemoveLabel(Label(7));
    state.SetProperty(Prop(1), ms::PropertyValue(int64_t{42}));
    state.RemoveProperty(Prop(2));
    state.AddDeletedEdge(20);
    state.AddDeletedEdge(3);
    state.AddDeletedEdge(20);
    return true;
  });
  ASSERT_TRUE(written.has_value());
  auto const *version = env.store.Read(1, *txn, View::NEW);
  ASSERT_NE(version, nullptr);
  EXPECT_FALSE(version->deleted());
  EXPECT_EQ(version->added_labels().size(), 299U);
  ASSERT_EQ(version->removed_labels().size(), 1U);
  EXPECT_EQ(version->removed_labels()[0], Label(7));
  EXPECT_EQ(version->properties().GetProperty(Prop(1)).ValueInt(), 42);
  ASSERT_EQ(version->removed_pids().size(), 1U);
  EXPECT_EQ(version->removed_pids()[0], Prop(2));
  EXPECT_EQ(std::vector<uint64_t>(version->deleted_edge_gids().begin(), version->deleted_edge_gids().end()),
            (std::vector<uint64_t>{3, 20}));
  EXPECT_EQ(version->AllocatedBytes(), OverlayVersion::AllocBytes(true, 299, 1, 1, 2));

  txn->AdvanceCommand();
  ASSERT_TRUE(env.store
                  .Write(1,
                         *txn,
                         [](OverlayState &state) {
                           state.MarkDeleted();
                           return true;
                         })
                  .has_value());
  version = env.store.Read(1, *txn, View::NEW);
  ASSERT_NE(version, nullptr);
  EXPECT_TRUE(version->deleted());
  EXPECT_TRUE(version->added_labels().empty());
  EXPECT_TRUE(version->deleted_edge_gids().empty());
  EXPECT_EQ(version->properties().GetProperty(Prop(1)).IsNull(), true);
}

TEST(VersioningVertexOverlayStore, ReportsVersionSizes) {
  Env env;
  auto txn = env.Begin();
  ASSERT_TRUE(env.AddLabel(*txn, 1, 1).has_value());
  ASSERT_TRUE(env.store
                  .Write(2,
                         *txn,
                         [](OverlayState &state) {
                           state.SetProperty(Prop(1), ms::PropertyValue(int64_t{1}));
                           return true;
                         })
                  .has_value());
  ASSERT_TRUE(env.store
                  .Write(3,
                         *txn,
                         [](OverlayState &state) {
                           state.AddLabel(Label(1));
                           state.AddLabel(Label(2));
                           state.AddLabel(Label(3));
                           return true;
                         })
                  .has_value());
  auto const usable = [&](uint64_t gid) {
    auto const *version = env.store.Read(gid, *txn, View::NEW);
    return malloc_usable_size(const_cast<OverlayVersion *>(version));
  };
  std::cout << "sizeof(OverlayVersion)=" << sizeof(OverlayVersion)
            << " single-label alloc=" << env.store.Read(1, *txn, View::NEW)->AllocatedBytes() << " (size class "
            << usable(1) << "), single-property alloc=" << env.store.Read(2, *txn, View::NEW)->AllocatedBytes()
            << " (size class " << usable(2)
            << "), three-label alloc=" << env.store.Read(3, *txn, View::NEW)->AllocatedBytes() << " (size class "
            << usable(3) << ")\n";

  EXPECT_EQ(sizeof(OverlayVersion), 40U);
  EXPECT_EQ(env.store.Read(1, *txn, View::NEW)->AllocatedBytes(), 40U);
  EXPECT_EQ(env.store.Read(2, *txn, View::NEW)->AllocatedBytes(), 40U);
  EXPECT_LE(usable(1), 48U);
  EXPECT_LE(usable(2), 48U);
  EXPECT_LE(usable(3), 48U);
}

// Writers set two properties to one value in a single write; every version a reader finds must hold equal values, an
// SI reader must see the same version on every read, and a GC thread frees concurrently. Run under TSan for races.
TEST(VersioningVertexOverlayStore, ConcurrentWritersReadersAndGc) {
  constexpr uint64_t kGids = 8;
  constexpr int kWriters = 2;
  constexpr int kReaders = 2;
  constexpr int kWritesPerWriter = 1500;

  Env env;
  std::atomic<int> writers_left{kWriters};
  std::atomic<bool> torn{false};
  std::atomic<bool> unstable{false};

  auto check = [&](const OverlayVersion *version) {
    if (version == nullptr) return;
    if (version->properties().GetProperty(Prop(1)).ValueInt() !=
        version->properties().GetProperty(Prop(2)).ValueInt()) {
      torn = true;
    }
  };

  std::vector<std::thread> threads;
  for (int w = 0; w < kWriters; ++w) {
    threads.emplace_back([&, w] {
      uint64_t value = static_cast<uint64_t>(w) << 32;
      for (int i = 0; i < kWritesPerWriter; ++i) {
        auto txn = env.Begin(i % 2 == 0 ? kSI : kRC);
        auto const gid = static_cast<uint64_t>(i * 7 + w) % kGids;
        ++value;
        auto const result = env.store.Write(gid, *txn, [&](OverlayState &state) {
          state.SetProperty(Prop(1), ms::PropertyValue(static_cast<int64_t>(value)));
          state.SetProperty(Prop(2), ms::PropertyValue(static_cast<int64_t>(value)));
          return true;
        });
        if (result.has_value() && i % 8 != 0) {
          env.Commit(*txn);
        } else {
          env.Abort(*txn);
        }
      }
      --writers_left;
    });
  }
  for (int r = 0; r < kReaders; ++r) {
    threads.emplace_back([&, r] {
      while (writers_left.load() > 0) {
        auto txn = env.Begin(r == 0 ? kSI : kRC);
        for (uint64_t gid = 0; gid < kGids; ++gid) {
          auto const *first = env.store.Read(gid, *txn, View::NEW);
          check(first);
          auto const *second = env.store.Read(gid, *txn, View::NEW);
          check(second);
          if (r == 0 && first != second) unstable = true;
        }
        env.Release(*txn);
      }
    });
  }
  threads.emplace_back([&] {
    while (writers_left.load() > 0) {
      env.Gc();
      std::this_thread::yield();
    }
  });
  for (auto &thread : threads) thread.join();

  EXPECT_FALSE(torn.load());
  EXPECT_FALSE(unstable.load());
  env.Gc();
  for (uint64_t gid = 0; gid < kGids; ++gid) EXPECT_LE(env.store.ChainLength(gid), 1U);
  EXPECT_EQ(env.store.live_versions(), env.store.size());
}
