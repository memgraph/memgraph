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

#include <atomic>
#include <cstdint>
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
  // An OverlayTransaction with writes must end in Commit or Abort; rolls back what a test leaves open.
  struct TxnEnd {
    Env *env;

    void operator()(OverlayTransaction *txn) const {
      if (txn->has_writes() && !txn->finished()) env->store.Abort(*txn);
      delete txn;
    }
  };

  using TxnPtr = std::unique_ptr<OverlayTransaction, TxnEnd>;

  VertexOverlayStore store;
  std::mutex mutex;
  uint64_t clock{1};
  uint64_t next_txn_id{ms::kTransactionInitialId};
  std::multiset<uint64_t> active;

  TxnPtr Begin(IsolationLevel isolation = kSI) {
    std::lock_guard guard{mutex};
    uint64_t const start = clock++;
    active.insert(start);
    return TxnPtr{new OverlayTransaction(start, next_txn_id++, isolation), TxnEnd{this}};
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

  VertexOverlayStore::Horizon Sample() {
    std::lock_guard guard{mutex};
    return {active.empty() ? clock : *active.begin(), clock};
  }

  void Gc() {
    store.CollectGarbage([this] { return Sample(); });
  }

  std::expected<bool, ms::Error> AddLabel(OverlayTransaction &txn, uint64_t gid, uint32_t label) {
    return store.Write(gid, txn, [&](OverlayState &state) {
      state.AddLabel(Label(label));
      return true;
    });
  }

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

  auto const noop = env.store.Write(1, *txn, [](OverlayState &) { return false; });
  ASSERT_TRUE(noop.has_value());
  EXPECT_FALSE(*noop);
  EXPECT_EQ(env.store.ChainLength(1), 3U);
  env.Abort(*txn);
}

TEST(VersioningVertexOverlayStore, WriteWriteConflictsInBothOrderings) {
  for (auto const isolation : {kSI, kRC}) {
    Env env;
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
    env.Abort(*third);
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

  env.Abort(*reader);
  env.Gc();
  EXPECT_EQ(env.store.live_versions(), 1U);
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

// Between GC's two samples a writer aborts and a reader begins (start 4, clock now 5). The unlinked version must be
// marked with the second sample (5 > oldest active 4, so it survives); the first (3 <= 4) would free it under the
// reader.
TEST(VersioningVertexOverlayStore, GcMarksGarbageWithAClockSampledAfterTheUnlink) {
  Env env;
  env.CommitLabel(1, 1);
  Env::TxnPtr writer;
  Env::TxnPtr reader;
  OverlayVersion const *held = nullptr;
  int calls = 0;

  env.store.CollectGarbage([&] {
    auto const horizon = env.Sample();  // nothing active: oldest == now
    if (calls++ == 0) {
      writer = env.Begin();
      EXPECT_TRUE(env.AddLabel(*writer, 1, 2).has_value());
      reader = env.Begin(kRU);
      held = env.store.Read(1, *reader, View::NEW);
      env.Abort(*writer);
    }
    return horizon;
  });
  ASSERT_EQ(calls, 2);
  ASSERT_NE(held, nullptr);
  EXPECT_EQ(Added(held), (Labels{1, 2}));
  EXPECT_EQ(env.store.live_versions(), 2U);

  env.Release(*reader);
  env.Gc();
  EXPECT_EQ(env.store.live_versions(), 1U);
}

TEST(VersioningVertexOverlayStore, VersionsRoundTripEveryPart) {
  Env env;
  auto txn = env.Begin();
  auto const written = env.store.Write(1, *txn, [](OverlayState &state) {
    for (uint32_t label = 0; label < 300; ++label) state.AddLabel(Label(label));  // forces the extended form
    state.RemoveLabel(Label(7));
    state.SetProperty(Prop(1), ms::PropertyValue(int64_t{42}));
    state.RemoveProperty(Prop(2));
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
  EXPECT_EQ(version->AllocatedBytes(), 1252U);  // 48 B extended header + 300 label ids + 1 property id, 4 B each

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
  EXPECT_EQ(version->properties().GetProperty(Prop(1)).IsNull(), true);
}

TEST(VersioningVertexOverlayStore, NormalFormVersionsRoundTripEveryPart) {
  Env env;
  auto txn = env.Begin();
  ASSERT_TRUE(env.store
                  .Write(1,
                         *txn,
                         [](OverlayState &state) {
                           state.AddLabel(Label(1));
                           state.AddLabel(Label(2));
                           state.AddLabel(Label(3));
                           state.RemoveLabel(Label(7));
                           state.SetProperty(Prop(1), ms::PropertyValue(int64_t{42}));
                           state.RemoveProperty(Prop(2));
                           return true;
                         })
                  .has_value());
  auto const *version = env.store.Read(1, *txn, View::NEW);
  ASSERT_NE(version, nullptr);
  EXPECT_FALSE(version->deleted());
  EXPECT_EQ(Added(version), (Labels{1, 2, 3}));
  ASSERT_EQ(version->removed_labels().size(), 1U);
  EXPECT_EQ(version->removed_labels()[0], Label(7));
  EXPECT_EQ(version->properties().GetProperty(Prop(1)).ValueInt(), 42);
  ASSERT_EQ(version->removed_pids().size(), 1U);
  EXPECT_EQ(version->removed_pids()[0], Prop(2));
  EXPECT_EQ(version->AllocatedBytes(), 56U);  // 36 B tail start + 3 + 1 label ids + 1 property id, 4 B each
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
  EXPECT_EQ(env.store.Read(1, *txn, View::NEW)->AllocatedBytes(), 40U);
  EXPECT_EQ(env.store.Read(2, *txn, View::NEW)->AllocatedBytes(), 40U);
  EXPECT_EQ(env.store.Read(3, *txn, View::NEW)->AllocatedBytes(), 48U);  // 36 B tail start + 3 label ids
}

// A fork edge is keyed by EdgeKey: it is its own conflict domain next to the vertex with the same gid, and one
// transaction commits or aborts both together.
TEST(VersioningVertexOverlayStore, EdgeKeysAreSeparateConflictDomainsUnderOneTransaction) {
  using memgraph::versioning::EdgeKey;
  using memgraph::versioning::IsEdgeKey;
  Env env;
  auto const set_prop = [&](uint64_t key, OverlayTransaction &txn, int64_t value) {
    return env.store.Write(key, txn, [&](OverlayState &state) {
      state.SetProperty(Prop(1), ms::PropertyValue(value));
      return true;
    });
  };
  EXPECT_TRUE(IsEdgeKey(EdgeKey(5)));
  EXPECT_FALSE(IsEdgeKey(5));

  auto a = env.Begin();
  auto b = env.Begin();
  ASSERT_TRUE(set_prop(EdgeKey(5), *a, 1).has_value());
  ASSERT_TRUE(env.AddLabel(*a, 5, 1).has_value());
  auto const conflicted = set_prop(5, *b, 2);
  ASSERT_FALSE(conflicted.has_value());
  EXPECT_EQ(conflicted.error(), ms::Error::SERIALIZATION_ERROR);
  EXPECT_FALSE(env.store.CanWrite(EdgeKey(5), *b));
  EXPECT_TRUE(env.store.CanWrite(EdgeKey(6), *b));
  EXPECT_EQ(set_prop(EdgeKey(5), *b, 3).error(), ms::Error::SERIALIZATION_ERROR);

  env.Abort(*a);
  EXPECT_EQ(env.store.size(), 0U) << "one abort rolls back the vertex and the edge version";
  EXPECT_TRUE(env.store.CanWrite(EdgeKey(5), *b));

  ASSERT_TRUE(set_prop(EdgeKey(5), *b, 3).has_value());
  ASSERT_TRUE(env.AddLabel(*b, 5, 2).has_value());
  EXPECT_EQ(b->write_count(), 2U);
  auto early = env.Begin();
  env.Commit(*b);
  auto reader = env.Begin();
  ASSERT_NE(env.store.Read(EdgeKey(5), *reader, View::NEW), nullptr);
  EXPECT_EQ(env.store.Read(EdgeKey(5), *reader, View::NEW)->properties().GetProperty(Prop(1)).ValueInt(), 3);
  EXPECT_TRUE(env.store.Read(5, *reader, View::NEW)->properties().GetProperty(Prop(1)).IsNull());
  EXPECT_FALSE(env.store.CanWrite(EdgeKey(5), *early)) << "committed after `early` started";
  EXPECT_TRUE(env.store.CanWrite(EdgeKey(5), *reader));
  env.Release(*early);
  env.Release(*reader);
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
  std::atomic<uint64_t> gc_cycles{0};
  std::atomic<uint64_t> verified_held{0};

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
        auto const write = [&] {
          ++value;
          return env.store.Write(gid, *txn, [&](OverlayState &state) {
            state.SetProperty(Prop(1), ms::PropertyValue(static_cast<int64_t>(value)));
            state.SetProperty(Prop(2), ms::PropertyValue(static_cast<int64_t>(value)));
            return true;
          });
        };
        bool ok = write().has_value();
        if (ok) {
          static_cast<void>(write());  // same command: replaces the head
          txn->AdvanceCommand();
          ok = write().has_value();  // later command: a second version
        }
        if (ok && i % 4 != 0) {
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
        // Hold a pointer across a GC cycle, then verify it still reads whole.
        auto const *held = env.store.Read(static_cast<uint64_t>(r), *txn, View::NEW);
        auto const before = held == nullptr ? int64_t{-1} : held->properties().GetProperty(Prop(1)).ValueInt();
        for (uint64_t const seen = gc_cycles.load(); writers_left.load() > 0 && gc_cycles.load() < seen + 2;) {
          std::this_thread::yield();
        }
        check(held);
        if (held != nullptr) {
          ++verified_held;
          if (held->properties().GetProperty(Prop(1)).ValueInt() != before) unstable = true;
        }
        env.Release(*txn);
      }
    });
  }
  threads.emplace_back([&] {
    while (writers_left.load() > 0) {
      env.Gc();
      ++gc_cycles;
      std::this_thread::yield();
    }
  });
  for (auto &thread : threads) thread.join();

  EXPECT_FALSE(torn.load());
  EXPECT_FALSE(unstable.load());
  EXPECT_GT(verified_held.load(), 0U) << "no reader ever held a non-null version across a GC cycle";
  EXPECT_GT(env.store.size(), 0U);
  env.Gc();
  for (uint64_t gid = 0; gid < kGids; ++gid) EXPECT_LE(env.store.ChainLength(gid), 1U);
  EXPECT_EQ(env.store.live_versions(), env.store.size());
}
