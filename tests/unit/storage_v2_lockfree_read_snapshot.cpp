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

// Phase 2, batch 1 of the lock-free-read-snapshot suite: concern-A (read visibility).
// A deterministic interleaving harness parks a commit inside the CommitProbe seam so a
// concurrent reader can be opened at a precise instant and its snapshot boundary asserted.

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <semaphore>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "storage/v2/commit_probe.hpp"
#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage_error.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/resource_lock.hpp"

using memgraph::storage::Config;
using memgraph::storage::ConstraintViolation;
using memgraph::storage::Gid;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::LabelId;
using memgraph::storage::PropertyValue;
using memgraph::storage::UniqueConstraints;
using memgraph::storage::View;
using Accessor = memgraph::storage::Storage::Accessor;

namespace {

std::unique_ptr<InMemoryStorage> MakeStorage(bool flag_on) {
  Config config{};
  // Logical visibility tests only: disable GC so nothing reclaims deltas underneath us.
  config.gc.type = Config::Gc::Type::NONE;
  // Isolation left at the default (SNAPSHOT_ISOLATION).
  config.experimental_lockfree_read_snapshot = flag_on;
  return std::make_unique<InMemoryStorage>(config);
}

Gid CreateVertexWithProp(InMemoryStorage &store, int value) {
  auto acc = store.Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();
  const auto gid = vertex.Gid();
  auto set = vertex.SetProperty(store.NameToProperty("p"), PropertyValue(value));
  EXPECT_TRUE(set.has_value());
  EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  return gid;
}

// Reads property "p" of vertex `gid` at the accessor's frozen snapshot (View::OLD).
int64_t ReadProp(Accessor &acc, Gid gid) {
  auto vertex = acc.FindVertex(gid, View::OLD);
  EXPECT_TRUE(vertex.has_value());
  auto value = vertex->GetProperty(acc.NameToProperty("p"), View::OLD);
  EXPECT_TRUE(value.has_value());
  return value->ValueInt();
}

// Concern-D (GC visibility horizon) storage: PERIODIC GC with a 3600s interval so the timer never
// auto-fires; every collection in these tests is driven manually via RunGc. Default SNAPSHOT
// isolation; the flag selects which horizon GC computes.
std::unique_ptr<InMemoryStorage> MakeStorageManualGc(bool flag_on) {
  Config config{};
  config.gc.type = Config::Gc::Type::PERIODIC;
  config.gc.interval = std::chrono::seconds(3600);
  config.experimental_lockfree_read_snapshot = flag_on;
  return std::make_unique<InMemoryStorage>(config);
}

// Forces a synchronous GC pass. Passes an EMPTY guard (not the UNIQUE-adopt idiom in
// storage_v2_gc.cpp): those tests reset every accessor before GC, whereas this batch keeps a reader
// open across the pass. An open accessor holds main_lock_ SHARED for its lifetime, so an adopted
// UNIQUE hold would deadlock. The empty guard makes CollectGarbage take its own READ (shared) hold
// -- the exact concurrent-GC path production's periodic scheduler uses (storage.cpp: FreeMemory({},
// true)). FreeMemory -> free_memory_func_ -> CollectGarbage, where the visibility horizon is
// computed and deltas are unlinked; the horizon logic is identical regardless of the hold mode.
void RunGc(InMemoryStorage &s) { s.FreeMemory({}, false); }

// Re-finds vertex `gid` in a fresh WRITE accessor, overwrites "p", and publishes -- appending a new
// committed version to the delta chain (the update-then-commit pattern from storage_v2.cpp).
void CommitProp(InMemoryStorage &store, Gid gid, int value) {
  auto acc = store.Access(memgraph::storage::WRITE);
  auto vertex = acc->FindVertex(gid, View::OLD);
  ASSERT_TRUE(vertex.has_value());
  ASSERT_TRUE(vertex->SetProperty(store.NameToProperty("p"), PropertyValue(value)).has_value());
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// Concern-E (abort semantics) helpers. Installs a UNIQUE(label, "p") constraint so a later duplicate
// commit fails the UniqueConstraintsViolation gate -- the abort-after-mint path under test.
void CreateUniquePConstraint(InMemoryStorage &store, LabelId label) {
  auto acc = store.ReadOnlyAccess();
  auto res = acc->CreateUniqueConstraint(label, {store.NameToProperty("p")});
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
  EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// Commits a fresh vertex carrying `label` and p=`value`; returns its gid. Seeds a row a later
// duplicate must collide with under the UNIQUE constraint.
Gid CommitLabeledVertex(InMemoryStorage &store, LabelId label, int value) {
  auto acc = store.Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();
  const auto gid = vertex.Gid();
  EXPECT_TRUE(vertex.AddLabel(label).has_value());
  EXPECT_TRUE(vertex.SetProperty(store.NameToProperty("p"), PropertyValue(value)).has_value());
  EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  return gid;
}

// Number of vertices visible at the accessor's frozen snapshot. An aborted duplicate that leaked
// into a reader's view would push this above the committed count.
int64_t CountVertices(Accessor &acc) {
  int64_t n = 0;
  for (auto vertex : acc.Vertices(View::OLD)) {
    (void)vertex;
    ++n;
  }
  return n;
}

}  // namespace

// Reader opened while a commit is parked between mint and publish must see the pre-commit
// value: the in-flight delta still carries the committing transaction's id marker and the
// visibility watermark has not advanced past it.
TEST(LockFreeReadSnapshot, ReaderBeginsBeforePublish_SeesPreCommitValue) {
  auto store = MakeStorage(/*flag_on=*/true);
  const auto gid = CreateVertexWithProp(*store, 1);

  std::binary_semaphore reached{0};
  std::binary_semaphore resume{0};
  std::optional<bool> commit_ok;

  std::thread committer([&] {
    memgraph::storage::CommitProbe probe;
    probe.before_publish = [&] {
      reached.release();
      resume.acquire();
    };
    store->SetCommitProbe(&probe);

    auto acc = store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(vertex->SetProperty(store->NameToProperty("p"), PropertyValue(2)).has_value());
    commit_ok = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value();
  });

  // Commit is now parked at before_publish: minted, engine_lock released, not yet published.
  reached.acquire();
  {
    auto reader = store->Access(memgraph::storage::READ);
    EXPECT_EQ(ReadProp(*reader, gid), 1);
  }
  resume.release();
  committer.join();

  ASSERT_TRUE(commit_ok.has_value());
  EXPECT_TRUE(*commit_ok);
  store->SetCommitProbe(nullptr);

  // After publish, a fresh reader observes the committed value.
  auto reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*reader, gid), 2);
}

// Reader opened after a full commit sees the new value: the watermark advanced and the
// snapshot boundary is inclusive of a commit whose timestamp equals the reader's start.
TEST(LockFreeReadSnapshot, ReaderBeginsAfterPublish_SeesNewValue) {
  auto store = MakeStorage(/*flag_on=*/true);
  const auto gid = CreateVertexWithProp(*store, 1);

  {
    auto acc = store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(vertex->SetProperty(store->NameToProperty("p"), PropertyValue(2)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*reader, gid), 2);
}

// A long-lived reader's snapshot is frozen at open time: a later commit is invisible to it,
// while a reader opened after the commit sees the new value.
TEST(LockFreeReadSnapshot, LongLivedReader_UnaffectedByLaterCommit) {
  auto store = MakeStorage(/*flag_on=*/true);
  const auto gid = CreateVertexWithProp(*store, 1);

  auto long_reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*long_reader, gid), 1);

  {
    auto acc = store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(vertex->SetProperty(store->NameToProperty("p"), PropertyValue(2)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  EXPECT_EQ(ReadProp(*long_reader, gid), 1);

  auto fresh_reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*fresh_reader, gid), 2);
}

// A transaction's own uncommitted write is visible to itself (View::NEW), independent of the
// snapshot boundary.
TEST(LockFreeReadSnapshot, OwnUncommittedWrite_Visible) {
  auto store = MakeStorage(/*flag_on=*/true);

  auto acc = store->Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();
  const auto gid = vertex.Gid();
  ASSERT_TRUE(vertex.SetProperty(store->NameToProperty("p"), PropertyValue(7)).has_value());

  auto self = acc->FindVertex(gid, View::NEW);
  ASSERT_TRUE(self.has_value());
  auto value = self->GetProperty(store->NameToProperty("p"), View::NEW);
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(value->ValueInt(), 7);
}

// A/B equivalence: the long-lived-reader scenario yields identical observed values whether the
// experimental flag is on or off.
TEST(LockFreeReadSnapshot, OffPath_SameVisibility_AB) {
  for (const bool flag_on : {true, false}) {
    auto store = MakeStorage(flag_on);
    const auto gid = CreateVertexWithProp(*store, 1);

    auto long_reader = store->Access(memgraph::storage::READ);
    EXPECT_EQ(ReadProp(*long_reader, gid), 1) << "flag_on=" << flag_on;

    {
      auto acc = store->Access(memgraph::storage::WRITE);
      auto vertex = acc->FindVertex(gid, View::OLD);
      ASSERT_TRUE(vertex.has_value());
      ASSERT_TRUE(vertex->SetProperty(store->NameToProperty("p"), PropertyValue(2)).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    EXPECT_EQ(ReadProp(*long_reader, gid), 1) << "flag_on=" << flag_on;

    auto fresh_reader = store->Access(memgraph::storage::READ);
    EXPECT_EQ(ReadProp(*fresh_reader, gid), 2) << "flag_on=" << flag_on;
  }
}

// Phase 2, batch 2: concern-C (write-conflict / lost-update). The write-conflict predicate
// shares the snapshot boundary with reads (Transaction::CommittedBeforeSnapshot): a delta whose
// head commit ts is at/below the writer's snapshot is writable; one published above it is a
// conflict. These tests pin the two happy-path directions and the lost-update interleaving.

// A writer whose head commit sits at or below its snapshot must NOT be falsely aborted: X=1 is
// published, so W's snapshot includes it (head ts <= W.snapshot), and W's overwrite commits.
TEST(LockFreeReadSnapshot, WriterHappyPath_HeadBelowSnapshot_NoFalseAbort) {
  auto store = MakeStorage(/*flag_on=*/true);
  const auto gid = CreateVertexWithProp(*store, 1);

  auto w = store->Access(memgraph::storage::WRITE);
  auto vertex = w->FindVertex(gid, View::OLD);
  ASSERT_TRUE(vertex.has_value());
  ASSERT_TRUE(vertex->SetProperty(store->NameToProperty("p"), PropertyValue(2)).has_value());
  ASSERT_TRUE(w->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  auto reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*reader, gid), 2);
}

// A transaction rewriting its OWN uncommitted delta is not a conflict: the head delta carries
// this txn's transaction_id, so the second SetProperty is allowed. Commit publishes the last write.
TEST(LockFreeReadSnapshot, WriterRewritesOwnUncommittedDelta_Succeeds) {
  auto store = MakeStorage(/*flag_on=*/true);

  auto w = store->Access(memgraph::storage::WRITE);
  auto vertex = w->CreateVertex();
  const auto gid = vertex.Gid();
  ASSERT_TRUE(vertex.SetProperty(store->NameToProperty("p"), PropertyValue(1)).has_value());
  ASSERT_TRUE(vertex.SetProperty(store->NameToProperty("p"), PropertyValue(2)).has_value());
  ASSERT_TRUE(w->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  auto reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*reader, gid), 2);
}

// THE lost-update test. A committer C is parked at after_mint (C_ts minted, engine_lock released,
// watermark NOT yet advanced). A writer W is opened in that window: W's snapshot excludes C. After
// C publishes X=2, W tries to overwrite the SAME vertex. Under the flag the head commit ts C_ts >
// W.snapshot, so X is invisible to W and the write MUST conflict (serialization error) rather than
// silently clobber C's value -- that is the lost update this feature prevents.
TEST(LockFreeReadSnapshot, LostUpdatePrevented_GapCommitPublishesAfterSnapshot_ON) {
  auto store = MakeStorage(/*flag_on=*/true);
  const auto gid = CreateVertexWithProp(*store, 1);

  std::binary_semaphore reached{0};
  std::binary_semaphore resume{0};
  std::optional<bool> c_ok;

  memgraph::storage::CommitProbe probe;
  probe.after_mint = [&] {
    reached.release();
    resume.acquire();
  };
  store->SetCommitProbe(&probe);

  std::thread committer([&] {
    auto acc = store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(vertex->SetProperty(store->NameToProperty("p"), PropertyValue(2)).has_value());
    c_ok = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value();
  });

  // C has minted C_ts but not published; open W now so W's snapshot is below C_ts.
  reached.acquire();
  auto w = store->Access(memgraph::storage::WRITE);

  // Let C finish publishing X=2, then confirm it committed.
  resume.release();
  committer.join();
  ASSERT_TRUE(c_ok.has_value());
  EXPECT_TRUE(*c_ok);
  store->SetCommitProbe(nullptr);

  // W now overwrites the same vertex. The conflict surfaces either at the mutation or at commit;
  // capture whichever, and assert the write did NOT silently succeed.
  auto w_vertex = w->FindVertex(gid, View::OLD);
  ASSERT_TRUE(w_vertex.has_value());
  auto set_res = w_vertex->SetProperty(store->NameToProperty("p"), PropertyValue(3));
  bool serialization_observed = false;
  if (!set_res.has_value()) {
    EXPECT_EQ(set_res.error(), memgraph::storage::Error::SERIALIZATION_ERROR);
    serialization_observed = true;
  } else {
    serialization_observed = !w->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value();
  }
  ASSERT_TRUE(serialization_observed)
      << "LOST UPDATE: W overwrote a gap-committed value it never saw; no serialization error was raised. "
         "This is a real feature bug, not a test problem -- the write-conflict predicate failed to reject a "
         "head commit published above W's snapshot.";
  w.reset();

  // C's value survives; W's write was rejected, not clobbered over C.
  auto reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*reader, gid), 2);
}

// A/B contrast to the test above with the flag OFF. Under OFF the write predicate is
// `ts < start_timestamp`, so a committed head below the writer's start is writable -- OFF prevents
// the lost update by a DIFFERENT mechanism: W SEES C's X=2 and updates on top of it (final X=3),
// rather than being told to retry. NOTE: the mid-window begin used by the ON test is flag-only and
// unreachable here. The OFF commit path never invokes after_mint (that InvokeProbe is gated on the
// flag), and engine_lock is held straight through mint->publish, so a concurrent Access(WRITE)
// would simply block until C finishes -- there is no unpublished window to open W in. We therefore
// let C commit fully first; W then necessarily observes C's commit (start_ts > C_ts) and writes on
// top, which is exactly the OFF no-lost-update mechanism.
TEST(LockFreeReadSnapshot, SameGapScenario_OFF_WriteSucceeds_AB) {
  auto store = MakeStorage(/*flag_on=*/false);
  const auto gid = CreateVertexWithProp(*store, 1);

  {
    auto c = store->Access(memgraph::storage::WRITE);
    auto vertex = c->FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(vertex->SetProperty(store->NameToProperty("p"), PropertyValue(2)).has_value());
    ASSERT_TRUE(c->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto w = store->Access(memgraph::storage::WRITE);
  // W's start is above C's commit, so C's X=2 is visible; W updates on top rather than conflicting.
  EXPECT_EQ(ReadProp(*w, gid), 2);
  auto w_vertex = w->FindVertex(gid, View::OLD);
  ASSERT_TRUE(w_vertex.has_value());
  ASSERT_TRUE(w_vertex->SetProperty(store->NameToProperty("p"), PropertyValue(3)).has_value());
  ASSERT_TRUE(w->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  auto reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*reader, gid), 3);
}

// Phase 2, batch 3: concern-D (GC visibility horizon). Validates the 1.4 GC split -- under the flag
// GC's visibility horizon is min(active snapshot_ts), so a long-lived low-snapshot reader keeps
// reading its own version even after newer versions are committed and GC unlinks the chain.

// A long-lived reader R (snapshot sees X=1) survives two later commits (X=2, X=3) plus a GC pass:
// the visibility horizon is pinned to R's snapshot_ts, so R's version is NOT unlinked and R still
// reads 1. After R is released and GC runs again, a fresh reader reads the latest (3).
TEST(LockFreeReadSnapshot, LongReaderVersionRetainedAcrossGc_ON) {
  auto store = MakeStorageManualGc(/*flag_on=*/true);
  const auto gid = CreateVertexWithProp(*store, 1);

  auto long_reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*long_reader, gid), 1);

  CommitProp(*store, gid, 2);
  CommitProp(*store, gid, 3);

  RunGc(*store);

  // CORE SAFETY ASSERTION: R's snapshot version must not have been reclaimed. A wrong value or a
  // crash here means GC over-reclaimed past the visibility horizon -- a real feature bug, not a
  // test problem. Do NOT weaken this.
  EXPECT_EQ(ReadProp(*long_reader, gid), 1)
      << "GC OVER-RECLAIM: long reader lost its snapshot version (X=1) after newer commits + GC. "
         "The visibility horizon advanced past the oldest active snapshot_ts.";

  long_reader.reset();
  RunGc(*store);

  auto fresh_reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*fresh_reader, gid), 3);
}

// Two readers with different snapshots: R1 sees 1, R2 sees 2. The horizon is min(active snapshot_ts)
// = R1's snapshot (the OldestActive txn), which protects BOTH older versions across GC. Releasing
// R1 lifts the floor to R2; releasing R2 lets GC reclaim down to the latest.
TEST(LockFreeReadSnapshot, MultipleReadersDifferentSnapshots_OldestHorizon_ON) {
  auto store = MakeStorageManualGc(/*flag_on=*/true);
  const auto gid = CreateVertexWithProp(*store, 1);

  auto r1 = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*r1, gid), 1);

  CommitProp(*store, gid, 2);
  auto r2 = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*r2, gid), 2);

  CommitProp(*store, gid, 3);

  RunGc(*store);

  // Horizon = R1's snapshot; both R1's version (1) and R2's version (2) are retained.
  EXPECT_EQ(ReadProp(*r1, gid), 1)
      << "GC OVER-RECLAIM: R1 lost its snapshot version (X=1); horizon advanced past the oldest active snapshot_ts.";
  EXPECT_EQ(ReadProp(*r2, gid), 2)
      << "GC OVER-RECLAIM: R2 lost its snapshot version (X=2) while R1 still pinned an older horizon.";

  r1.reset();
  RunGc(*store);

  // R1 gone: floor rises to R2's snapshot. R2 still reads its version; a fresh reader reads latest.
  EXPECT_EQ(ReadProp(*r2, gid), 2)
      << "GC OVER-RECLAIM: R2 lost its snapshot version (X=2) after R1 released; horizon overshot R2's snapshot_ts.";
  {
    auto fresh_reader = store->Access(memgraph::storage::READ);
    EXPECT_EQ(ReadProp(*fresh_reader, gid), 3);
  }

  r2.reset();
  RunGc(*store);

  auto fresh_reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*fresh_reader, gid), 3);
}

// A/B equivalence to LongReaderVersionRetainedAcrossGc_ON with the flag OFF. The observable outcome
// is identical: OFF pins the version via the start_ts horizon, ON via the snapshot_ts horizon. The
// flag changes the horizon computation, not the observable read result.
TEST(LockFreeReadSnapshot, LongReaderVersionRetainedAcrossGc_OFF_AB) {
  auto store = MakeStorageManualGc(/*flag_on=*/false);
  const auto gid = CreateVertexWithProp(*store, 1);

  auto long_reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*long_reader, gid), 1);

  CommitProp(*store, gid, 2);
  CommitProp(*store, gid, 3);

  RunGc(*store);

  EXPECT_EQ(ReadProp(*long_reader, gid), 1)
      << "GC OVER-RECLAIM (flag OFF): long reader lost its snapshot version (X=1); the start_ts horizon "
         "failed to retain a version an active reader still needs.";

  long_reader.reset();
  RunGc(*store);

  auto fresh_reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*fresh_reader, gid), 3);
}

// Phase 2, batch 3 (stress): concern-D racy surfaces under real concurrency. The deterministic
// batch-1..4 tests park a single commit at a CommitProbe seam to assert one interleaving at a time;
// this test instead drives the feature's shared mutable state -- the BEGIN-time snapshot-ring writes,
// GC's concurrent reads of that ring at OldestActive, the read watermark, and the commit path -- with
// many threads at once, with no probe. It is a crash/consistency smoke test under the normal build
// and a race detector under ThreadSanitizer. kWriters committers mint fresh-vertex commits (no
// write-write conflict, so every commit must succeed) while kReaders readers open frozen SI snapshots
// and a GC thread collects garbage concurrently. Two invariants are asserted: (1) within one reader
// accessor a repeated read of the same vertex's "p" returns the same value (a frozen snapshot is
// stable), and (2) after every writer has joined, exactly kWriters*kWritesPerWriter vertices are
// committed and all are visible -- none lost, none double-counted. Any crash, hang, or violated
// invariant is a real feature bug, not a test problem. Do NOT weaken it.
TEST(LockFreeReadSnapshot, ConcurrentReadersWritersGc_NoCrash_SnapshotStable_ON) {
  auto store = MakeStorageManualGc(/*flag_on=*/true);
  const auto p = store->NameToProperty("p");

  constexpr int kWriters = 4;
  constexpr int kReaders = 4;
  constexpr int kWritesPerWriter = 500;

  std::atomic<bool> stop{false};
  std::atomic<uint64_t> committed{0};

  auto writer_fn = [&](int writer_id) {
    for (int i = 0; i < kWritesPerWriter; ++i) {
      auto acc = store->Access(memgraph::storage::WRITE);
      auto vertex = acc->CreateVertex();
      // Globally unique value across all (writer, iteration) pairs; never re-used, so no writer ever
      // collides with another and every commit is a clean fresh-vertex append.
      const int value = writer_id * kWritesPerWriter + i;
      ASSERT_TRUE(vertex.SetProperty(p, PropertyValue(value)).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value())
          << "concurrent fresh-vertex commit failed unexpectedly (writer " << writer_id << ", iter " << i << ")";
      committed.fetch_add(1, std::memory_order_relaxed);
    }
  };

  auto reader_fn =
      [&] {
        while (!stop.load(std::memory_order_relaxed)) {
          auto r = store->Access(memgraph::storage::READ);

          // Snapshot-stability invariant: the first vertex's "p" read twice within this one accessor must
          // agree. A frozen SI snapshot may not shift under concurrent commits/GC. Guard against an empty
          // graph (no committed vertex yet) by skipping the check that iteration.
          std::optional<memgraph::storage::VertexAccessor> first;
          for (auto vertex : r->Vertices(View::OLD)) {
            first.emplace(vertex);
            break;
          }
          if (first.has_value()) {
            auto v1 = first->GetProperty(p, View::OLD);
            ASSERT_TRUE(v1.has_value());
            const int64_t read1 = v1->ValueInt();
            // A little unrelated work between the two reads to widen the window for a racing writer/GC.
            int64_t churn = 0;
            for (int k = 0; k < 32; ++k) churn += k;
            (void)churn;
            auto v2 = first->GetProperty(p, View::OLD);
            ASSERT_TRUE(v2.has_value());
            ASSERT_EQ(read1, v2->ValueInt())
                << "SNAPSHOT INSTABILITY: two reads of the same vertex's \"p\" within one frozen SI accessor "
                   "returned different values. A committed write or GC mutated a version the reader's snapshot "
                   "still pins -- a real feature bug, not a test problem.";
          }

          // Full concurrent walk of the version chains: every visible vertex must yield a valid int for
          // "p". This is the surface GC unlinks under; a crash or a missing property here is a bug.
          for (auto vertex : r->Vertices(View::OLD)) {
            auto value = vertex.GetProperty(p, View::OLD);
            ASSERT_TRUE(value.has_value());
            (void)value->ValueInt();
          }
          // r closes here, releasing its snapshot so the GC horizon can advance.
        }
      };

  auto gc_fn = [&] {
    while (!stop.load(std::memory_order_relaxed)) {
      // GC concurrent with live readers/writers is exactly the path that reads the snapshot ring at
      // OldestActive to compute the visibility horizon.
      RunGc(*store);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  };

  std::vector<std::thread> readers;
  readers.reserve(kReaders);
  for (int i = 0; i < kReaders; ++i) readers.emplace_back(reader_fn);
  std::thread gc(gc_fn);

  std::vector<std::thread> writers;
  writers.reserve(kWriters);
  for (int i = 0; i < kWriters; ++i) writers.emplace_back(writer_fn, i);
  for (auto &w : writers) w.join();

  // Writers are done; stop the open-ended readers and GC.
  stop.store(true, std::memory_order_relaxed);
  for (auto &r : readers) r.join();
  gc.join();

  ASSERT_EQ(committed.load(), static_cast<uint64_t>(kWriters) * kWritesPerWriter);

  // Every committed vertex is visible exactly once at a fresh snapshot: nothing was lost to a racing
  // GC pass and nothing was double-counted by a torn snapshot-ring write.
  auto final_reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(CountVertices(*final_reader), static_cast<int64_t>(committed.load()))
      << "COMMIT/VISIBILITY LOSS: the fresh-snapshot vertex count does not match the number of committed "
         "transactions. A committed vertex was lost or double-counted under concurrent commits + GC + reads "
         "-- a real feature bug, not a test problem.";
}

// Phase 2, batch 4: concern-E (abort semantics). A committer that mints a commit timestamp and then
// aborts must NOT advance the read watermark (last_committed_mvcc_ts_). The abort here is a UNIQUE
// constraint violation, which storage.cpp checks AFTER GetCommitTimestamp mints the ts and returns
// without ever reaching FinalizeCommitPhase -- so the minted ts is "wasted": the watermark advances
// only on success, at the end of FinalizeCommitPhase. The observable invariant is that the aborted
// transaction is invisible to every reader (before, during, and after the failed commit) and leaves
// subsequent progress uncorrupted.

// A minted-then-aborted duplicate is invisible to all readers and does not disturb later commits. B
// duplicates published row A under UNIQUE(L, p): it mints a ts, fails the unique check, and aborts.
// A reader opened before the failed commit and a fresh reader opened after it both see only A; a
// later distinct-value commit C then advances normally -- proving the watermark never moved to B's
// wasted ts.
TEST(LockFreeReadSnapshot, AbortAfterMint_DoesNotAdvanceWatermark_ON) {
  auto store = MakeStorage(/*flag_on=*/true);
  const auto label = store->NameToLabel("L");
  CreateUniquePConstraint(*store, label);

  const auto gid_a = CommitLabeledVertex(*store, label, 1);

  // Reader opened BEFORE the failing commit: its snapshot includes A only.
  auto reader_before = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*reader_before, gid_a), 1);
  EXPECT_EQ(CountVertices(*reader_before), 1);

  // A duplicate of A: it mints a commit ts, then the UNIQUE check aborts it (no FinalizeCommitPhase).
  std::optional<Gid> gid_b;
  {
    auto w = store->Access(memgraph::storage::WRITE);
    auto vertex = w->CreateVertex();
    gid_b = vertex.Gid();
    ASSERT_TRUE(vertex.AddLabel(label).has_value());
    ASSERT_TRUE(vertex.SetProperty(store->NameToProperty("p"), PropertyValue(1)).has_value());
    auto res = w->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    ASSERT_FALSE(res.has_value()) << "the duplicate commit must fail the UNIQUE constraint";
    EXPECT_EQ(std::get<ConstraintViolation>(res.error()).type, ConstraintViolation::Type::UNIQUE);
  }

  // The reader opened before the abort still sees exactly the pre-abort state.
  EXPECT_EQ(ReadProp(*reader_before, gid_a), 1);
  EXPECT_EQ(CountVertices(*reader_before), 1)
      << "ABORTED-TXN LEAK: a reader opened before the failed commit observed the aborted vertex B.";
  EXPECT_FALSE(reader_before->FindVertex(*gid_b, View::OLD).has_value());

  // A fresh reader opened AFTER the abort also sees only A: the watermark never moved to B's ts.
  {
    auto fresh = store->Access(memgraph::storage::READ);
    EXPECT_EQ(ReadProp(*fresh, gid_a), 1);
    EXPECT_EQ(CountVertices(*fresh), 1)
        << "WATERMARK ADVANCED ON ABORT: a reader opened after the failed commit saw the aborted "
           "vertex B; the read watermark moved to B's wasted commit timestamp. This is a real feature "
           "bug, not a test problem -- an aborted commit must not advance last_committed_mvcc_ts_.";
    EXPECT_FALSE(fresh->FindVertex(*gid_b, View::OLD).has_value());
  }

  // Monotonic progress after the abort: a distinct-value commit C succeeds and is visible alongside A.
  const auto gid_c = CommitLabeledVertex(*store, label, 2);
  {
    auto fresh = store->Access(memgraph::storage::READ);
    EXPECT_EQ(ReadProp(*fresh, gid_a), 1);
    EXPECT_EQ(ReadProp(*fresh, gid_c), 2);
    EXPECT_EQ(CountVertices(*fresh), 2);
    EXPECT_FALSE(fresh->FindVertex(*gid_b, View::OLD).has_value());
  }
}

// Concern-E under concurrency. The mid-window CommitProbe choreography used elsewhere in this suite
// is deliberately NOT used here: on the abort path the UNIQUE check runs BEFORE the after_mint seam.
// In storage.cpp, GetCommitTimestamp mints under engine_lock, UniqueConstraintsViolation is checked
// immediately after (still under engine_lock) and, on violation, aborts and returns -- all before
// the InvokeProbe(after_mint) that only a surviving commit reaches. A constraint-aborting commit
// therefore fires no probe, so its mint->abort window cannot be deterministically parked. Instead we
// stress that window: a committer thread runs the aborting duplicate in a loop (each iteration wastes
// a minted ts) while the main thread hammers reader opens. The invariant holds under every
// interleaving -- no reader ever sees the aborted vertex, so every snapshot contains exactly one
// vertex (A), and the many wasted mints never advance the watermark.
TEST(LockFreeReadSnapshot, ReaderBeginsDuringAbortingCommitWindow_NeverSeesAborted_ON) {
  auto store = MakeStorage(/*flag_on=*/true);
  const auto label = store->NameToLabel("L");
  CreateUniquePConstraint(*store, label);

  const auto gid_a = CommitLabeledVertex(*store, label, 1);

  constexpr int kIters = 500;
  std::binary_semaphore start{0};
  bool all_failed = true;
  bool all_unique = true;

  std::thread committer([&] {
    start.acquire();
    for (int i = 0; i < kIters; ++i) {
      auto w = store->Access(memgraph::storage::WRITE);
      auto vertex = w->CreateVertex();
      EXPECT_TRUE(vertex.AddLabel(label).has_value());
      EXPECT_TRUE(vertex.SetProperty(store->NameToProperty("p"), PropertyValue(1)).has_value());
      auto res = w->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
      all_failed = all_failed && !res.has_value();
      if (!res.has_value()) {
        all_unique = all_unique && std::get<ConstraintViolation>(res.error()).type == ConstraintViolation::Type::UNIQUE;
      }
    }
  });

  start.release();
  for (int i = 0; i < kIters; ++i) {
    auto reader = store->Access(memgraph::storage::READ);
    EXPECT_EQ(CountVertices(*reader), 1)
        << "ABORTED-TXN LEAK: a reader opened during the aborting commit's window saw the aborted "
           "vertex (iteration "
        << i << "). A minted-but-aborted commit must never advance the read watermark.";
    EXPECT_EQ(ReadProp(*reader, gid_a), 1);
  }
  committer.join();

  EXPECT_TRUE(all_failed) << "every duplicate commit must fail the UNIQUE constraint";
  EXPECT_TRUE(all_unique);

  auto fresh = store->Access(memgraph::storage::READ);
  EXPECT_EQ(CountVertices(*fresh), 1);
  EXPECT_EQ(ReadProp(*fresh, gid_a), 1);
}

// A/B equivalence with the flag OFF: the abort-after-mint semantics are identical. The UNIQUE check
// is flag-independent (it runs in both modes), so a duplicate commit aborts the same way and leaves
// the same observable state -- aborted B invisible, later C visible. No probe is needed off-path.
TEST(LockFreeReadSnapshot, AbortAfterMint_DoesNotAdvanceWatermark_OFF_AB) {
  auto store = MakeStorage(/*flag_on=*/false);
  const auto label = store->NameToLabel("L");
  CreateUniquePConstraint(*store, label);

  const auto gid_a = CommitLabeledVertex(*store, label, 1);

  auto reader_before = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*reader_before, gid_a), 1);
  EXPECT_EQ(CountVertices(*reader_before), 1);

  std::optional<Gid> gid_b;
  {
    auto w = store->Access(memgraph::storage::WRITE);
    auto vertex = w->CreateVertex();
    gid_b = vertex.Gid();
    ASSERT_TRUE(vertex.AddLabel(label).has_value());
    ASSERT_TRUE(vertex.SetProperty(store->NameToProperty("p"), PropertyValue(1)).has_value());
    auto res = w->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    ASSERT_FALSE(res.has_value()) << "the duplicate commit must fail the UNIQUE constraint";
    EXPECT_EQ(std::get<ConstraintViolation>(res.error()).type, ConstraintViolation::Type::UNIQUE);
  }

  EXPECT_EQ(ReadProp(*reader_before, gid_a), 1);
  EXPECT_EQ(CountVertices(*reader_before), 1)
      << "ABORTED-TXN LEAK (flag OFF): a reader opened before the failed commit observed aborted B.";
  EXPECT_FALSE(reader_before->FindVertex(*gid_b, View::OLD).has_value());

  {
    auto fresh = store->Access(memgraph::storage::READ);
    EXPECT_EQ(ReadProp(*fresh, gid_a), 1);
    EXPECT_EQ(CountVertices(*fresh), 1)
        << "ABORTED-TXN LEAK (flag OFF): a reader opened after the failed commit saw aborted B.";
    EXPECT_FALSE(fresh->FindVertex(*gid_b, View::OLD).has_value());
  }

  const auto gid_c = CommitLabeledVertex(*store, label, 2);
  {
    auto fresh = store->Access(memgraph::storage::READ);
    EXPECT_EQ(ReadProp(*fresh, gid_a), 1);
    EXPECT_EQ(ReadProp(*fresh, gid_c), 2);
    EXPECT_EQ(CountVertices(*fresh), 2);
    EXPECT_FALSE(fresh->FindVertex(*gid_b, View::OLD).has_value());
  }
}

// Phase 2, batch 5: RECOV / INV-DURABLE. The flag is a runtime-only choice about how live readers
// compute their snapshot boundary; it must never bleed into on-disk artifacts. A database written
// with the flag ON must recover into an instance with the flag OFF, and vice versa. If a
// cross-recovery case here loses or corrupts data, the flag leaked into durable state -- a real
// design-invariant violation, not a test problem. Do NOT weaken these tests to make them pass.
//
// Durability idiom mirrors storage_v2_durability_inmemory.cpp: a plain InMemoryStorage over a
// PERIODIC_SNAPSHOT_WITH_WAL storage_directory, an explicit forced CreateSnapshot so the data is on
// disk before the storage is destroyed, then a second InMemoryStorage with recover_on_startup=true
// on the same directory.

namespace {

// Builds durable storage on `dir` with the flag as given, writes 5 vertices carrying p = index,
// commits, forces a snapshot so the dataset is persisted, then destroys the storage cleanly.
void WriteDurable(const std::filesystem::path &dir, bool flag_on) {
  Config config{};
  config.durability.storage_directory = dir;
  config.durability.recover_on_startup = false;
  config.durability.snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  config.experimental_lockfree_read_snapshot = flag_on;

  auto store = std::make_unique<InMemoryStorage>(config);
  {
    auto acc = store->Access(memgraph::storage::WRITE);
    for (int i = 0; i < 5; ++i) {
      auto vertex = acc->CreateVertex();
      ASSERT_TRUE(vertex.SetProperty(store->NameToProperty("p"), PropertyValue(i)).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  // Force the snapshot so the dataset is on disk regardless of WAL flush timing.
  ASSERT_TRUE(store->CreateSnapshot(/*force=*/true).has_value());
  store.reset();
}

// Recovers a fresh InMemoryStorage on `dir` with the (possibly different) flag value and asserts the
// recovered vertices carry exactly p = {0, .., expected_count-1}.
void RecoverAndCheck(const std::filesystem::path &dir, bool flag_on, int expected_count) {
  Config config{};
  config.durability.storage_directory = dir;
  config.durability.recover_on_startup = true;
  config.durability.snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  config.experimental_lockfree_read_snapshot = flag_on;

  auto store = std::make_unique<InMemoryStorage>(config);
  auto acc = store->Access(memgraph::storage::READ);

  std::vector<int64_t> props;
  for (auto vertex : acc->Vertices(View::OLD)) {
    auto value = vertex.GetProperty(store->NameToProperty("p"), View::OLD);
    ASSERT_TRUE(value.has_value());
    props.push_back(value->ValueInt());
  }
  std::sort(props.begin(), props.end());

  std::vector<int64_t> expected;
  expected.reserve(expected_count);
  for (int i = 0; i < expected_count; ++i) expected.push_back(i);

  EXPECT_EQ(props, expected) << "CROSS-RECOVERY DATA MISMATCH: the recovered vertex/property set differs from what "
                                "was written. The experimental_lockfree_read_snapshot flag leaked into durable "
                                "state -- a real violation of the runtime-only invariant, not a test problem.";
}

// Per-test unique storage_directory, cleaned up on both ends of the test to avoid cross-test
// contamination -- the remove_all cleanup idiom from DurabilityTest.
class LockFreeReadSnapshotRecovery : public ::testing::Test {
 protected:
  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

  void Clear() {
    if (std::filesystem::exists(storage_directory)) std::filesystem::remove_all(storage_directory);
  }

  std::filesystem::path storage_directory{
      std::filesystem::temp_directory_path() /
      ("MG_test_unit_storage_v2_lockfree_read_snapshot_" +
       std::string(::testing::UnitTest::GetInstance()->current_test_info()->name()))};
};

}  // namespace

// Wrote with the feature ON, restarted with it OFF: the durable data is fully recovered.
TEST_F(LockFreeReadSnapshotRecovery, WriteOn_RecoverOff_DataIntact) {
  WriteDurable(storage_directory, /*flag_on=*/true);
  RecoverAndCheck(storage_directory, /*flag_on=*/false, 5);
}

// Reverse direction: wrote with the feature OFF, restarted with it ON.
TEST_F(LockFreeReadSnapshotRecovery, WriteOff_RecoverOn_DataIntact) {
  WriteDurable(storage_directory, /*flag_on=*/false);
  RecoverAndCheck(storage_directory, /*flag_on=*/true, 5);
}

// Sanity: ON -> ON round-trips.
TEST_F(LockFreeReadSnapshotRecovery, WriteOn_RecoverOn_DataIntact) {
  WriteDurable(storage_directory, /*flag_on=*/true);
  RecoverAndCheck(storage_directory, /*flag_on=*/true, 5);
}

// Baseline: OFF -> OFF round-trips.
TEST_F(LockFreeReadSnapshotRecovery, WriteOff_RecoverOff_DataIntact) {
  WriteDurable(storage_directory, /*flag_on=*/false);
  RecoverAndCheck(storage_directory, /*flag_on=*/false, 5);
}

// Phase 2, batch 7: concern-B (index-creation gap). Under the flag a label-property index is
// built by scanning the vertex store at the accessor's frozen snapshot_ts (=
// last_committed_mvcc_ts_ at AccessorOpen time). If a vertex committed in the window
// (snapshot_ts, start_timestamp) -- i.e. it minted its ts and ran FinalizeCommitPhase steps
// (set delta timestamps, update active indices) BEFORE the index was registered, but is still
// parked at before_publish when CreateIndex opens -- it is invisible to PopulateIndex (its
// delta ts > snapshot_ts under the flag-ON predicate) AND is not covered by the automatic
// commit-time index update (because RegisterIndex had not yet run when it executed that step).
// Result: the vertex is permanently absent from the index. Under flag-OFF the engine_lock is
// held straight through GetCommitTimestamp→FinalizeCommitPhase, so a concurrent commit cannot
// be interleaved into CreateIndex; the first-committed vertex is fully visible at start_ts.
//
// NOTE: This test is EXPECTED TO FAIL for flag_on=true (it reproduces a real bug). It will
// turn green when the fix (e.g. a PopulateIndex re-pass using start_ts, or seeding
// last_committed_mvcc_ts_ as the accessor's snapshot at index-scan time) is applied.

// Helper: count vertices reachable via a label-property index scan on the given accessor.
namespace {

int64_t CountViaLabelPropertyIndex(Accessor &acc, LabelId label, memgraph::storage::PropertyId prop) {
  const std::array paths = {memgraph::storage::PropertyPath{prop}};
  int64_t n = 0;
  for (auto v :
       acc.Vertices(label, std::span<memgraph::storage::PropertyPath const>{paths.data(), paths.size()}, View::OLD)) {
    (void)v;
    ++n;
  }
  return n;
}

}  // namespace

// Concurrent writers commit fresh (L, P) vertices while the main thread builds the
// label-property index via ReadOnlyAccess(). ReadOnlyAccess() is exclusive with an in-flight
// WRITE commit on main_lock_, so it blocks briefly per iteration until the current writer
// finishes — there is no indefinite park and no deadlock. After all writers join, every
// committed vertex must appear in the index: pre-seeded ones captured by PopulateIndex,
// later ones picked up by commit-time index update.
TEST(LockFreeReadSnapshot, IndexCreate_CompleteUnderConcurrentWrites_ON) {
  auto store = MakeStorage(/*flag_on=*/true);
  const auto label = store->NameToLabel("L");
  const auto prop = store->NameToProperty("p");

  constexpr int kSeed = 5;
  constexpr int kWriters = 4;
  constexpr int kWritesPerWriter = 50;

  // Pre-seed vertices (negative p values, distinct from writer range) so PopulateIndex
  // has committed rows to scan regardless of when the concurrent writers commit.
  for (int i = 0; i < kSeed; ++i) {
    CommitLabeledVertex(*store, label, -(i + 1));
  }

  std::atomic<int> committed{0};

  auto writer_fn = [&](int writer_id) {
    for (int i = 0; i < kWritesPerWriter; ++i) {
      auto acc = store->Access(memgraph::storage::WRITE);
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.AddLabel(label).has_value());
      // Globally unique value across all (writer_id, i) pairs; no write-write conflicts.
      ASSERT_TRUE(v.SetProperty(prop, PropertyValue(writer_id * kWritesPerWriter + i)).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
      committed.fetch_add(1, std::memory_order_relaxed);
    }
  };

  std::vector<std::thread> writers;
  writers.reserve(kWriters);
  for (int i = 0; i < kWriters; ++i) writers.emplace_back(writer_fn, i);

  {
    auto idx_acc = store->ReadOnlyAccess();
    auto res = idx_acc->CreateIndex(label, {prop});
    ASSERT_TRUE(res.has_value()) << "CreateIndex failed unexpectedly (flag_on=true).";
    ASSERT_TRUE(idx_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  for (auto &w : writers) w.join();

  const int64_t total = kSeed + committed.load(std::memory_order_relaxed);

  auto reader = store->Access(memgraph::storage::READ);
  const int64_t full_count = CountVertices(*reader);
  ASSERT_EQ(full_count, total) << "FULL SCAN BUG (flag_on=true): expected " << total << " committed vertices.";

  const int64_t index_count = CountViaLabelPropertyIndex(*reader, label, prop);
  EXPECT_EQ(index_count, full_count)
      << "INDEX COMPLETENESS FAILURE (flag_on=true): " << (full_count - index_count) << " of " << full_count
      << " committed (L,P) vertices are missing from the label-property index. "
         "Pre-seeded vertices must be captured by PopulateIndex; vertices committed after "
         "CreateIndex must be picked up by the commit-time index update.";
}

// A/B counterpart with the flag OFF. The flag-OFF lock path (engine_lock held through
// mint→publish) has no gap window; the completeness invariant is identical.
TEST(LockFreeReadSnapshot, IndexCreate_CompleteUnderConcurrentWrites_OFF_AB) {
  auto store = MakeStorage(/*flag_on=*/false);
  const auto label = store->NameToLabel("L");
  const auto prop = store->NameToProperty("p");

  constexpr int kSeed = 5;
  constexpr int kWriters = 4;
  constexpr int kWritesPerWriter = 50;

  for (int i = 0; i < kSeed; ++i) {
    CommitLabeledVertex(*store, label, -(i + 1));
  }

  std::atomic<int> committed{0};

  auto writer_fn = [&](int writer_id) {
    for (int i = 0; i < kWritesPerWriter; ++i) {
      auto acc = store->Access(memgraph::storage::WRITE);
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.AddLabel(label).has_value());
      ASSERT_TRUE(v.SetProperty(prop, PropertyValue(writer_id * kWritesPerWriter + i)).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
      committed.fetch_add(1, std::memory_order_relaxed);
    }
  };

  std::vector<std::thread> writers;
  writers.reserve(kWriters);
  for (int i = 0; i < kWriters; ++i) writers.emplace_back(writer_fn, i);

  {
    auto idx_acc = store->ReadOnlyAccess();
    auto res = idx_acc->CreateIndex(label, {prop});
    ASSERT_TRUE(res.has_value()) << "CreateIndex failed unexpectedly (flag_on=false).";
    ASSERT_TRUE(idx_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  for (auto &w : writers) w.join();

  const int64_t total = kSeed + committed.load(std::memory_order_relaxed);

  auto reader = store->Access(memgraph::storage::READ);
  const int64_t full_count = CountVertices(*reader);
  ASSERT_EQ(full_count, total) << "FULL SCAN BUG (flag_on=false): expected " << total << " committed vertices.";

  const int64_t index_count = CountViaLabelPropertyIndex(*reader, label, prop);
  EXPECT_EQ(index_count, full_count) << "INDEX COMPLETENESS FAILURE (flag_on=false): " << (full_count - index_count)
                                     << " of " << full_count
                                     << " committed (L,P) vertices are missing from the label-property index.";
}

// Phase 2, batch 7 (GC horizon safety): Under flag-ON, the GC visibility horizon for SI
// readers is min(active snapshot_ts). When a version is committed BEFORE the reader opens
// (so the reader's snapshot_ts == that version's commit_ts), GC must retain that exact version
// -- it is the one the reader needs to answer View::OLD queries. The [S, T_r) gap (where
// S = snapshot_ts and T_r = start_ts, with S <= T_r) must not cause GC to use start_ts as
// the floor, which would let it reclaim the S-version as "old".
//
// Sequence: CommitProp(2) → open long SI reader (snapshot_ts = T_2) → CommitProp(3) → RunGc
// (horizon = T_2) → assert reader still reads 2. Then release reader, RunGc (horizon lifts),
// fresh reader reads 3.
TEST(LockFreeReadSnapshot, HorizonGapCommit_RetainsDeltaBetweenSnapshotAndStart_ON) {
  auto store = MakeStorageManualGc(/*flag_on=*/true);
  const auto gid = CreateVertexWithProp(*store, 1);

  // Commit p=2 BEFORE the reader opens. Under flag-ON, the next Access(READ) will capture
  // snapshot_ts = last_committed_mvcc_ts_ = T_2 (the commit ts of p=2). The gap
  // [S=T_2, T_r=start_ts) arises because start_ts is minted after the snapshot load; it is
  // always >= T_2 (and typically T_2 + 1 for a quiescent store). GC must use S = T_2 as the
  // horizon for this reader, NOT T_r -- the p=2 version lives exactly at S and is what the
  // reader returns for View::OLD.
  CommitProp(*store, gid, 2);

  auto long_reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*long_reader, gid), 2);

  // Commit p=3 while the reader is alive. The reader's snapshot_ts (T_2) is frozen; p=3 at
  // T_3 > T_2 is invisible to the reader. GC horizon = T_2 (the oldest active snapshot_ts).
  CommitProp(*store, gid, 3);

  RunGc(*store);

  // CORE SAFETY ASSERTION: p=2 was committed at exactly snapshot_ts = T_2. GC horizon = T_2
  // means the p=2 version must be retained (its commit_ts == horizon; the reader needs it).
  // A crash or wrong value here means GC over-reclaimed into the [S, T_r) gap -- it used
  // start_ts (T_r > T_2) as the floor instead of snapshot_ts (T_2). This is a real feature
  // bug, not a test problem. Do NOT weaken this assertion.
  EXPECT_EQ(ReadProp(*long_reader, gid), 2)
      << "GC OVER-RECLAIM: the long reader lost its snapshot version (p=2) after CommitProp(3) "
         "plus GC. The GC horizon must be snapshot_ts (T_2), not start_ts (T_r > T_2). "
         "Using start_ts as the floor would reclaim p=2 (T_2 < T_r, newer version p=3 exists), "
         "leaving the SI reader with no accessible version at its snapshot.";

  // Release the reader; the snapshot ring entry is gone. GC horizon lifts to T_3 (or beyond).
  long_reader.reset();
  RunGc(*store);

  auto fresh_reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*fresh_reader, gid), 3);
}

// Phase 2, batch 7 (V4 -- RC isolation does not depress GC floor): Under flag-ON, only
// SNAPSHOT_ISOLATION transactions register in the lockfree snapshot ring
// (transaction.lockfree_snapshot = flag_on && isolation == SI; see storage.cpp). A
// READ_COMMITTED accessor therefore does NOT inject a stale snapshot_ts into the ring: its GC
// footprint is its start_timestamp (same as flag-OFF). This means GC can advance freely past
// the "committed-as-of-reader-open" tier, unlike with an SI reader whose frozen snapshot_ts
// would pin the floor.
//
// Observable proxy (GcVisibilityHorizon() is private): we assert two properties:
//   (1) RC semantics: the accessor sees the LATEST committed value on each read -- it is NOT
//       frozen to the state at accessor-open time (which would signal an SI snapshot in use).
//   (2) GC safety: after RunGc with only the RC reader as the sole active transaction, the
//       reader still reads correctly (GC did not corrupt the live chain).
//
// TODO: expose InMemoryStorage::TestGcHorizon() returning the last value from
// GcVisibilityHorizon() so we can directly assert horizon >= rc_reader.start_timestamp rather
// than relying on the read-value proxy. Until that getter exists, this test is best-effort and
// documents the intended behaviour.
TEST(LockFreeReadSnapshot, NonSiReaderDoesNotPinGcFloorLow_ON) {
  auto store = MakeStorageManualGc(/*flag_on=*/true);
  const auto gid = CreateVertexWithProp(*store, 1);

  CommitProp(*store, gid, 2);
  CommitProp(*store, gid, 3);

  // Open a READ_COMMITTED accessor. Its transaction.lockfree_snapshot = false (RC is excluded
  // from the snapshot ring regardless of the flag). The GC horizon treats this txn at its
  // start_timestamp, not at some older snapshot_ts -- so GC can advance past T_2 and T_3 once
  // they have a newer version, without waiting for this reader to close.
  auto rc_reader =
      store->Access(memgraph::storage::READ, memgraph::storage::IsolationLevel::READ_COMMITTED, std::nullopt);

  // (1) RC semantics: the reader's first access sees the current committed head (p=3). An SI
  // reader opened at the same instant would also see 3 because last_committed_mvcc_ts_ == T_3
  // at open time; the distinction surfaces on the SECOND read below, after CommitProp(4).
  EXPECT_EQ(ReadProp(*rc_reader, gid), 3);

  // Commit p=4 WHILE the RC reader is open. Under SI the reader would remain frozen at T_3 and
  // still return 3. Under RC each read re-evaluates against the latest committed snapshot, so
  // the next read must return 4.
  CommitProp(*store, gid, 4);

  // (1) RC semantics (distinguishing assertion): RC sees p=4; an SI reader would see p=3.
  // If this returns 3, the isolation-level override was not applied (the accessor behaves like
  // SI), and the subsequent GC-floor argument is moot.
  EXPECT_EQ(ReadProp(*rc_reader, gid), 4)
      << "RC ISOLATION MISS: expected p=4 (latest committed) but got a stale value. "
         "The Access(READ, IsolationLevel::READ_COMMITTED, ...) override did not take effect; "
         "the accessor is frozen like a SNAPSHOT_ISOLATION reader. Check that "
         "transaction.lockfree_snapshot is false for RC and that View::OLD re-snapshots per read.";

  // GC with only the RC reader as the sole active transaction. Horizon = rc_reader.start_ts
  // (the RC txn does NOT depress the floor via a stale snapshot_ts). Under that horizon,
  // p=1 (T_1) and p=2 (T_2) and p=3 (T_3) are all below start_ts and have a newer version
  // (p=4) -- they are candidates for reclaim. p=4 (T_4) is the current head; it is kept.
  RunGc(*store);

  // (2) GC safety: the RC reader still reads correctly after GC.
  EXPECT_EQ(ReadProp(*rc_reader, gid), 4)
      << "RC READER BROKEN AFTER GC: expected p=4 (current head) but the RC accessor returned "
         "a wrong value or crashed. GC must not reclaim the current head (p=4 has no successor).";

  rc_reader.reset();
  RunGc(*store);

  auto fresh_reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*fresh_reader, gid), 4);
}

// Phase 2, batch 6: concern-F (non-sequential edge path). Two of the four flag-converted MVCC
// predicate sites live on the non-sequential edge path: PrepareForNonSequentialWrite's head check
// at mvcc.hpp:160 and its chain-walk at :202. These are exercised by edge-creation interleavings
// that do not arise on the property-write path (SetProperty routes through PrepareForWrite, not
// PrepareForNonSequentialWrite).
//
// The scenario: vertex A is clean. Committer C creates edge A->B, yielding a REMOVE_OUT_EDGE undo
// delta on A (DeltaChainState::SEQUENTIAL) with commit_info->timestamp = C_ts after C publishes.
// Writer W opens AFTER C's before_publish probe fires but BEFORE C publishes (so W.snapshot_ts is
// captured from last_committed_mvcc_ts_, which has not yet advanced to C_ts).
//
// When W then attempts to create another edge on A:
//
//   PrepareForNonSequentialWrite (mvcc.hpp:152):
//     ts = A.head.commit_info->timestamp = C_ts          (C has finished publishing)
//     ts != W.transaction_id                             (C_ts < kTransactionInitialId)
//     CommittedBeforeSnapshot(C_ts):
//       flag ON:  C_ts <= W.snapshot_ts  → FALSE         ← gap: C_ts published above W's snapshot
//       flag OFF: C_ts <  W.start_ts     → TRUE (C fully committed before W opened; no gap)
//
// Under flag ON the FALSE branch at :160 enters the chain-walk at :202. C's REMOVE_OUT_EDGE undo
// delta (action == REMOVE_OUT_EDGE) is explicitly allowed by the walk (only non-REMOVE_EDGE
// actions are blocking). The loop exits with NON_SEQUENTIAL: W's edge creation SUCCEEDS but in
// non-sequential mode. The observable difference from flag OFF is the snapshot boundary:
//
//   ON:  CommittedBeforeSnapshot(C_ts) = FALSE → C's undo is applied for View::OLD → A shows 0
//        out-edges from W's perspective (C's gap-committed edge is invisible to W's snapshot).
//   OFF: CommittedBeforeSnapshot(C_ts) = TRUE  → C's undo is NOT applied → A shows 1 out-edge
//        (C's edge is visible; W.start_ts > C_ts, so it is in W's snapshot).
//
// Final graph state is consistent: after both C and W commit, a fresh reader sees 2 out-edges on
// A under both flag settings.
//
// NOTE: the before_publish probe (used here) fires inside FinalizeCommitPhase, BEFORE engine_lock
// is reacquired and BEFORE commit_info->timestamp is set to C_ts. At the probe's park point the
// engine_lock is released, so W can open freely. After C publishes, A's head delta carries C_ts
// (permanently above W.snapshot_ts for flag ON). The probe is flag-gated (acquire_engine_lock =
// lockfree): it does not fire for flag OFF, which is why the OFF test runs C to completion first.

// Flag ON: C creates edge A->B and parks at before_publish. W opens in the gap. After C
// publishes, A's head delta (REMOVE_OUT_EDGE, ts=C_ts) is above W.snapshot_ts. The FALSE branch
// at mvcc.hpp:160 is taken; the chain-walk at :202 traverses the REMOVE_OUT_EDGE (allowed) and
// returns NON_SEQUENTIAL. W's CreateEdge succeeds. The observable signature of the non-sequential
// path is the snapshot boundary: W's View::OLD sees 0 out-edges on A (C's edge invisible), while
// View::NEW shows W's own uncommitted edge. After W commits, a fresh reader sees 2 out-edges.
TEST(LockFreeReadSnapshot, NonSequentialEdgeWriteInGap_ON) {
  auto store = MakeStorage(/*flag_on=*/true);

  // Commit vertices A and B into clean state (no edge deltas on either).
  Gid gid_a{};
  Gid gid_b{};
  {
    auto acc = store->Access(memgraph::storage::WRITE);
    auto a = acc->CreateVertex();
    gid_a = a.Gid();
    auto b = acc->CreateVertex();
    gid_b = b.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  std::binary_semaphore reached{0};
  std::binary_semaphore resume{0};
  std::optional<bool> c_ok;

  // before_publish fires inside FinalizeCommitPhase (flag ON: acquire_engine_lock=true), BEFORE
  // engine_lock is reacquired and BEFORE commit_info->timestamp is promoted to C_ts. At this park
  // point, last_committed_mvcc_ts_ has not yet advanced to C_ts, so a concurrent Access(WRITE)
  // sees the pre-publish watermark and captures W.snapshot_ts < C_ts.
  memgraph::storage::CommitProbe probe;
  probe.before_publish = [&] {
    reached.release();
    resume.acquire();
  };
  store->SetCommitProbe(&probe);

  std::thread committer([&] {
    auto acc = store->Access(memgraph::storage::WRITE);
    auto a = acc->FindVertex(gid_a, View::OLD);
    auto b = acc->FindVertex(gid_b, View::OLD);
    ASSERT_TRUE(a.has_value() && b.has_value());
    // C creates edge A->B. CreateEdgeInternal prepends a REMOVE_OUT_EDGE undo delta (state=
    // SEQUENTIAL) to vertex A. The undo delta's commit_info->timestamp = TRANSACTION_ID at the
    // park point; it is promoted to C_ts inside FinalizeCommitPhase after we resume C.
    ASSERT_TRUE(acc->CreateEdge(&*a, &*b, acc->NameToEdgeType("e")).has_value());
    c_ok = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value();
  });

  // C is parked at before_publish: C_ts is minted, engine_lock released, watermark not advanced.
  reached.acquire();

  // W opens: snapshot_ts = last_committed_mvcc_ts_.load() at Access time (flag ON).
  // Because the watermark has not advanced, W.snapshot_ts < C_ts.
  auto w = store->Access(memgraph::storage::WRITE);

  // Let C finish. FinalizeCommitPhase sets:
  //   A.head_delta.commit_info->timestamp = C_ts   (line 1293 in storage.cpp)
  //   last_committed_mvcc_ts_            = C_ts   (line 1382)
  // W.snapshot_ts is already frozen below C_ts and does not change.
  resume.release();
  committer.join();
  ASSERT_TRUE(c_ok.has_value());
  ASSERT_TRUE(*c_ok);
  store->SetCommitProbe(nullptr);

  // With A.head_delta.ts = C_ts and W.snapshot_ts < C_ts:
  // CommittedBeforeSnapshot(C_ts) = (C_ts <= W.snapshot_ts) = FALSE   [flag ON]
  // The undo delta (REMOVE_OUT_EDGE) is visible to W's read and is applied: to reconstruct
  // View::OLD, ApplyDeltasForRead undoes C's REMOVE_OUT_EDGE entry, yielding 0 out-edges.
  auto w_a = w->FindVertex(gid_a, View::OLD);
  ASSERT_TRUE(w_a.has_value());
  {
    auto w_old_edges = w_a->OutEdges(View::OLD);
    ASSERT_TRUE(w_old_edges.has_value());
    EXPECT_EQ(w_old_edges->edges.size(), 0u)
        << "SNAPSHOT LEAK: W's View::OLD shows C's gap-committed edge on A. "
           "CommittedBeforeSnapshot(C_ts) = FALSE under flag ON means the REMOVE_OUT_EDGE undo "
           "must be applied, rolling A back to its state before C_ts (0 out-edges).";
  }

  // W creates a second edge on A. PrepareForNonSequentialWrite (mvcc.hpp:152):
  //   ts = C_ts, CommittedBeforeSnapshot = FALSE → FALSE branch at :160
  //   chain-walk at :202: REMOVE_OUT_EDGE delta → action == REMOVE_OUT_EDGE → allowed (continue)
  //   loop exits → NON_SEQUENTIAL (not SERIALIZATION_ERROR)
  // CreateEdge succeeds; the new delta on A gets DeltaChainState::NON_SEQUENTIAL.
  auto w_b = w->FindVertex(gid_b, View::OLD);
  ASSERT_TRUE(w_b.has_value());

  auto edge_res = w->CreateEdge(&*w_a, &*w_b, w->NameToEdgeType("e2"));
  ASSERT_TRUE(edge_res.has_value())
      << "NON-SEQUENTIAL PATH BROKEN: W could not create an edge on vertex A whose head delta "
         "(REMOVE_OUT_EDGE, C_ts > W.snapshot_ts) was a gap-committed edge-creation undo. "
         "PrepareForNonSequentialWrite at mvcc.hpp:202 must traverse REMOVE_OUT_EDGE (allowed) "
         "and return NON_SEQUENTIAL. A SERIALIZATION_ERROR here is a real predicate bug.";

  // W commits. Both C's and W's edges are now durable.
  ASSERT_TRUE(w->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  w.reset();

  // Fresh reader (snapshot_ts = last_committed_mvcc_ts_, above both C_ts and W_ts) sees both
  // out-edges on A.
  auto reader = store->Access(memgraph::storage::READ);
  auto ra = reader->FindVertex(gid_a, View::OLD);
  ASSERT_TRUE(ra.has_value());
  auto out_edges = ra->OutEdges(View::OLD);
  ASSERT_TRUE(out_edges.has_value());
  EXPECT_EQ(out_edges->edges.size(), 2u) << "EDGE COUNT WRONG after both C and W committed: expected 2 out-edges on A.";
}

// A/B contrast: flag OFF. Under OFF, engine_lock spans GetCommitTimestamp→FinalizeCommitPhase
// (acquire_engine_lock=false so before_publish is never invoked). No gap window exists: W cannot
// open until C has fully committed. W.start_timestamp > C_ts, so CommittedBeforeSnapshot(C_ts) =
// (C_ts < W.start_ts) = TRUE (flag OFF path at mvcc.hpp:160). PrepareForNonSequentialWrite exits
// early with SUCCESS (not NON_SEQUENTIAL). The observable difference from the ON scenario is the
// snapshot boundary for W's View::OLD: W opened after C committed, so C's edge IS in W's snapshot
// (1 out-edge), unlike the ON scenario where the gap made it invisible (0 out-edges). Final graph
// state is consistent with the ON scenario: both C's and W's edges committed (2 out-edges total).
TEST(LockFreeReadSnapshot, NonSequentialEdgeWriteInGap_OFF_AB) {
  auto store = MakeStorage(/*flag_on=*/false);

  Gid gid_a{};
  Gid gid_b{};
  {
    auto acc = store->Access(memgraph::storage::WRITE);
    auto a = acc->CreateVertex();
    gid_a = a.Gid();
    auto b = acc->CreateVertex();
    gid_b = b.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // C commits edge A->B fully. No probe: under flag OFF, before_publish is not invoked
  // (acquire_engine_lock = lockfree = false). Engine_lock is held through the full commit.
  {
    auto c = store->Access(memgraph::storage::WRITE);
    auto a = c->FindVertex(gid_a, View::OLD);
    auto b = c->FindVertex(gid_b, View::OLD);
    ASSERT_TRUE(a.has_value() && b.has_value());
    ASSERT_TRUE(c->CreateEdge(&*a, &*b, c->NameToEdgeType("e")).has_value());
    ASSERT_TRUE(c->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // W opens after C's full publish. W.start_timestamp > C_ts (flag OFF uses start_timestamp as
  // the snapshot boundary; no snapshot_ts ring is involved). CommittedBeforeSnapshot(C_ts) =
  // (C_ts < W.start_ts) = TRUE. PrepareForNonSequentialWrite takes the TRUE early exit at :160
  // → SUCCESS (not NON_SEQUENTIAL). C's edge is committed before W's snapshot.
  auto w = store->Access(memgraph::storage::WRITE);
  auto w_a = w->FindVertex(gid_a, View::OLD);
  ASSERT_TRUE(w_a.has_value());
  {
    // C's committed edge is visible to W's snapshot (W.start_ts > C_ts → undo NOT applied).
    auto w_old_edges = w_a->OutEdges(View::OLD);
    ASSERT_TRUE(w_old_edges.has_value());
    EXPECT_EQ(w_old_edges->edges.size(), 1u)
        << "FLAG OFF SNAPSHOT: W should see C's committed edge (1 out-edge). "
           "CommittedBeforeSnapshot(C_ts) = TRUE (flag OFF: ts < start_ts) means the "
           "REMOVE_OUT_EDGE undo is NOT applied; the current in-memory state (1 edge) is used.";
  }

  auto w_b = w->FindVertex(gid_b, View::OLD);
  ASSERT_TRUE(w_b.has_value());
  auto edge_res = w->CreateEdge(&*w_a, &*w_b, w->NameToEdgeType("e2"));
  ASSERT_TRUE(edge_res.has_value())
      << "FLAG OFF: PrepareForNonSequentialWrite should return SUCCESS for a head delta committed "
         "before W's start_timestamp. CommittedBeforeSnapshot = TRUE → early exit, no chain-walk.";
  ASSERT_TRUE(w->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  w.reset();

  // Final state consistent with ON: both edges committed, fresh reader sees 2 out-edges.
  auto reader = store->Access(memgraph::storage::READ);
  auto ra = reader->FindVertex(gid_a, View::OLD);
  ASSERT_TRUE(ra.has_value());
  auto out_edges = ra->OutEdges(View::OLD);
  ASSERT_TRUE(out_edges.has_value());
  EXPECT_EQ(out_edges->edges.size(), 2u) << "EDGE COUNT WRONG: expected 2 out-edges on A (C's + W's) under flag OFF.";
}

// Post-restart read of a multiply-updated vertex, flag ON. Under the flag a live reader's SI snapshot
// boundary is last_committed_mvcc_ts_ (mvcc.hpp: CommittedBeforeSnapshot -> ts <= snapshot_ts), which
// starts at 0 on a fresh restart; Fix B (storage.cpp recovery) lifts it back to the recovered
// last-durable-timestamp. The concern being guarded is that a reader whose snapshot_ts froze at 0
// would reject any recovered value carried by a committed delta (commit ts > 0) and revert to the
// oldest version in the chain.
//
// The vertex is driven through FOUR separate committed transactions (p = 1,2,3,4) persisted via WAL
// replay -- an early forced snapshot captures p=1 and p=2,3,4 land as WAL deltas replayed on restart,
// the recovery path most likely to rebuild a version chain. The post-restart reader must read the
// LATEST value 4, with exactly the one recovered vertex present.
//
// EMPIRICAL NOTE (verified by A/B, both storage.cpp restore sites neutralized + rebuilt): in-memory
// recovery reconstructs FLAT vertices -- both snapshot load (snapshot.cpp: Vertex{gid, nullptr} +
// InitProperties) and WAL replay (wal.cpp: Vertex{gid, nullptr} + in-place SetProperty) leave
// delta()==nullptr, so the read returns the in-place latest value regardless of snapshot_ts. The
// value/count assertions therefore still pass with Fix B removed (no committed delta chain to
// expose the watermark). To close that gap the test NOW also asserts the watermark scalar directly:
// LastCommittedMvccTimestamp() must be > 0 after recovery. A zero here is the exact Fix B
// regression signature -- the reseed (last_committed_mvcc_ts_ <- last-durable-timestamp) was never
// reached. Do not weaken either the value/count assertions or the watermark assertion.
TEST_F(LockFreeReadSnapshotRecovery, RecoveredUpdateChain_ReadsLatestUnderFlagOn) {
  Gid gid{};
  {
    Config config{};
    config.durability.storage_directory = storage_directory;
    config.durability.recover_on_startup = false;
    config.durability.snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    config.experimental_lockfree_read_snapshot = true;

    auto store = std::make_unique<InMemoryStorage>(config);
    const auto p = store->NameToProperty("p");

    // Create the vertex at p=1 and commit.
    {
      auto acc = store->Access(memgraph::storage::WRITE);
      auto vertex = acc->CreateVertex();
      gid = vertex.Gid();
      ASSERT_TRUE(vertex.SetProperty(p, PropertyValue(1)).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    // Snapshot the p=1 baseline so the subsequent updates persist as WAL deltas, not folded into the
    // snapshot -- the recovery path most likely to rebuild a version chain.
    ASSERT_TRUE(store->CreateSnapshot(/*force=*/true).has_value());

    // Three more separate committed transactions: p = 2, 3, 4. In a live store this is a four-version
    // MVCC delta chain on the vertex.
    for (const int value : {2, 3, 4}) {
      auto acc = store->Access(memgraph::storage::WRITE);
      auto vertex = acc->FindVertex(gid, View::OLD);
      ASSERT_TRUE(vertex.has_value());
      ASSERT_TRUE(vertex->SetProperty(p, PropertyValue(value)).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    store.reset();
  }

  // Restart with the flag ON and recover from snapshot + WAL.
  {
    Config config{};
    config.durability.storage_directory = storage_directory;
    config.durability.recover_on_startup = true;
    config.durability.snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    config.experimental_lockfree_read_snapshot = true;

    auto store = std::make_unique<InMemoryStorage>(config);
    const auto p = store->NameToProperty("p");

    // Non-vacuous: assert the recovery watermark was reseeded. A zero means Fix B
    // (the last_committed_mvcc_ts_ <- last-durable-timestamp reseed in storage.cpp)
    // regressed. The value/count assertions below still pass even without Fix B because
    // in-memory recovery leaves delta()==nullptr (FLAT vertices), so this is the only
    // assertion that directly catches the regression.
    const uint64_t watermark = store->LastCommittedMvccTimestamp();
    EXPECT_GT(watermark, 0u) << "Recovery watermark not reseeded: last_committed_mvcc_ts_ is 0 after restart. "
                                "The reseed (last_committed_mvcc_ts_ <- last-durable-timestamp) in storage.cpp "
                                "is missing or not reached -- Fix B regressed.";

    auto reader = store->Access(memgraph::storage::READ);

    // Exactly one vertex survived recovery: the repeated updates targeted the same object.
    EXPECT_EQ(CountVertices(*reader), 1);

    auto vertex = reader->FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex.has_value());
    auto value = vertex->GetProperty(p, View::OLD);
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(value->ValueInt(), 4)
        << "RECOVERED-READ STALE: a post-restart reader read an older version of a multiply-updated "
           "vertex instead of the latest. If recovery ever begins reconstructing a committed delta "
           "chain, this means the reader's snapshot_ts is below the head commit ts -- i.e. the "
           "recovery watermark restore (last_committed_mvcc_ts_ <- last-durable-timestamp) is missing.";
  }
}
