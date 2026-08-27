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

#include <optional>
#include <semaphore>
#include <thread>

#include "storage/v2/commit_probe.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/resource_lock.hpp"

using memgraph::storage::Config;
using memgraph::storage::Gid;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::PropertyValue;
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
