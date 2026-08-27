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
