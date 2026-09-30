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

// Commit-lock-narrowing suite: snapshot visibility, GC horizon, abort semantics and cross-flag durability
// for experimental_commit_lock_narrowing. _AB tests loop over both flag states.

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <semaphore>
#include <string>
#include <thread>
#include <variant>
#include <vector>

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

// Handshake wait with a timeout: a regression that never reaches the probe fails the test
// instead of hanging the binary forever on a bare acquire().
void AcquireOrFail(std::binary_semaphore &sem) {
  ASSERT_TRUE(sem.try_acquire_for(std::chrono::seconds(10))) << "semaphore handshake timed out";
}

std::unique_ptr<InMemoryStorage> MakeStorage(bool flag_on) {
  Config config{};
  config.gc.type = Config::Gc::Type::NONE;
  config.experimental_commit_lock_narrowing = flag_on;
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

// PERIODIC GC with a 3600s interval so it never auto-fires; collections are driven via RunGc.
std::unique_ptr<InMemoryStorage> MakeStorageManualGc(bool flag_on) {
  Config config{};
  config.gc.type = Config::Gc::Type::PERIODIC;
  config.gc.interval = std::chrono::seconds(3600);
  config.experimental_commit_lock_narrowing = flag_on;
  return std::make_unique<InMemoryStorage>(config);
}

// Synchronous GC pass with an EMPTY guard: readers stay open across the pass and hold main_lock_
// SHARED, so an adopted UNIQUE hold (as in storage_v2_gc.cpp) would deadlock.
void RunGc(InMemoryStorage &s) { s.FreeMemory({}, false); }

// Overwrites "p" of `gid` in a fresh WRITE accessor and commits, appending a delta-chain version.
void CommitProp(InMemoryStorage &store, Gid gid, int value) {
  auto acc = store.Access(memgraph::storage::WRITE);
  auto vertex = acc->FindVertex(gid, View::OLD);
  ASSERT_TRUE(vertex.has_value());
  ASSERT_TRUE(vertex->SetProperty(store.NameToProperty("p"), PropertyValue(value)).has_value());
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// Installs UNIQUE(label, "p") so a later duplicate commit aborts after minting its timestamp.
void CreateUniquePConstraint(InMemoryStorage &store, LabelId label) {
  auto acc = store.ReadOnlyAccess();
  auto res = acc->CreateUniqueConstraint(label, {store.NameToProperty("p")});
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ(res.value(), UniqueConstraints::CreationStatus::SUCCESS);
  EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

Gid CommitLabeledVertex(InMemoryStorage &store, LabelId label, int value) {
  auto acc = store.Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();
  const auto gid = vertex.Gid();
  EXPECT_TRUE(vertex.AddLabel(label).has_value());
  EXPECT_TRUE(vertex.SetProperty(store.NameToProperty("p"), PropertyValue(value)).has_value());
  EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  return gid;
}

int64_t CountVertices(Accessor &acc) {
  int64_t n = 0;
  for (auto vertex : acc.Vertices(View::OLD)) {
    (void)vertex;
    ++n;
  }
  return n;
}

}  // namespace

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

// Head commit at/below the writer's snapshot must not be a false conflict.
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

// Rewriting the txn's own uncommitted delta is not a conflict.
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

// Under OFF, W starts after C commits, so it sees X=2 and writes on top (no lost update, no retry).
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
  EXPECT_EQ(ReadProp(*w, gid), 2);
  auto w_vertex = w->FindVertex(gid, View::OLD);
  ASSERT_TRUE(w_vertex.has_value());
  ASSERT_TRUE(w_vertex->SetProperty(store->NameToProperty("p"), PropertyValue(3)).has_value());
  ASSERT_TRUE(w->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  auto reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*reader, gid), 3);
}

// A reader pinned at X=1 keeps its version across two later commits plus GC; after release a fresh
// reader sees 3.
TEST(LockFreeReadSnapshot, LongReaderVersionRetainedAcrossGc_AB) {
  for (const bool flag_on : {false, true}) {
    auto store = MakeStorageManualGc(flag_on);
    const auto gid = CreateVertexWithProp(*store, 1);

    auto long_reader = store->Access(memgraph::storage::READ);
    EXPECT_EQ(ReadProp(*long_reader, gid), 1) << "flag_on=" << flag_on;

    CommitProp(*store, gid, 2);
    CommitProp(*store, gid, 3);

    RunGc(*store);

    // GC must not reclaim past the oldest active snapshot.
    EXPECT_EQ(ReadProp(*long_reader, gid), 1)
        << "GC reclaimed the long reader's snapshot version (X=1) (flag_on=" << flag_on << ").";

    long_reader.reset();
    RunGc(*store);

    auto fresh_reader = store->Access(memgraph::storage::READ);
    EXPECT_EQ(ReadProp(*fresh_reader, gid), 3) << "flag_on=" << flag_on;
  }
}

// Horizon = min(active snapshot_ts) = R1's snapshot, protecting both older versions; releasing R1
// lifts the floor to R2.
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

  EXPECT_EQ(ReadProp(*r1, gid), 1)
      << "GC OVER-RECLAIM: R1 lost its snapshot version (X=1); horizon advanced past the oldest active snapshot_ts.";
  EXPECT_EQ(ReadProp(*r2, gid), 2)
      << "GC OVER-RECLAIM: R2 lost its snapshot version (X=2) while R1 still pinned an older horizon.";

  r1.reset();
  RunGc(*store);

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

// Stress: writers, SI readers and a concurrent GC thread race on the commit windows and watermark.
// A repeated read within one accessor must be stable and every commit visible at the end.
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
      const int value = writer_id * kWritesPerWriter + i;
      ASSERT_TRUE(vertex.SetProperty(p, PropertyValue(value)).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value())
          << "concurrent fresh-vertex commit failed unexpectedly (writer " << writer_id << ", iter " << i << ")";
      committed.fetch_add(1, std::memory_order_relaxed);
    }
  };

  auto reader_fn = [&] {
    while (!stop.load(std::memory_order_relaxed)) {
      auto r = store->Access(memgraph::storage::READ);

      // A frozen SI snapshot must not shift under concurrent commits/GC.
      std::optional<memgraph::storage::VertexAccessor> first;
      for (auto vertex : r->Vertices(View::OLD)) {
        first.emplace(vertex);
        break;
      }
      if (first.has_value()) {
        auto v1 = first->GetProperty(p, View::OLD);
        ASSERT_TRUE(v1.has_value());
        const int64_t read1 = v1->ValueInt();
        int64_t churn = 0;
        for (int k = 0; k < 32; ++k) churn += k;
        (void)churn;
        auto v2 = first->GetProperty(p, View::OLD);
        ASSERT_TRUE(v2.has_value());
        ASSERT_EQ(read1, v2->ValueInt()) << "two reads of \"p\" within one SI accessor returned different values.";
      }

      for (auto vertex : r->Vertices(View::OLD)) {
        auto value = vertex.GetProperty(p, View::OLD);
        ASSERT_TRUE(value.has_value());
        (void)value->ValueInt();
      }
    }
  };

  auto gc_fn = [&] {
    while (!stop.load(std::memory_order_relaxed)) {
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

  stop.store(true, std::memory_order_relaxed);
  for (auto &r : readers) r.join();
  gc.join();

  ASSERT_EQ(committed.load(), static_cast<uint64_t>(kWriters) * kWritesPerWriter);

  auto final_reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(CountVertices(*final_reader), static_cast<int64_t>(committed.load()))
      << "fresh-snapshot vertex count differs from the number of committed transactions.";
}

// A committer that mints a ts and then aborts (UNIQUE violation, never reaching FinalizeCommitPhase)
// must not advance last_committed_mvcc_ts_: the aborted vertex stays invisible to readers opened before
// and after it, and later commits proceed.
TEST(LockFreeReadSnapshot, AbortAfterMint_DoesNotAdvanceWatermark_AB) {
  for (const bool flag_on : {false, true}) {
    auto store = MakeStorage(flag_on);
    const auto label = store->NameToLabel("L");
    CreateUniquePConstraint(*store, label);

    const auto gid_a = CommitLabeledVertex(*store, label, 1);

    auto reader_before = store->Access(memgraph::storage::READ);
    EXPECT_EQ(ReadProp(*reader_before, gid_a), 1) << "flag_on=" << flag_on;
    EXPECT_EQ(CountVertices(*reader_before), 1) << "flag_on=" << flag_on;

    std::optional<Gid> gid_b;
    {
      auto w = store->Access(memgraph::storage::WRITE);
      auto vertex = w->CreateVertex();
      gid_b = vertex.Gid();
      ASSERT_TRUE(vertex.AddLabel(label).has_value());
      ASSERT_TRUE(vertex.SetProperty(store->NameToProperty("p"), PropertyValue(1)).has_value());
      auto res = w->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
      ASSERT_FALSE(res.has_value()) << "the duplicate commit must fail the UNIQUE constraint (flag_on=" << flag_on
                                    << ")";
      EXPECT_EQ(std::get<ConstraintViolation>(res.error()).type, ConstraintViolation::Type::UNIQUE);
    }

    EXPECT_EQ(ReadProp(*reader_before, gid_a), 1) << "flag_on=" << flag_on;
    EXPECT_EQ(CountVertices(*reader_before), 1)
        << "ABORTED-TXN LEAK (flag_on=" << flag_on
        << "): a reader opened before the failed commit observed the aborted vertex B.";
    EXPECT_FALSE(reader_before->FindVertex(*gid_b, View::OLD).has_value()) << "flag_on=" << flag_on;

    {
      auto fresh = store->Access(memgraph::storage::READ);
      EXPECT_EQ(ReadProp(*fresh, gid_a), 1) << "flag_on=" << flag_on;
      EXPECT_EQ(CountVertices(*fresh), 1)
          << "ABORTED-TXN LEAK (flag_on=" << flag_on
          << "): a reader opened after the failed commit saw the aborted vertex B; the read watermark "
             "must not advance on abort (last_committed_mvcc_ts_ must not move to B's wasted commit timestamp).";
      EXPECT_FALSE(fresh->FindVertex(*gid_b, View::OLD).has_value()) << "flag_on=" << flag_on;
    }

    const auto gid_c = CommitLabeledVertex(*store, label, 2);
    {
      auto fresh = store->Access(memgraph::storage::READ);
      EXPECT_EQ(ReadProp(*fresh, gid_a), 1) << "flag_on=" << flag_on;
      EXPECT_EQ(ReadProp(*fresh, gid_c), 2) << "flag_on=" << flag_on;
      EXPECT_EQ(CountVertices(*fresh), 2) << "flag_on=" << flag_on;
      EXPECT_FALSE(fresh->FindVertex(*gid_b, View::OLD).has_value()) << "flag_on=" << flag_on;
    }
  }
}

// The mint->abort window cannot be parked deterministically, so stress it: a committer loops aborting
// duplicates while the main thread opens readers; every snapshot must contain only A.
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
    AcquireOrFail(start);
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

// Cross-flag recovery: data written with the flag ON must recover with it OFF and vice versa; the flag
// must not leak into durable state.

namespace {

void WriteDurable(const std::filesystem::path &dir, bool flag_on) {
  Config config{};
  config.durability.storage_directory = dir;
  config.durability.recover_on_startup = false;
  config.durability.snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  config.experimental_commit_lock_narrowing = flag_on;

  auto store = std::make_unique<InMemoryStorage>(config);
  {
    auto acc = store->Access(memgraph::storage::WRITE);
    for (int i = 0; i < 5; ++i) {
      auto vertex = acc->CreateVertex();
      ASSERT_TRUE(vertex.SetProperty(store->NameToProperty("p"), PropertyValue(i)).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  ASSERT_TRUE(store->CreateSnapshot(/*force=*/true).has_value());
  store.reset();
}

void RecoverAndCheck(const std::filesystem::path &dir, bool flag_on, int expected_count) {
  Config config{};
  config.durability.storage_directory = dir;
  config.durability.recover_on_startup = true;
  config.durability.snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  config.experimental_commit_lock_narrowing = flag_on;

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

  EXPECT_EQ(props, expected) << "recovered vertex/property set differs from what was written.";
}

class LockFreeReadSnapshotRecovery : public ::testing::Test {
 protected:
  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

  void Clear() {
    if (std::filesystem::exists(storage_directory)) std::filesystem::remove_all(storage_directory);
  }

  std::filesystem::path storage_directory{
      std::filesystem::temp_directory_path() /
      ("MG_test_unit_storage_v2_commit_lock_narrowing_" +
       std::string(::testing::UnitTest::GetInstance()->current_test_info()->name()))};
};

}  // namespace

TEST_F(LockFreeReadSnapshotRecovery, WriteOn_RecoverOff_DataIntact) {
  WriteDurable(storage_directory, /*flag_on=*/true);
  RecoverAndCheck(storage_directory, /*flag_on=*/false, 5);
}

TEST_F(LockFreeReadSnapshotRecovery, WriteOff_RecoverOn_DataIntact) {
  WriteDurable(storage_directory, /*flag_on=*/false);
  RecoverAndCheck(storage_directory, /*flag_on=*/true, 5);
}

TEST_F(LockFreeReadSnapshotRecovery, WriteOn_RecoverOn_DataIntact) {
  WriteDurable(storage_directory, /*flag_on=*/true);
  RecoverAndCheck(storage_directory, /*flag_on=*/true, 5);
}

TEST_F(LockFreeReadSnapshotRecovery, WriteOff_RecoverOff_DataIntact) {
  WriteDurable(storage_directory, /*flag_on=*/false);
  RecoverAndCheck(storage_directory, /*flag_on=*/false, 5);
}

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

// Writers commit (L, p) vertices while the main thread builds the index; afterwards every committed
// vertex must be indexed (seeded ones via PopulateIndex, later ones via the commit-time update).
TEST(LockFreeReadSnapshot, IndexCreate_CompleteUnderConcurrentWrites_AB) {
  for (const bool flag_on : {false, true}) {
    auto store = MakeStorage(flag_on);
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
      ASSERT_TRUE(res.has_value()) << "CreateIndex failed unexpectedly (flag_on=" << flag_on << ").";
      ASSERT_TRUE(idx_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    for (auto &w : writers) w.join();

    const int64_t total = kSeed + committed.load(std::memory_order_relaxed);

    auto reader = store->Access(memgraph::storage::READ);
    const int64_t full_count = CountVertices(*reader);
    ASSERT_EQ(full_count, total) << "FULL SCAN BUG (flag_on=" << flag_on << "): expected " << total
                                 << " committed vertices.";

    const int64_t index_count = CountViaLabelPropertyIndex(*reader, label, prop);
    EXPECT_EQ(index_count, full_count)
        << "INDEX COMPLETENESS FAILURE (flag_on=" << flag_on << "): " << (full_count - index_count) << " of "
        << full_count
        << " committed (L,P) vertices are missing from the label-property index. "
           "Pre-seeded vertices must be captured by PopulateIndex; vertices committed after "
           "CreateIndex must be picked up by the commit-time index update.";
  }
}

namespace {

// WAL-enabled (a STRICT_SYNC replica prepare needs an open WAL file); GC runs only via RunGc.
std::unique_ptr<InMemoryStorage> MakeWalStorageManualGc(const std::filesystem::path &dir) {
  Config config{};
  config.durability.storage_directory = dir;
  config.durability.recover_on_startup = false;
  config.durability.snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  config.gc.type = Config::Gc::Type::PERIODIC;
  config.gc.interval = std::chrono::seconds(3600);
  config.experimental_commit_lock_narrowing = true;
  return std::make_unique<InMemoryStorage>(config);
}

struct PreparedCommit {
  std::unique_ptr<Accessor> writer;
  uint64_t durable_ts{0};
};

// Sets "p" = value through a STRICT_SYNC replica prepare: the commit ts is minted and engine_lock_ released,
// but nothing is published, so a BEGIN now lands inside this commit's window.
void PrepareWithoutPublish(InMemoryStorage &store, Gid gid, int value, PreparedCommit &out) {
  out.durable_ts = store.LastCommittedMvccTimestamp() + 1;
  out.writer = store.Access(memgraph::storage::WRITE);
  auto vertex = out.writer->FindVertex(gid, View::OLD);
  ASSERT_TRUE(vertex.has_value());
  ASSERT_TRUE(vertex->SetProperty(store.NameToProperty("p"), PropertyValue(value)).has_value());
  ASSERT_TRUE(out.writer
                  ->PrepareForCommitPhase(memgraph::storage::CommitArgs::make_replica_write(
                      out.durable_ts, /*two_phase_commit=*/true, [] {}))
                  .has_value());
}

// Publishes without re-minting (as the narrowed main does), then ends the writer so its ts is marked finished.
void PublishAndEnd(InMemoryStorage &store, PreparedCommit &commit) {
  auto engine_guard = std::unique_lock{store.engine_lock_};
  static_cast<InMemoryStorage::InMemoryAccessor &>(*commit.writer).FinalizeCommitPhase(commit.durable_ts, engine_guard);
  commit.writer.reset();
}

void CommitDelete(InMemoryStorage &store, Gid gid) {
  auto acc = store.Access(memgraph::storage::WRITE);
  auto victim = acc->FindVertex(gid, View::OLD);
  ASSERT_TRUE(victim.has_value());
  ASSERT_TRUE(acc->DeleteVertex(&*victim).has_value());
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

}  // namespace

// A reader that BEGINs between a commit's mint and publish has snapshot < c < start; GC must keep c's
// pre-image while it lives, and resume reclaiming once it ends.
TEST_F(LockFreeReadSnapshotRecovery, CommitWindowReader_RetainsPreImageAcrossGc_ON) {
  auto store = MakeWalStorageManualGc(storage_directory);
  const auto gid = CreateVertexWithProp(*store, 1);
  const auto victim_gid = CreateVertexWithProp(*store, 1);

  PreparedCommit commit;
  PrepareWithoutPublish(*store, gid, 2, commit);
  auto reader = store->Access(memgraph::storage::READ);
  ASSERT_EQ(ReadProp(*reader, gid), 1);
  PublishAndEnd(*store, commit);

  RunGc(*store);
  EXPECT_EQ(ReadProp(*reader, gid), 1) << "GC reclaimed the pre-image of a commit whose window the oldest reader "
                                          "began in; the horizon must stay at that commit's ts.";
  {
    auto fresh = store->Access(memgraph::storage::READ);
    EXPECT_EQ(ReadProp(*fresh, gid), 2);
  }

  CommitDelete(*store, victim_gid);
  RunGc(*store);
  EXPECT_EQ(store->VertexStoreSize(), 2U) << "victim deleted after the reader began must survive while it is live";

  reader.reset();
  RunGc(*store);
  RunGc(*store);
  EXPECT_EQ(store->VertexStoreSize(), 1U) << "GC must reclaim once the window reader has ended";
}

// The oldest reader began in an earlier window C1; a later window C2 (reader R2) publishes and R2 ends.
// GC must still hold the horizon at C1.
TEST_F(LockFreeReadSnapshotRecovery, EarlierWindowReader_RetainsPreImageAcrossGc_ON) {
  auto store = MakeWalStorageManualGc(storage_directory);
  const auto gid = CreateVertexWithProp(*store, 1);
  const auto victim_gid = CreateVertexWithProp(*store, 1);

  PreparedCommit c1;
  PrepareWithoutPublish(*store, gid, 2, c1);
  auto r1 = store->Access(memgraph::storage::READ);
  ASSERT_EQ(ReadProp(*r1, gid), 1);
  PublishAndEnd(*store, c1);

  PreparedCommit c2;
  PrepareWithoutPublish(*store, gid, 3, c2);
  auto r2 = store->Access(memgraph::storage::READ);
  ASSERT_EQ(ReadProp(*r2, gid), 2);
  PublishAndEnd(*store, c2);

  r2.reset();
  RunGc(*store);
  EXPECT_EQ(ReadProp(*r1, gid), 1) << "GC reclaimed the pre-image of the earlier commit window the oldest reader "
                                      "began in; the horizon must stay at that commit's ts.";

  CommitDelete(*store, victim_gid);
  RunGc(*store);
  EXPECT_EQ(store->VertexStoreSize(), 2U) << "victim deleted after R1 began must survive while R1 is live";
  EXPECT_EQ(ReadProp(*r1, gid), 1);

  r1.reset();
  RunGc(*store);
  RunGc(*store);
  EXPECT_EQ(store->VertexStoreSize(), 1U) << "GC must reclaim once the window reader has ended";
}

// Under the flag every txn publishes its snapshot_ts, so an open READ_COMMITTED reader must not pin the GC
// horizon at its floor: GC must still reclaim old versions, and the RC reader must stay valid.
TEST(LockFreeReadSnapshot, NonSiReaderDoesNotPinGcFloorLow_ON) {
  auto store = MakeStorageManualGc(/*flag_on=*/true);
  const auto gid = CreateVertexWithProp(*store, 1);
  const auto victim_gid = CreateVertexWithProp(*store, 1);

  // An older SI reader stops the commits below from being discarded at commit time.
  auto si_holder = store->Access(memgraph::storage::READ);
  {
    auto acc = store->Access(memgraph::storage::WRITE);
    auto victim = acc->FindVertex(victim_gid, View::OLD);
    ASSERT_TRUE(victim.has_value());
    ASSERT_TRUE(acc->DeleteVertex(&*victim).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  CommitProp(*store, gid, 2);
  CommitProp(*store, gid, 3);

  auto rc_reader =
      store->Access(memgraph::storage::READ, memgraph::storage::IsolationLevel::READ_COMMITTED, std::nullopt);
  si_holder.reset();

  EXPECT_EQ(ReadProp(*rc_reader, gid), 3);

  CommitProp(*store, gid, 4);

  // RC sees p=4; an SI reader would still see 3.
  EXPECT_EQ(ReadProp(*rc_reader, gid), 4)
      << "RC ISOLATION MISS: expected p=4 (latest committed) but got a stale value. "
         "The Access(READ, IsolationLevel::READ_COMMITTED, ...) override did not take effect; "
         "the accessor is frozen like a SNAPSHOT_ISOLATION reader. Check that "
         "transaction.commit_lock_narrowing is false for RC and that View::OLD re-snapshots per read.";

  ASSERT_EQ(store->VertexStoreSize(), 2U);
  RunGc(*store);
  EXPECT_EQ(store->VertexStoreSize(), 1U)
      << "GC RECLAIMED NOTHING while an RC reader is open: the deleted vertex must be reclaimed once the "
         "horizon passes it.";

  EXPECT_EQ(ReadProp(*rc_reader, gid), 4)
      << "RC READER BROKEN AFTER GC: expected p=4 (current head) but the RC accessor returned "
         "a wrong value or crashed. GC must not reclaim the current head (p=4 has no successor).";

  rc_reader.reset();
  RunGc(*store);

  auto fresh_reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*fresh_reader, gid), 4);
}

// Recovery must reseed last_committed_mvcc_ts_ from the last durable timestamp, else SI readers freeze at 0.
// Recovery builds flat vertices (delta()==nullptr), so value checks pass regardless; the watermark
// assertion is the real guard.
TEST_F(LockFreeReadSnapshotRecovery, RecoveredUpdateChain_ReadsLatestUnderFlagOn) {
  Gid gid{};
  {
    Config config{};
    config.durability.storage_directory = storage_directory;
    config.durability.recover_on_startup = false;
    config.durability.snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    config.experimental_commit_lock_narrowing = true;

    auto store = std::make_unique<InMemoryStorage>(config);
    const auto p = store->NameToProperty("p");

    {
      auto acc = store->Access(memgraph::storage::WRITE);
      auto vertex = acc->CreateVertex();
      gid = vertex.Gid();
      ASSERT_TRUE(vertex.SetProperty(p, PropertyValue(1)).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    // Snapshot p=1 so the later updates persist as WAL deltas.
    ASSERT_TRUE(store->CreateSnapshot(/*force=*/true).has_value());

    for (const int value : {2, 3, 4}) {
      auto acc = store->Access(memgraph::storage::WRITE);
      auto vertex = acc->FindVertex(gid, View::OLD);
      ASSERT_TRUE(vertex.has_value());
      ASSERT_TRUE(vertex->SetProperty(p, PropertyValue(value)).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    store.reset();
  }

  {
    Config config{};
    config.durability.storage_directory = storage_directory;
    config.durability.recover_on_startup = true;
    config.durability.snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    config.experimental_commit_lock_narrowing = true;

    auto store = std::make_unique<InMemoryStorage>(config);
    const auto p = store->NameToProperty("p");

    // Only this catches a missing reseed: recovered vertices are flat, so the value checks pass anyway.
    const uint64_t watermark = store->LastCommittedMvccTimestamp();
    EXPECT_GT(watermark, 0u) << "last_committed_mvcc_ts_ not reseeded after recovery (is 0).";

    auto reader = store->Access(memgraph::storage::READ);

    EXPECT_EQ(CountVertices(*reader), 1);

    auto vertex = reader->FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex.has_value());
    auto value = vertex->GetProperty(p, View::OLD);
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(value->ValueInt(), 4)
        << "post-restart reader read an older version than the latest; snapshot_ts is below the head commit ts.";
  }
}

// A STRICT_SYNC replica prepare mints a commit ts but publishes only on FinalizeCommitRpc; destroying the
// accessor before that must release the ts, else it pins the GC horizon forever.
TEST_F(LockFreeReadSnapshotRecovery, DestroyedPreparedReplicaAccessorReleasesCommitTs) {
  for (const bool flag_on : {false, true}) {
    Config config{};
    config.durability.storage_directory = storage_directory / (flag_on ? "on" : "off");
    config.durability.recover_on_startup = false;
    config.durability.snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    config.gc.type = Config::Gc::Type::PERIODIC;
    config.gc.interval = std::chrono::seconds(3600);
    config.experimental_commit_lock_narrowing = flag_on;
    auto store = std::make_unique<InMemoryStorage>(config);

    const auto victim_gid = CreateVertexWithProp(*store, 1);
    {
      auto acc = store->Access(memgraph::storage::WRITE);
      auto vertex = acc->CreateVertex();
      ASSERT_TRUE(vertex.SetProperty(store->NameToProperty("p"), PropertyValue(2)).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::storage::CommitArgs::make_replica_write(
                                                 /*desired_commit_timestamp=*/1, /*two_phase_commit=*/true, [] {}))
                      .has_value());
    }

    {
      auto acc = store->Access(memgraph::storage::WRITE);
      auto victim = acc->FindVertex(victim_gid, View::OLD);
      ASSERT_TRUE(victim.has_value());
      ASSERT_TRUE(acc->DeleteVertex(&*victim).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    RunGc(*store);
    RunGc(*store);
    EXPECT_EQ(store->VertexStoreSize(), 0U) << "LEAKED COMMIT TS (flag_on=" << flag_on
                                            << "): destroyed prepared accessor left its ts active in commit_log_.";
  }
}
