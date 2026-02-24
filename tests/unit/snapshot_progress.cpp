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

#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "storage/v2/snapshot_progress.hpp"

using memgraph::storage::BatchedProgressCounter;
using memgraph::storage::SnapshotProgress;
using memgraph::storage::SnapshotProgressSnapshot;

// --- SnapshotProgress tests ---

TEST(SnapshotProgressTest, InitialStateIsIdle) {
  SnapshotProgress progress;
  EXPECT_EQ(progress.phase.load(), SnapshotProgress::Phase::IDLE);
  EXPECT_EQ(progress.items_done.load(), 0);
  EXPECT_EQ(progress.items_total.load(), 0);
  EXPECT_EQ(progress.start_time_us.load(), 0);
}

TEST(SnapshotProgressTest, StartSetsTimestamp) {
  SnapshotProgress progress;
  progress.Start();
  EXPECT_GT(progress.start_time_us.load(), 0);
}

TEST(SnapshotProgressTest, SetPhaseUpdatesAllFields) {
  SnapshotProgress progress;
  progress.items_done.store(42);

  progress.SetPhase(SnapshotProgress::Phase::EDGES, 1000);
  EXPECT_EQ(progress.phase.load(), SnapshotProgress::Phase::EDGES);
  EXPECT_EQ(progress.items_done.load(), 0);
  EXPECT_EQ(progress.items_total.load(), 1000);
}

TEST(SnapshotProgressTest, IncrementDoneDefaultIncrement) {
  SnapshotProgress progress;
  progress.IncrementDone();
  EXPECT_EQ(progress.items_done.load(), 1);
  progress.IncrementDone();
  EXPECT_EQ(progress.items_done.load(), 2);
}

TEST(SnapshotProgressTest, IncrementDoneByN) {
  SnapshotProgress progress;
  progress.IncrementDone(10);
  EXPECT_EQ(progress.items_done.load(), 10);
  progress.IncrementDone(5);
  EXPECT_EQ(progress.items_done.load(), 15);
}

TEST(SnapshotProgressTest, ResetClearsAllFields) {
  SnapshotProgress progress;
  progress.Start();
  progress.SetPhase(SnapshotProgress::Phase::VERTICES, 500);
  progress.IncrementDone(100);

  progress.Reset();
  EXPECT_EQ(progress.phase.load(), SnapshotProgress::Phase::IDLE);
  EXPECT_EQ(progress.items_done.load(), 0);
  EXPECT_EQ(progress.items_total.load(), 0);
  EXPECT_EQ(progress.start_time_us.load(), 0);
}

TEST(SnapshotProgressTest, PhaseToStringAllValues) {
  EXPECT_STREQ(SnapshotProgress::PhaseToString(SnapshotProgress::Phase::IDLE), "idle");
  EXPECT_STREQ(SnapshotProgress::PhaseToString(SnapshotProgress::Phase::EDGES), "edges");
  EXPECT_STREQ(SnapshotProgress::PhaseToString(SnapshotProgress::Phase::VERTICES), "vertices");
  EXPECT_STREQ(SnapshotProgress::PhaseToString(SnapshotProgress::Phase::INDICES), "indices");
  EXPECT_STREQ(SnapshotProgress::PhaseToString(SnapshotProgress::Phase::CONSTRAINTS), "constraints");
  EXPECT_STREQ(SnapshotProgress::PhaseToString(SnapshotProgress::Phase::FINALIZING), "finalizing");
}

TEST(SnapshotProgressTest, PhaseTransitionsFullLifecycle) {
  SnapshotProgress progress;
  progress.Start();

  progress.SetPhase(SnapshotProgress::Phase::EDGES, 100);
  EXPECT_EQ(progress.phase.load(), SnapshotProgress::Phase::EDGES);

  progress.SetPhase(SnapshotProgress::Phase::VERTICES, 200);
  EXPECT_EQ(progress.phase.load(), SnapshotProgress::Phase::VERTICES);
  EXPECT_EQ(progress.items_done.load(), 0);
  EXPECT_EQ(progress.items_total.load(), 200);

  progress.SetPhase(SnapshotProgress::Phase::INDICES, 0);
  EXPECT_EQ(progress.phase.load(), SnapshotProgress::Phase::INDICES);

  progress.SetPhase(SnapshotProgress::Phase::CONSTRAINTS, 0);
  EXPECT_EQ(progress.phase.load(), SnapshotProgress::Phase::CONSTRAINTS);

  progress.SetPhase(SnapshotProgress::Phase::FINALIZING, 0);
  EXPECT_EQ(progress.phase.load(), SnapshotProgress::Phase::FINALIZING);

  progress.Reset();
  EXPECT_EQ(progress.phase.load(), SnapshotProgress::Phase::IDLE);
}

// --- BatchedProgressCounter tests ---

TEST(BatchedProgressCounterTest, NullProgressIsSafe) {
  // Should not crash with nullptr
  BatchedProgressCounter counter(nullptr);
  counter.Increment();
  counter.Increment();
  counter.Flush();
}

TEST(BatchedProgressCounterTest, FlushOnDestruction) {
  SnapshotProgress progress;
  progress.SetPhase(SnapshotProgress::Phase::VERTICES, 100);

  {
    BatchedProgressCounter counter(&progress);
    for (int i = 0; i < 10; ++i) {
      counter.Increment();
    }
    // No explicit flush — destructor should handle it
  }

  EXPECT_EQ(progress.items_done.load(), 10);
}

TEST(BatchedProgressCounterTest, FlushAtBatchSize) {
  SnapshotProgress progress;
  progress.SetPhase(SnapshotProgress::Phase::VERTICES, 1000);

  BatchedProgressCounter counter(&progress);

  // Increment exactly kBatchSize times
  for (uint64_t i = 0; i < BatchedProgressCounter::kBatchSize; ++i) {
    counter.Increment();
  }

  // Should have flushed at the batch boundary
  EXPECT_EQ(progress.items_done.load(), BatchedProgressCounter::kBatchSize);

  // Increment a few more
  for (int i = 0; i < 5; ++i) {
    counter.Increment();
  }

  // Not yet flushed (only 5 items, below batch size)
  EXPECT_EQ(progress.items_done.load(), BatchedProgressCounter::kBatchSize);

  // Explicit flush
  counter.Flush();
  EXPECT_EQ(progress.items_done.load(), BatchedProgressCounter::kBatchSize + 5);
}

TEST(BatchedProgressCounterTest, MultipleBatches) {
  SnapshotProgress progress;
  progress.SetPhase(SnapshotProgress::Phase::EDGES, 10000);

  constexpr uint64_t total_items = BatchedProgressCounter::kBatchSize * 3 + 17;
  {
    BatchedProgressCounter counter(&progress);
    for (uint64_t i = 0; i < total_items; ++i) {
      counter.Increment();
    }
  }

  EXPECT_EQ(progress.items_done.load(), total_items);
}

TEST(BatchedProgressCounterTest, DoubleFlushIsSafe) {
  SnapshotProgress progress;
  progress.SetPhase(SnapshotProgress::Phase::VERTICES, 100);

  BatchedProgressCounter counter(&progress);
  for (int i = 0; i < 10; ++i) {
    counter.Increment();
  }

  counter.Flush();
  EXPECT_EQ(progress.items_done.load(), 10);

  // Second flush should be a no-op (local_count is 0)
  counter.Flush();
  EXPECT_EQ(progress.items_done.load(), 10);
}

TEST(BatchedProgressCounterTest, ZeroIncrements) {
  SnapshotProgress progress;
  progress.SetPhase(SnapshotProgress::Phase::VERTICES, 100);

  {
    BatchedProgressCounter counter(&progress);
    // No increments
  }

  EXPECT_EQ(progress.items_done.load(), 0);
}

TEST(BatchedProgressCounterTest, ConcurrentBatchedCounters) {
  SnapshotProgress progress;
  constexpr uint64_t items_per_thread = 10000;
  constexpr unsigned n_threads = 4;

  progress.SetPhase(SnapshotProgress::Phase::VERTICES, items_per_thread * n_threads);

  std::vector<std::thread> threads;
  threads.reserve(n_threads);
  for (unsigned t = 0; t < n_threads; ++t) {
    threads.emplace_back([&progress] {
      BatchedProgressCounter counter(&progress);
      for (uint64_t i = 0; i < items_per_thread; ++i) {
        counter.Increment();
      }
    });
  }
  for (auto &t : threads) t.join();

  EXPECT_EQ(progress.items_done.load(), items_per_thread * n_threads);
}
