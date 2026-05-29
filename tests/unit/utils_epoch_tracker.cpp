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

#include "gtest/gtest.h"

#include "utils/epoch_tracker.hpp"

// Acquire() returns epoch IDs monotonically from 0; CurrentEpoch() equals
// the count of Acquire() calls made so far.
TEST(EpochTracker, AcquireMonotonic) {
  using namespace memgraph::utils;
  EpochTracker tracker;

  EXPECT_EQ(tracker.CurrentEpoch(), 0u);

  EXPECT_EQ(tracker.Acquire(), 0u);
  EXPECT_EQ(tracker.Acquire(), 1u);
  EXPECT_EQ(tracker.Acquire(), 2u);

  EXPECT_EQ(tracker.CurrentEpoch(), 3u);

  // Release all acquired ids before destruction.
  tracker.Release(0);
  tracker.Release(1);
  tracker.Release(2);
}

// A fresh tracker with no released ids: guard 0 is always safe; any positive
// guard is not (no dead prefix has been established).
TEST(EpochTracker, EmptyTrackerSafe) {
  using namespace memgraph::utils;
  EpochTracker tracker;

  EXPECT_TRUE(tracker.IsSafeToFree(0));
  EXPECT_FALSE(tracker.IsSafeToFree(5));
}

// A single reader: reclamation is blocked while the reader is live and allowed
// once it releases.
TEST(EpochTracker, SingleReaderReclamation) {
  using namespace memgraph::utils;
  EpochTracker tracker;

  uint64_t id = tracker.Acquire();      // == 0
  uint64_t g = tracker.CurrentEpoch();  // == 1; recorded after the acquire

  EXPECT_FALSE(tracker.IsSafeToFree(g));  // reader 0 still live

  tracker.Release(id);

  EXPECT_TRUE(tracker.IsSafeToFree(g));
}

// Out-of-order releases: releasing higher ids first does NOT establish the
// dead prefix as long as id 0 is still live; releasing id 0 completes it.
TEST(EpochTracker, OutOfOrderRelease) {
  using namespace memgraph::utils;
  EpochTracker tracker;

  (void)tracker.Acquire();              // 0
  (void)tracker.Acquire();              // 1
  (void)tracker.Acquire();              // 2
  uint64_t g = tracker.CurrentEpoch();  // == 3

  tracker.Release(1);
  tracker.Release(2);
  EXPECT_FALSE(tracker.IsSafeToFree(g));  // contiguous prefix requires id 0

  tracker.Release(0);
  EXPECT_TRUE(tracker.IsSafeToFree(g));
}

// Partial prefix: releasing 0..2 makes guard 3 safe but not guard 4 (ids 3
// and 4 are still live).
TEST(EpochTracker, PartialPrefix) {
  using namespace memgraph::utils;
  EpochTracker tracker;

  (void)tracker.Acquire();  // 0
  (void)tracker.Acquire();  // 1
  (void)tracker.Acquire();  // 2
  (void)tracker.Acquire();  // 3
  (void)tracker.Acquire();  // 4

  tracker.Release(0);
  tracker.Release(1);
  tracker.Release(2);
  // ids 3 and 4 still live

  EXPECT_TRUE(tracker.IsSafeToFree(3));   // last_dead == 2 >= 3-1 == 2 → safe
  EXPECT_FALSE(tracker.IsSafeToFree(4));  // last_dead == 2 <  4-1 == 3 → not safe

  tracker.Release(3);
  tracker.Release(4);
}

// kIdsInField == 64 per the header (one full 64-bit field). Crossing one
// field boundary exercises the full-field fast path in ComputeLastDead.
TEST(EpochTracker, BlockBoundary64) {
  using namespace memgraph::utils;

  // Sub-case 1: release all 65 ids (0..64) — last_dead reaches 64.
  {
    EpochTracker t1;
    for (uint64_t i = 0; i <= 64; ++i) (void)t1.Acquire();
    for (uint64_t i = 0; i <= 64; ++i) t1.Release(i);
    EXPECT_TRUE(t1.IsSafeToFree(65));
  }

  // Sub-case 2: release 0..63 (one full field) but keep id 64 live.
  // Guard 64 is safe (last_dead == 63 >= 64-1); guard 65 is not.
  {
    EpochTracker t2;
    for (uint64_t i = 0; i <= 64; ++i) (void)t2.Acquire();
    for (uint64_t i = 0; i <= 63; ++i) t2.Release(i);
    // id 64 still live
    EXPECT_TRUE(t2.IsSafeToFree(64));   // last_dead == 63 >= 63 → safe
    EXPECT_FALSE(t2.IsSafeToFree(65));  // last_dead == 63 <  64 → not safe
    t2.Release(64);
  }
}

// kIdsInBlock == 4096 per the header; acquiring more than 4096 ids forces a
// second Block allocation inside Release().
TEST(EpochTracker, BlockAllocationAcross4096) {
  using namespace memgraph::utils;
  EpochTracker tracker;

  constexpr uint64_t kCount = 4097;  // one past a full block; ties to kIdsInBlock==4096 in header

  for (uint64_t i = 0; i < kCount; ++i) (void)tracker.Acquire();
  for (uint64_t i = 0; i < kCount; ++i) tracker.Release(i);

  EXPECT_TRUE(tracker.IsSafeToFree(kCount));  // every id released across both blocks
}

// C2 contract: the destructor (Clear()) may be called only after every id
// handed out by Acquire() has been Release()d.  Verify no crash and no leak
// (ASan/LSan in test config catches leaks).
TEST(EpochTracker, TeardownAllReleased) {
  using namespace memgraph::utils;

  {
    EpochTracker tracker;
    uint64_t ids[5];
    for (uint64_t i = 0; i < 5; ++i) ids[i] = tracker.Acquire();
    for (uint64_t i = 0; i < 5; ++i) tracker.Release(ids[i]);
    // tracker goes out of scope here; destructor calls Clear()
  }

  SUCCEED();  // reaching here means no crash and no assertion failure
}
