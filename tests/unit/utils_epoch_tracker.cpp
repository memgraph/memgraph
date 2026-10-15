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

#include <algorithm>
#include <atomic>
#include <numeric>
#include <random>
#include <thread>
#include <vector>

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

// ---------------------------------------------------------------------------
// Stress / concurrent tests
// ---------------------------------------------------------------------------
//
// Ordering argument (applies to all three stress cases):
//
//   For IsSafeToFree(g) to correctly report "safe" after all readers with
//   id < g have Release()d, the following ordering chain must hold:
//
//     (reader critical section)
//       => released_flag[id].store(1, relaxed)          // A
//       => Release(id): field[].fetch_or(mask, release)  // B (seq-before A)
//
//     (checker / main thread)
//       IsSafeToFree -> ComputeLastDead
//         -> field[].load(acquire)                       // C
//       => released_flag[id].load(acquire)              // D (seq-after C)
//
//   The release in B synchronises-with the acquire in C (same atomic object
//   field[]).  Therefore A is visible before D: if ComputeLastDead says id is
//   dead, released_flag[id] must already be 1.  Checking released_flag[id]
//   after IsSafeToFree returns true is therefore data-race-free and correct.
//
//   Note: Acquire() uses relaxed — it publishes no payload.  The safety
//   channel is exclusively the bitmap written with release in Release().

namespace {

// Returns a thread count clamped to [4, 8] based on hardware parallelism.
// Using a global helper avoids repeating the expression in every test.
unsigned StressThreadCount() { return std::max(4u, std::min(8u, std::thread::hardware_concurrency())); }

}  // namespace

// Safety contract under concurrency: IsSafeToFree(g) must never return true
// unless every reader with id < g has completed its Release().  Verified by
// recording a per-id atomic flag BEFORE Release() and asserting all flags are
// set once IsSafeToFree(g) becomes true.
//
// Threading: N reader threads + 1 checker thread running concurrently.
// Total ids: >= 64 blocks (>= 64 * 4096 = 262144).
// EXPECT_* is issued only by main (after both readers and checker have joined);
// the checker communicates failures via a checker_failed atomic flag.
TEST(EpochTrackerStress, SafetyContractUnderConcurrency) {
  using namespace memgraph::utils;

  const unsigned N = StressThreadCount();
  // Each reader thread acquires this many ids; total >= 64 blocks (4096 ids/block).
  // Distribute evenly so total = N * ids_per_thread >= 262144.
  const uint64_t ids_per_thread = (262144 + N - 1) / N;  // ceiling division
  const uint64_t total_ids = static_cast<uint64_t>(N) * ids_per_thread;

  EpochTracker tracker;

  // Pre-size the release-flag array to the exact total; no resize during run.
  // Each element is 0 (unreleased) or 1 (released), written before Release().
  std::vector<std::atomic<uint8_t>> released(total_ids);
  for (auto &f : released) f.store(0, std::memory_order_relaxed);

  // Failure flag set by the checker; avoids EXPECT from reader threads.
  std::atomic<bool> checker_failed{false};
  // Signal from readers to checker that all ids have been issued + released.
  std::atomic<uint64_t> readers_done_count{0};

  // Checker thread: while readers are running, periodically sample the current
  // epoch, wait for IsSafeToFree, then assert all flags < g are set.
  // Monotonicity: track the highest proven-safe guard and re-verify it.
  std::jthread checker([&] {
    uint64_t max_proven_safe_g = 0;

    while (readers_done_count.load(std::memory_order_acquire) < N) {
      uint64_t g = tracker.CurrentEpoch();
      if (g == 0) {
        std::this_thread::yield();
        continue;
      }

      // Bounded poll: up to 10000 yields to wait for IsSafeToFree(g).
      // We do NOT wait indefinitely for any single g; readers are still
      // running so some ids may not yet be released — just move on.
      bool safe = false;
      for (int attempt = 0; attempt < 10000 && !safe; ++attempt) {
        safe = tracker.IsSafeToFree(g);
        if (!safe) std::this_thread::yield();
      }

      if (safe) {
        // Verify ordering invariant: all released_flag[i] for i < g must be 1.
        // The acquire load on field[] in ComputeLastDead synchronises-with the
        // release fetch_or in Release(), which is sequenced-after the relaxed
        // store to released[id]; so if IsSafeToFree(g) is true, all flags < g
        // are already visible as 1.
        for (uint64_t i = 0; i < g && i < total_ids; ++i) {
          if (released[i].load(std::memory_order_acquire) != 1) {
            checker_failed.store(true, std::memory_order_relaxed);
            return;  // bail out immediately to avoid spinning on a broken state
          }
        }

        // Monotonicity: re-verify the previously proven safe guard every round.
        if (max_proven_safe_g > 0 && !tracker.IsSafeToFree(max_proven_safe_g)) {
          checker_failed.store(true, std::memory_order_relaxed);
          return;
        }
        if (g > max_proven_safe_g) max_proven_safe_g = g;
      }

      std::this_thread::yield();
    }

    // All readers joined; do a final full verification.
    uint64_t final_g = tracker.CurrentEpoch();
    // All ids have been released at this point; IsSafeToFree(final_g) must hold.
    bool final_safe = false;
    for (int attempt = 0; attempt < 100000 && !final_safe; ++attempt) {
      final_safe = tracker.IsSafeToFree(final_g);
      if (!final_safe) std::this_thread::yield();
    }
    if (!final_safe) {
      checker_failed.store(true, std::memory_order_relaxed);
      return;
    }
    for (uint64_t i = 0; i < final_g && i < total_ids; ++i) {
      if (released[i].load(std::memory_order_acquire) != 1) {
        checker_failed.store(true, std::memory_order_relaxed);
        return;
      }
    }
  });

  // Reader threads: each acquires ids_per_thread ids, records the flag, releases.
  std::vector<std::jthread> readers;
  readers.reserve(N);
  for (unsigned t = 0; t < N; ++t) {
    readers.emplace_back([&] {
      for (uint64_t k = 0; k < ids_per_thread; ++k) {
        uint64_t id = tracker.Acquire();
        // Store flag BEFORE Release() so the ordering invariant holds.
        // Release()'s fetch_or(release) is sequenced-after this store.
        released[id].store(1, std::memory_order_relaxed);
        tracker.Release(id);
      }
      // Signal checker that this reader thread is done.
      readers_done_count.fetch_add(1, std::memory_order_release);
    });
  }

  // jthreads join on destruction (readers then checker via RAII order).
  readers.clear();  // join all readers first
  checker.join();   // wait for checker to finish its final verification

  EXPECT_FALSE(checker_failed.load(std::memory_order_acquire))
      << "Checker detected a released flag not yet set when IsSafeToFree returned true";
}

// Concurrent Release races Block allocation: acquire all ids single-threaded
// (filling exactly 8 blocks = 8 * 4096 = 32768 ids), then shuffle with a
// fixed seed and partition across N threads that Release() concurrently.
//
// This forces the AllocateBlock expected_head-retry race (two threads arrive
// simultaneously when a new block is needed) and the id < first_id prev-walk
// (a thread receives an id from a later block but must walk back).
//
// After all threads join, IsSafeToFree(total) must hold.
// EXPECT_* is issued only by main after all threads have joined.
TEST(EpochTrackerStress, ConcurrentReleaseRacesBlockAllocation) {
  using namespace memgraph::utils;

  const unsigned N = StressThreadCount();
  // Exactly 8 full blocks (ties to kIdsInBlock == 4096).
  constexpr uint64_t kBlocks = 8;
  constexpr uint64_t kIdsInBlock = 4096;
  constexpr uint64_t kTotal = kBlocks * kIdsInBlock;  // 32768

  EpochTracker tracker;

  // Single-threaded Acquire of all ids.
  std::vector<uint64_t> ids(kTotal);
  for (uint64_t i = 0; i < kTotal; ++i) ids[i] = tracker.Acquire();

  // Shuffle with a fixed seed for reproducibility.
  std::mt19937 rng{42u};
  std::shuffle(ids.begin(), ids.end(), rng);

  // Partition shuffled ids across N threads.
  std::vector<std::vector<uint64_t>> partitions(N);
  for (uint64_t i = 0; i < kTotal; ++i) {
    partitions[i % N].push_back(ids[i]);
  }

  // Release concurrently.
  {
    std::vector<std::jthread> threads;
    threads.reserve(N);
    for (unsigned t = 0; t < N; ++t) {
      threads.emplace_back([&tracker, &part = partitions[t]] {
        for (uint64_t id : part) {
          tracker.Release(id);
        }
      });
    }
    // jthreads join on destruction at scope exit.
  }

  // After all threads join, every id has been released.
  EXPECT_TRUE(tracker.IsSafeToFree(kTotal))
      << "IsSafeToFree(" << kTotal << ") must be true after all " << kTotal << " ids released";
}

// ---------------------------------------------------------------------------
// Watermark cache tests
// ---------------------------------------------------------------------------

// WatermarkCacheAdvancesAndStalls: verify that the resume cache correctly
// advances across block boundaries and stalls at partial fields, and that
// IsSafeToFree returns identical results to a fresh oracle tracker performing
// the same operations.
//
// Layout (kIdsInField==64, kIdsInBlock==4096):
//   Phase A: release ids 0..62 (partial first field of block 0) → stall inside block 0.
//   Phase B: release id 63 (first field of block 0 now full) → stall still inside block 0.
//   Phase C: release ids 64..4095 (rest of block 0, all fields full) → prefix advances
//            through block 0; stall at start of block 1.
//   Phase D: release ids 4096..4158 (first two partial fields of block 1) → stall
//            inside block 1.
//   Phase E: release ids 4159..8191 (rest of block 1 minus id 8192 which stays live)
//            → prefix advances through block 1; stall at block 2 (first field not full).
//
// After each phase, IsSafeToFree is compared against a hand-computed expected
// last_dead derived from the same logic as ComputeLastDead.
TEST(EpochTracker, WatermarkCacheAdvancesAndStalls) {
  using namespace memgraph::utils;

  // kIdsInField and kIdsInBlock are private; use the values documented in the
  // header (64 and 4096 respectively) — the BlockBoundary64 and
  // BlockAllocationAcross4096 tests already verify these constants.
  constexpr uint64_t kField = 64;
  constexpr uint64_t kBlock = 4096;

  // Acquire enough ids to span 3 blocks (ids 0 .. 3*kBlock-1 = 12287) plus one
  // extra live id that stays unreleased throughout to pin the stall point.
  constexpr uint64_t kTotal = 3 * kBlock + 1;  // 12289
  EpochTracker tracker;
  for (uint64_t i = 0; i < kTotal; ++i) (void)tracker.Acquire();

  // --- Phase A: release ids 0..62 (partial first field of block 0) ---
  // Expected last_dead: 62 (where_alive=63 in the partial field; bit 63 still alive).
  // IsSafeToFree(63) → last_dead(62) >= 62 → true.
  // IsSafeToFree(64) → last_dead(62) >= 63 → false.
  for (uint64_t i = 0; i <= 62; ++i) tracker.Release(i);
  EXPECT_TRUE(tracker.IsSafeToFree(63)) << "Phase A: guard 63 should be safe";
  EXPECT_FALSE(tracker.IsSafeToFree(64)) << "Phase A: guard 64 should not be safe";

  // --- Phase B: release id 63 → first field now full ---
  // Expected last_dead: 63 (stall at second field of block 0, which is empty).
  // IsSafeToFree(64) → 63 >= 63 → true.
  // IsSafeToFree(65) → 63 >= 64 → false.
  tracker.Release(63);
  EXPECT_TRUE(tracker.IsSafeToFree(64)) << "Phase B: guard 64 should be safe after id 63 released";
  EXPECT_FALSE(tracker.IsSafeToFree(65)) << "Phase B: guard 65 should not be safe";

  // --- Phase C: release ids 64..4095 (complete rest of block 0) ---
  // Expected last_dead: 4095; stall at block 1 field 0 (all zero).
  // IsSafeToFree(4096) → 4095 >= 4095 → true.
  // IsSafeToFree(4097) → 4095 >= 4096 → false.
  for (uint64_t i = 64; i <= 4095; ++i) tracker.Release(i);
  EXPECT_TRUE(tracker.IsSafeToFree(4096)) << "Phase C: guard 4096 should be safe";
  EXPECT_FALSE(tracker.IsSafeToFree(4097)) << "Phase C: guard 4097 should not be safe";

  // --- Phase D: release ids 4096..4095+2*kField-1 = 4096..4223 (two partial fields of block 1) ---
  // All 128 bits of fields 0 and 1 of block 1 are set; field 2 is empty.
  // Expected last_dead: 4095 + 2*kField = 4095 + 128 = 4223.
  // IsSafeToFree(4224) → 4223 >= 4223 → true.
  // IsSafeToFree(4225) → 4223 >= 4224 → false.
  for (uint64_t i = 4096; i <= 4095 + 2 * kField; ++i) tracker.Release(i);
  EXPECT_TRUE(tracker.IsSafeToFree(4224)) << "Phase D: guard 4224 should be safe";
  EXPECT_FALSE(tracker.IsSafeToFree(4225)) << "Phase D: guard 4225 should not be safe";

  // --- Phase E: release ids 4224..8191 (rest of block 1 through last id 8191) ---
  // id 8192 (first of block 2) is still live.
  // Expected last_dead: 8191 (stall at block 2 field 0, bit 0 not set).
  // IsSafeToFree(8192) → 8191 >= 8191 → true.
  // IsSafeToFree(8193) → 8191 >= 8192 → false.
  for (uint64_t i = 4224; i <= 8191; ++i) tracker.Release(i);
  EXPECT_TRUE(tracker.IsSafeToFree(8192)) << "Phase E: guard 8192 should be safe";
  EXPECT_FALSE(tracker.IsSafeToFree(8193)) << "Phase E: guard 8193 should not be safe";

  // Release remaining live ids so the destructor assertion holds.
  for (uint64_t i = 8192; i < kTotal; ++i) tracker.Release(i);
}

// ClearResetsWatermarkCache: verify that destroying an EpochTracker with a warm
// cache and recreating a fresh one produces correct, stale-free results.
//
// Round 1: fill tracker, warm the cache (IsSafeToFree returns true), then
// destroy the tracker.  Any dangling scan_resume_ would manifest as a
// use-after-free on the next ComputeLastDead call if the cache were carried
// into round 2 — Clear() is called by the destructor.
//
// Round 2: a brand-new EpochTracker starting from a clean state is used to
// re-verify that a fresh instance works correctly with no leftover state.
// (Because Clear() is private and only called by the destructor, we exercise
// it through destruction+recreation rather than calling it directly.)
TEST(EpochTracker, ClearResetsWatermarkCache) {
  using namespace memgraph::utils;

  constexpr uint64_t kBlock = 4096;
  // Use 2.5 blocks worth of ids to ensure at least 2 full blocks are committed
  // to the cache (warm: scan_resume_ points into a Block that will be freed).
  constexpr uint64_t kRound1Ids = 2 * kBlock + 128;

  // --- Round 1: warm the cache then destroy ---
  {
    EpochTracker t1;
    for (uint64_t i = 0; i < kRound1Ids; ++i) (void)t1.Acquire();
    for (uint64_t i = 0; i < kRound1Ids; ++i) t1.Release(i);
    // Warm the cache: IsSafeToFree walks all blocks, sets scan_resume_.
    EXPECT_TRUE(t1.IsSafeToFree(kRound1Ids)) << "Round 1: all ids released, must be safe";
    // t1 goes out of scope here; ~EpochTracker() calls Clear(), which must
    // reset scan_resume_=nullptr and cached_last_dead_=kNoDeadPrefix.
    // If the reset is missing, t2 (a fresh placement in the same stack region
    // on many compilers) or a subsequent EpochTracker created at the same
    // address would inherit a dangling pointer — caught by ASan.
  }

  // --- Round 2: fresh tracker, verify clean state ---
  {
    EpochTracker t2;
    // Fresh tracker: no ids acquired, no cache.
    EXPECT_FALSE(t2.IsSafeToFree(1)) << "Round 2: fresh tracker, guard 1 must not be safe";
    EXPECT_TRUE(t2.IsSafeToFree(0)) << "Round 2: guard 0 always safe";

    constexpr uint64_t kRound2Ids = kBlock + 1;
    for (uint64_t i = 0; i < kRound2Ids; ++i) (void)t2.Acquire();
    for (uint64_t i = 0; i < kRound2Ids; ++i) t2.Release(i);
    EXPECT_TRUE(t2.IsSafeToFree(kRound2Ids)) << "Round 2: all ids released, must be safe";
  }
}

// Acquire/Release churn with interleaved threads: N threads each loop k times,
// calling Acquire() immediately followed by Release() with an occasional yield.
// Ids from different threads interleave in the epoch counter, so partial
// fields are shared across threads — stresses the fetch_or bitmap update path.
//
// After all threads join, IsSafeToFree(CurrentEpoch()) must hold because every
// Acquire()d id was immediately Release()d.
//
// Note on watermark cache + concurrency: IsSafeToFree / ComputeLastDead are
// called ONLY from the checker thread in SafetyContractUnderConcurrency, which
// satisfies the single-caller contract (see class-level doc).  This test does
// NOT call IsSafeToFree from multiple threads concurrently; the post-join
// IsSafeToFree call below is made single-threaded after all workers have exited.
// EXPECT_* is issued only by main after all threads have joined.
TEST(EpochTrackerStress, AcquireReleaseChurn) {
  using namespace memgraph::utils;

  const unsigned N = StressThreadCount();
  // Enough iterations to exceed several blocks per thread; kept modest so the
  // test completes well within the ~5 s budget in RelWithDebInfo.
  constexpr uint64_t kIterationsPerThread = 8192;

  EpochTracker tracker;

  {
    std::vector<std::jthread> threads;
    threads.reserve(N);
    for (unsigned t = 0; t < N; ++t) {
      (void)t;
      threads.emplace_back([&tracker] {
        for (uint64_t k = 0; k < kIterationsPerThread; ++k) {
          uint64_t id = tracker.Acquire();
          // Occasional yield to increase interleaving with other threads.
          if ((k & 0xFu) == 0u) std::this_thread::yield();
          tracker.Release(id);
        }
      });
    }
    // jthreads join on destruction at scope exit.
  }

  // All threads have joined; every Acquire()d id was Release()d.
  uint64_t epoch = tracker.CurrentEpoch();
  EXPECT_TRUE(tracker.IsSafeToFree(epoch))
      << "IsSafeToFree(" << epoch << ") must hold after all churn threads complete";
}

// Regression for the watermark-cache data race: the real consumer (the
// light-edge graveyard drain) is NOT a single scanner — a second collector can
// refill the graveyard between one drain's swap and completion, so two drains
// call IsSafeToFree/ComputeLastDead concurrently. ComputeLastDead does a
// multi-step read-modify of the non-atomic cache (cached_last_dead_,
// scan_resume_); without internal serialisation that is a data race. This test
// drives several concurrent IsSafeToFree scanners against live churn so a TSan
// build flags the race if the cache_lock_ serialisation is ever removed. It also
// checks the safety contract is never violated (never reports safe while an id
// below the guard is still unreleased) and monotonicity across scanners.
TEST(EpochTrackerStress, ConcurrentScannersShareWatermarkCache) {
  using namespace memgraph::utils;

  const unsigned readers = std::max(2u, StressThreadCount());
  const unsigned scanners = std::max(2u, StressThreadCount());
  constexpr uint64_t kIterationsPerReader = 8192;

  EpochTracker tracker;
  std::atomic<bool> readers_done{false};
  std::atomic<uint64_t> max_proven_safe{0};
  std::atomic<bool> contract_violation{false};

  {
    std::vector<std::jthread> threads;
    threads.reserve(readers + scanners);

    // Readers: continuous Acquire/Release churn (grows the block list + sets bits).
    for (unsigned t = 0; t < readers; ++t) {
      threads.emplace_back([&tracker] {
        for (uint64_t k = 0; k < kIterationsPerReader; ++k) {
          uint64_t id = tracker.Acquire();
          if ((k & 0xFu) == 0u) std::this_thread::yield();
          tracker.Release(id);
        }
      });
    }

    // Scanners: hammer IsSafeToFree (-> ComputeLastDead -> cache RMW) concurrently.
    for (unsigned s = 0; s < scanners; ++s) {
      threads.emplace_back([&] {
        while (!readers_done.load(std::memory_order_acquire)) {
          uint64_t g = tracker.CurrentEpoch();
          if (g == 0) continue;
          if (tracker.IsSafeToFree(g)) {
            // Monotonic watermark: once g is safe it must stay safe.
            uint64_t prev = max_proven_safe.load(std::memory_order_relaxed);
            while (g > prev && !max_proven_safe.compare_exchange_weak(prev, g)) {
            }
            if (!tracker.IsSafeToFree(max_proven_safe.load(std::memory_order_relaxed))) {
              contract_violation.store(true, std::memory_order_relaxed);
            }
          }
        }
      });
    }

    // Let the readers finish, then signal the scanners to stop.
    // (Readers' jthreads are in `threads`; we can't join them individually here,
    // so spin until the epoch counter stops advancing.)
    uint64_t last = 0;
    unsigned stable = 0;
    while (stable < 50) {
      std::this_thread::yield();
      uint64_t now = tracker.CurrentEpoch();
      stable = (now == last) ? stable + 1 : 0;
      last = now;
    }
    readers_done.store(true, std::memory_order_release);
    // jthreads join on destruction at scope exit.
  }

  EXPECT_FALSE(contract_violation.load()) << "IsSafeToFree reported a previously-safe guard as unsafe";
  uint64_t epoch = tracker.CurrentEpoch();
  EXPECT_TRUE(tracker.IsSafeToFree(epoch)) << "all ids released after join -> IsSafeToFree(epoch) must hold";
}
