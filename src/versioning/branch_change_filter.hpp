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

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "storage/v2/id_types.hpp"
#include "versioning/branch_change_kind.hpp"

namespace memgraph::versioning {

// Graph Versioning v1 -- branch-side change filters (design: perf-box/docs/
// 2026-07-16--branch-side-change-filter-design.md).
//
// PROBLEM. On a checked-out branch, reading a field of a MAIN-resident vertex whose global
// `branched()` bit is set costs a diff-engine skiplist `FindVertex` (BranchContext::ResolveVertex)
// -- the dominant worst-churn branch-read hotspot. That bit is (i) GLOBAL (set by ANY branch, never
// cleared -- a vertex changed by branch X forces a resolve on branch Y that never touched it) and
// (ii) COARSE (it says *something* changed, not *what* -- reading `:User` / `n.id` on a vertex whose
// only change was `SET n.age` still resolves, only to re-fetch fork-identical fields).
//
// FIX. A per-branch, monotonic, gid-keyed approximate-membership filter, one per change KIND, that
// answers "did *this* branch change *this kind* of thing on gid G?" BEFORE paying the resolve. A
// CLEAR answer => definitely-unchanged => read the fork copy the branch already holds (impl_), no
// lookup. A SET answer => maybe-changed => resolve as today (a false positive only wastes a resolve,
// never a stale read).
//
// CORRECTNESS rests on the no-false-negatives property of a monotonic filter: a clear bit means the
// key was *never inserted*. So the single load-bearing invariant is INV-1 (completeness): every
// branch mutation of gid inserts into the matching kind filter, with zero misses, on BOTH the
// live-write path and the change-log replay path. See the design doc for the enumerated
// population sites and the replay re-walk.

// A monotonic (insert-only, never-clear), thread-safe, approximate-membership filter keyed by gid.
//
// Register-blocked bloom, k=2: gids are dense, unique, high-entropy integers, so a single
// multiply-shift Fibonacci mix distributes them well with no hash cascade. One gid maps to exactly
// ONE 64-bit word (one atomic op, within a single cache line) in which two bits are set/tested --
// "one memory access, a few ALU ops" (design section 8).
//
// THREAD SAFETY (design section 7): bits are only ever set, never cleared. Insert is a lock-free
// atomic release fetch-or; MayContain is an acquire atomic load. No lock, no torn read: `fetch_or`
// is an indivisible RMW (no lost update even under a concurrent same-word insert, independent of any
// higher-level exclusivity), and a reader either sees a bit or does not -- either answer is safe (a
// set bit => resolve; a not-yet-visible set bit only risks a missed resolve, i.e. a stale read, so
// visibility DOES matter and is guaranteed as follows).
//
// A branch's own write (VertexAccessor::CowIfNeeded -> Insert, during a mutating query) must be
// visible to that branch's later read (MayContain, during a subsequent query). Two invariants make
// this hold WITHOUT relying on literal same-thread program order (Bolt sessions run on a shared
// asio io_context pool -- consecutive queries of one session may execute on DIFFERENT OS threads,
// serialised by the session's asio strand, not by running on one thread):
//   (1) SINGLE-WRITER-PER-BRANCH: VersionStore::TryAcquireCheckout (version_store.cpp) admits at most
//       one live BranchContext per branch, so branch writes are never concurrent with each other.
//   (2) RELEASE/ACQUIRE: Insert publishes with memory_order_release and MayContain consumes with
//       memory_order_acquire -- so once a later read observes a set bit, it also observes every write
//       the setting query made, establishing happens-before across the strand's cross-thread handoff
//       (belt-and-suspenders atop asio's own strand synchronisation, which is not documented as a
//       memory contract). The genuine cross-thread path (enterprise PARALLEL EXECUTION) cannot run a
//       write concurrently with a read fan-out anyway -- the planner refuses to parallelise a
//       non-read-only sub-plan (plan/rewrite/parallel_rewrite.hpp).
class MonotonicGidFilter {
 public:
  // `num_words` MUST be a power of two (enforced by the sole caller, BranchChangeFilters, via a
  // static_assert on its size constant). Value-initialises every word to 0 (make_unique for arrays
  // value-initialises, and a value-initialised std::atomic<uint64_t> holds 0).
  explicit MonotonicGidFilter(size_t num_words)
      : index_mask_(num_words - 1), words_(std::make_unique<std::atomic<uint64_t>[]>(num_words)) {}

  void Insert(storage::Gid gid) noexcept {
    auto const [idx, bits] = Locate(gid);
    words_[idx].fetch_or(bits, std::memory_order_release);
  }

  [[nodiscard]] bool MayContain(storage::Gid gid) const noexcept {
    auto const [idx, bits] = Locate(gid);
    return (words_[idx].load(std::memory_order_acquire) & bits) == bits;
  }

 private:
  struct Slot {
    size_t idx;
    uint64_t bits;
  };

  [[nodiscard]] Slot Locate(storage::Gid gid) const noexcept {
    // Fibonacci hashing: multiply by 2^64/phi (odd => bijective mod 2^64) avalanches the gid's
    // entropy toward the high bits. Word index from the high bits; the two in-word bit positions
    // from two disjoint low 6-bit fields -- all three drawn from non-overlapping regions of `h`.
    uint64_t const h = gid.AsUint() * 0x9E3779B97F4A7C15ULL;
    size_t const idx = (h >> 40U) & index_mask_;
    uint64_t const bit_a = 1ULL << (h & 63U);
    uint64_t const bit_b = 1ULL << ((h >> 6U) & 63U);
    return {idx, bit_a | bit_b};
  }

  size_t index_mask_;  // num_words - 1; num_words is a power of two, so this masks the word index.
  std::unique_ptr<std::atomic<uint64_t>[]> words_;
};

// The three per-kind filters a branch maintains, plus the record/query surface BranchContext
// forwards to. One instance lives on each BranchContext (branch_engine.hpp), rebuilt fresh on every
// checkout and re-seeded from the branch's change-log (BuildFromFork's replay re-walk).
//
// NO `any_change` CATCH-ALL (deviation from design section 4, deliberate): the design carried a 4th
// `any_change` filter set on every mutation, as a backstop. It is omitted here because it is
// runtime-dead for the current read set: monotonicity gives `any_kind_change ⇒ any_change`, so
// `!any_change ⇒ !any_kind_change` -- a read gated on its own kind filter already gets the
// cross-branch fast-exit (a gid this branch never touched has every kind filter clear), and a read
// of an instrumented kind gains nothing from also consulting `any_change` (a missed kind-filter
// insert is a stale read whether or not `any_change` was set, since the read trusts the kind filter).
// MAINTENANCE INVARIANT this places on future work: every value/adjacency read on a branch MUST gate
// on a dedicated kind filter that ALL of that kind's mutation sites populate (INV-1). A new read kind
// with no dedicated filter has nothing safe to gate on -- add its filter + instrument its mutations;
// do NOT reintroduce a read that trusts a coarse bit without the matching kind coverage.
//
// Sized by kFilterWords below -- FIXED per branch (a monotonic bloom cannot cheaply resize). The
// design assumes "small branches"; a branch that changes far more gids than the filter is tuned for
// saturates gracefully (rising false-positive rate => more resolves => degrades toward today's
// all-resolve behaviour, never toward incorrectness -- design section 11, risk 5).
class BranchChangeFilters {
 public:
  // 4096 words * 8 bytes = 32 KiB per filter, 96 KiB per branch. Holds ~8k changed gids at ~1%
  // false-positive; ~20k at ~10% (still correct, just more resolves). Power of two (Locate's mask).
  static constexpr size_t kFilterWords = 4096;
  static_assert((kFilterWords & (kFilterWords - 1)) == 0, "kFilterWords must be a power of two");

  BranchChangeFilters() : property_(kFilterWords), label_(kFilterWords), edge_(kFilterWords) {}

  BranchChangeFilters(const BranchChangeFilters &) = delete;
  BranchChangeFilters &operator=(const BranchChangeFilters &) = delete;
  BranchChangeFilters(BranchChangeFilters &&) = delete;
  BranchChangeFilters &operator=(BranchChangeFilters &&) = delete;
  ~BranchChangeFilters() = default;

  void RecordVertexChange(storage::Gid gid, BranchChangeKind kind) noexcept {
    (kind == BranchChangeKind::kLabel ? label_ : property_).Insert(gid);
  }

  // Records that `endpoint_gid`'s adjacency (edge add/remove/property, or delete-cascade) may have
  // changed on this branch. Called for BOTH endpoints of every changed edge -- a traversal reads a
  // vertex's incident edges keyed by that vertex's gid, so both sides must be flagged so an
  // expansion from EITHER endpoint sees the change (design section 6).
  void RecordEdgeChange(storage::Gid endpoint_gid) noexcept { edge_.Insert(endpoint_gid); }

  [[nodiscard]] bool MayHaveLabelChange(storage::Gid gid) const noexcept { return label_.MayContain(gid); }

  [[nodiscard]] bool MayHavePropertyChange(storage::Gid gid) const noexcept { return property_.MayContain(gid); }

  [[nodiscard]] bool MayHaveEdgeChange(storage::Gid gid) const noexcept { return edge_.MayContain(gid); }

 private:
  MonotonicGidFilter property_;
  MonotonicGidFilter label_;
  MonotonicGidFilter edge_;
};

}  // namespace memgraph::versioning
