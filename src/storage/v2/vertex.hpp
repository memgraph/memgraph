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

#include <tuple>

#include "storage/v2/delta.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "utils/db_aware_allocator.hpp"
#include "utils/pointer_pack.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/small_vector.hpp"

namespace memgraph::storage {

struct Vertex;

using EdgeTriple = std::tuple<EdgeTypeId, Vertex *, EdgeRef>;
using Edges = utils::small_vector<EdgeTriple, memory::DbAwareAllocator<EdgeTriple>>;

struct Vertex {
  Vertex(Gid gid, Delta *delta) : gid(gid), delta_(delta) {
    MG_ASSERT(delta == nullptr || delta->action == Delta::Action::DELETE_OBJECT ||
                  delta->action == Delta::Action::DELETE_DESERIALIZED_OBJECT,
              "Vertex must be created with an initial DELETE_OBJECT delta!");
  }

  const Gid gid;

  utils::small_vector<LabelId, memory::DbAwareAllocator<LabelId>> labels;

  Edges in_edges;
  Edges out_edges;

  PropertyStore properties;
  mutable utils::RWSpinLock lock;

  Delta *delta() const { return delta_.GetPtr(); }

  void SetDelta(Delta *d) { delta_.SetPtr(d); }

  bool deleted() const { return delta_.Get<kDeletedBit>() != 0; }

  void SetDeleted(bool b) { delta_.Set<kDeletedBit>(b ? 1 : 0); }

  bool has_uncommitted_non_sequential_deltas() const { return delta_.Get<kNonSeqDeltasBit>() != 0; }

  void set_has_uncommitted_non_sequential_deltas(bool b) { delta_.Set<kNonSeqDeltasBit>(b ? 1 : 0); }

  // Graph Versioning v1 -- phase 1 (write-side only) of a per-object "touched by any branch" hint.
  // MONOTONIC / NEVER-CLEAR: once set, stays set for the lifetime of the object -- there is no
  // corresponding ClearBranched, and none is planned (an over-set bit only ever costs a missed
  // fast-path in phase 2's reader, never a correctness bug; clearing it early would be one).
  //
  // INVARIANT (maintained entirely by callers, not by this class): `branched() == false` implies
  // this vertex's labels, properties, AND full incident-edge set are IDENTICAL to every checked-out
  // branch's fork-state view of it -- because every branch mutation that could disagree (a COW, or
  // a tombstone) marks this bit on MAIN's own live Vertex object before/while it happens (see
  // versioning::BranchContext::MarkMainObjectBranched, branch_engine.cpp/.hpp). Main's own code
  // never reads this bit -- it exists purely as a branch-side hint (R35: main is never aware of
  // branches).
  //
  // CONCURRENCY: `SetBranched` is an ordinary, non-atomic tag-bit write -- like `SetDeleted`/
  // `set_has_uncommitted_non_sequential_deltas` above, the CALLER MUST HOLD THIS VERTEX'S OWN
  // `lock` (see e.g. `std::unique_lock{vertex_->lock}` in vertex_accessor.cpp) before calling it;
  // it is safe against other lock-holding writers of `delta_` (SetDelta/SetDeleted/
  // set_has_uncommitted_non_sequential_deltas) purely because they're all serialized by that same
  // lock. `branched()`, in contrast, is intended to be read WITHOUT that lock (a lock-free hot-path
  // read, landing in a later phase) -- it reads the packed word via `PointerPack::GetRelaxed`
  // (a relaxed `std::atomic_ref` load) rather than a plain read, so that lock-free read never
  // races (in the data-race/UB sense) with a concurrent, lock-held `SetBranched` on another thread.
  bool branched() const { return delta_.GetRelaxed<kBranchedBit>(); }

  // Caller MUST hold `lock` (see `branched()`'s own doc-comment above for the full concurrency
  // contract).
  void SetBranched(bool b) { delta_.Set<kBranchedBit>(b ? 1 : 0); }

 private:
  static constexpr int kDeletedBit = 0;
  static constexpr int kNonSeqDeltasBit = 1;
  static constexpr int kBranchedBit = 2;

  // 3 tag bits now packed into the pointer's low bits -- still legal: PointerPack<T, N> requires
  // alignof(T) >= (1 << N), and delta.hpp already static_asserts alignof(Delta) >= 8 (>= 1 << 3),
  // independently of this change. `sizeof(Vertex)` is unaffected -- PointerPack's own storage is a
  // single `uintptr_t` regardless of how many of its bits are in use (see the static_assert below).
  utils::PointerPack<Delta, 3> delta_;
};

static constexpr std::size_t kEdgeTypeIdPos = 0U;
static constexpr std::size_t kVertexPos = 1U;
static constexpr std::size_t kEdgeRefPos = 2U;

static_assert(alignof(Vertex) >= 8, "The Vertex should be aligned to at least 8!");
static_assert(sizeof(Vertex) == 80, "If this changes documentation needs changing");

inline bool operator==(const Vertex &first, const Vertex &second) { return first.gid == second.gid; }

inline bool operator<(const Vertex &first, const Vertex &second) { return first.gid < second.gid; }

inline bool operator==(const Vertex &first, const Gid &second) { return first.gid == second; }

inline bool operator<(const Vertex &first, const Gid &second) { return first.gid < second; }

}  // namespace memgraph::storage
