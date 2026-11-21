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

#include <array>
#include <concepts>
#include <cstddef>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <utility>

#include <absl/container/inlined_vector.h>
#include <boost/unordered/unordered_flat_map.hpp>

#include "storage/v2/edge_direction.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/db_aware_allocator.hpp"
#include "utils/small_vector.hpp"
#include "versioning/overlay_state.hpp"

namespace memgraph::versioning {

// The base edges that may be deleted on this branch at one base vertex (pointers to EdgeOverlayIndex::BaseEdgeOverlay
// bases). Insert-only and non-MVCC: a reader checks each candidate's delete mark. Guarded by the owning shard's mutex.
struct BaseEdgeDeletes {
  using EdgeCandidates = utils::small_vector<storage::Edge *, memory::DbAwareAllocator<storage::Edge *>>;
  EdgeCandidates out;
  EdgeCandidates in;
};

static_assert(sizeof(BaseEdgeDeletes) == 32, "the 32 B jemalloc class");

// A copy of one direction's candidates, taken under the shard's shared lock.
using CandidateCopy = absl::InlinedVector<storage::Edge *, 16>;

// A branch's base-vertex state, keyed by base gid, in two maps under one shard lock: delta-less heap Vertex overlays
// (outside the diff engine's vertices_) and BaseEdgeDeletes entries. An entry in either means main's vertex has its
// branched bit set (callers set it first). Entries are never removed while the branch lives, and a BaseEdgeDeletes
// entry never moves between owners.
// L-e: a side a deleter did not request is the own side of the vertex that DETACH deletes in the same call; that
// vertex's delete mark covers it, or the fallback lists it when the mark does not land.
// F: a deletes entry for g exists => MayHaveEdgeChange(g) (the delete records before it appends), so readers may skip
// the deletes probe on a clear bit.
// Owns the overlays, so the owner must destroy the diff engine first (its deltas
// point at them).
class OverlayIndex {
 public:
  // Every Vertex* this index hands out points at the base of one of these.
  struct BaseVertexOverlay : storage::Vertex {
    BaseVertexOverlay(storage::Gid gid, storage::Vertex *base_vertex) noexcept
        : storage::Vertex(gid, nullptr), base_vertex(base_vertex) {}

    storage::Vertex *base_vertex;
  };

  static_assert(sizeof(BaseVertexOverlay) == 88, "the 96 B jemalloc class");

  struct Found {
    storage::Vertex *overlay{nullptr};
    storage::Vertex *base_vertex{nullptr};
  };

  // `overlay` / `base_vertex` as in Found; `candidates` is one direction's deleted-edge candidates, empty if the vertex
  // has no BaseEdgeDeletes entry or they were not asked for.
  struct Expansion {
    storage::Vertex *overlay{nullptr};
    storage::Vertex *base_vertex{nullptr};
    CandidateCopy candidates;
  };

  storage::Vertex *FindOverlay(storage::Gid gid) const noexcept { return Find(gid).overlay; }

  storage::Vertex *FindBase(storage::Gid gid) const noexcept { return Find(gid).base_vertex; }

  Found Find(storage::Gid gid) const noexcept {
    const Shard &shard = ShardFor(gid);
    std::shared_lock guard{shard.mutex};
    const auto it = shard.map.find(gid);
    if (it == shard.map.end()) return {};
    return {it->second.get(), it->second->base_vertex};
  }

  // Find plus, if `with_deletes`, a copy of `direction`'s candidates, under one shared lock acquisition. The caller
  // checks each candidate's delete mark afterwards, with no index lock held.
  // @throw std::bad_alloc past 16 candidates
  Expansion FindForExpansion(storage::Gid gid, storage::EdgeDirection direction, bool with_deletes) const {
    const Shard &shard = ShardFor(gid);
    std::shared_lock guard{shard.mutex};
    Expansion found;
    if (const auto it = shard.map.find(gid); it != shard.map.end()) {
      found.overlay = it->second.get();
      found.base_vertex = it->second->base_vertex;
    }
    if (with_deletes) {
      if (const auto it = shard.deletes.find(gid); it != shard.deletes.end()) {
        const auto &list = direction == storage::EdgeDirection::OUT ? it->second->out : it->second->in;
        found.candidates.assign(list.begin(), list.end());
      }
    }
    return found;
  }

  // Appends `edge_overlay` to `direction`'s candidates of `gid`, creating the entry if absent. The caller has set the
  // base vertex's branched bit and the edge change filter bit first. A throw may leave an entry with the list
  // unchanged.
  // @throw utils::OutOfMemoryException, std::bad_alloc
  void AppendDeleteCandidate(storage::Gid gid, storage::EdgeDirection direction, storage::Edge *edge_overlay) {
    Shard &shard = ShardFor(gid);
    std::unique_lock guard{shard.mutex};
    auto it = shard.deletes.find(gid);
    if (it == shard.deletes.end()) {
      it = shard.deletes.try_emplace(gid, memory::MakeDbAwareUnique<BaseEdgeDeletes>()).first;
    }
    (direction == storage::EdgeDirection::OUT ? it->second->out : it->second->in).push_back(edge_overlay);
  }

  // An overlay or a BaseEdgeDeletes entry exists for `gid`.
  bool HasEntry(storage::Gid gid) const noexcept {
    const Shard &shard = ShardFor(gid);
    std::shared_lock guard{shard.mutex};
    return shard.map.contains(gid) || shard.deletes.contains(gid);
  }

  // Returns the overlay for `gid`, creating one if none exists. The creating call records `base_vertex` and gives the
  // new overlay `base_labels` (the base vertex's labels at the fork point, which the caller reads: `base_vertex` is
  // main's live vertex) before it is published, so no reader sees an overlay without them; later calls ignore both. The
  // create runs under the shard lock, so only the winner copies labels. boost's strong guarantee means a throw
  // publishes nothing.
  // @throw utils::OutOfMemoryException, std::bad_alloc
  storage::Vertex *GetOrCreateOverlay(storage::Gid gid, storage::Vertex *base_vertex,
                                      std::span<const storage::LabelId> base_labels) {
    if (auto *found = FindOverlay(gid); found != nullptr) return found;
    Shard &shard = ShardFor(gid);
    std::unique_lock guard{shard.mutex};
    if (const auto it = shard.map.find(gid); it != shard.map.end()) return it->second.get();
    auto overlay = memory::MakeDbAwareUnique<BaseVertexOverlay>(gid, base_vertex);
    overlay::CopyBaseLabels(overlay.get(), base_labels);
    return shard.map.try_emplace(gid, std::move(overlay)).first->second.get();
  }

  // Calls `fn(overlay)` for every overlay in unspecified order until it returns false. Concurrent creators may or may
  // not be visited. Returns true if every overlay was visited.
  template <std::predicate<storage::Vertex *> Fn>
  bool ForEachOverlay(Fn &&fn) const {
    for (const Shard &shard : shards_) {
      std::shared_lock guard{shard.mutex};
      for (const auto &[gid, overlay] : shard.map) {
        if (!fn(static_cast<storage::Vertex *>(overlay.get()))) return false;
      }
    }
    return true;
  }

 private:
  static constexpr size_t kShards = 64;

  struct alignas(64) Shard {
    mutable std::shared_mutex mutex;  // guards both maps and every BaseEdgeDeletes they own
    boost::unordered_flat_map<storage::Gid, memory::ArenaAwareUniquePtr<BaseVertexOverlay>, std::hash<storage::Gid>>
        map;
    boost::unordered_flat_map<storage::Gid, memory::ArenaAwareUniquePtr<BaseEdgeDeletes>, std::hash<storage::Gid>>
        deletes;
  };

  Shard &ShardFor(storage::Gid gid) noexcept { return shards_[gid.AsUint() % kShards]; }

  const Shard &ShardFor(storage::Gid gid) const noexcept { return shards_[gid.AsUint() % kShards]; }

  std::array<Shard, kShards> shards_;
};

}  // namespace memgraph::versioning
