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
#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <utility>

#include <boost/unordered/unordered_flat_map.hpp>

#include "storage/v2/edge.hpp"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/atomic_memory_block.hpp"
#include "utils/db_aware_allocator.hpp"
#include "versioning/overlay_index.hpp"

namespace memgraph::versioning {

enum ListSide : uint8_t { kNoSide = 0, kFromSide = 1, kToSide = 2, kBothSides = 3 };

// A branch's base-edge overlays: delta-less heap Edges keyed by base edge gid. Insert-only; owns the overlays, so
// declare it after the diff engine and the teardown arena scope (overlay delta chains point at these Edges).
class EdgeOverlayIndex {
 public:
  // Every Edge* this index hands out points at the base of one of these.
  struct BaseEdgeOverlay : storage::Edge {
    BaseEdgeOverlay(storage::Gid gid, storage::EdgeRef base_edge) noexcept
        : storage::Edge(gid, nullptr), base_edge(base_edge) {}

    storage::EdgeRef base_edge;  // as main holds it: a pointer, or a bare gid when main has no edge properties

    // Bit s set: this edge is in side s's endpoint list. Set after the append, never cleared (see ListBaseEdgeDelete).
    std::atomic<uint8_t> listed_sides{kNoSide};
  };

  static_assert(sizeof(BaseEdgeOverlay) == 48, "the 48 B jemalloc class");

  // `overlay` must come from this index.
  static storage::EdgeRef BaseOf(const storage::Edge *overlay) noexcept {
    return static_cast<const BaseEdgeOverlay *>(overlay)->base_edge;
  }

  storage::Edge *Find(storage::Gid gid) const {
    const Shard &shard = ShardFor(gid);
    std::shared_lock guard{shard.mutex};
    const auto it = shard.map.find(gid);
    return it == shard.map.end() ? nullptr : it->second.get();
  }

  // The base edge `gid`'s overlay was created for, as main holds it. Empty if there is no overlay.
  std::optional<storage::EdgeRef> FindBase(storage::Gid gid) const {
    const auto *overlay = Find(gid);
    if (overlay == nullptr) return std::nullopt;
    return BaseOf(overlay);
  }

  // Returns the overlay for `gid`, creating one if none exists; `base_edge` is recorded only by the creating call.
  // boost's strong guarantee means a throw publishes nothing.
  // @throw utils::OutOfMemoryException, std::bad_alloc
  storage::Edge *GetOrCreate(storage::Gid gid, storage::EdgeRef base_edge) {
    if (auto *found = Find(gid); found != nullptr) return found;
    Shard &shard = ShardFor(gid);
    auto candidate = memory::MakeDbAwareUnique<BaseEdgeOverlay>(gid, base_edge);
    std::unique_lock guard{shard.mutex};
    return shard.map.try_emplace(gid, std::move(candidate)).first->second.get();
  }

 private:
  static constexpr size_t kShards = 64;

  struct alignas(64) Shard {
    mutable std::shared_mutex mutex;
    boost::unordered_flat_map<storage::Gid, memory::ArenaAwareUniquePtr<BaseEdgeOverlay>, std::hash<storage::Gid>> map;
  };

  Shard &ShardFor(storage::Gid gid) noexcept { return shards_[gid.AsUint() % kShards]; }

  const Shard &ShardFor(storage::Gid gid) const noexcept { return shards_[gid.AsUint() % kShards]; }

  std::array<Shard, kShards> shards_;
};

// Records `edge_overlay` as a possibly-deleted base edge on the requested endpoint sides, before the caller marks it
// deleted, so a visible delete mark is always listed on every side that was asked for. The caller has set the branched
// bit of each requested endpoint. A self-loop appends once to each list. Never nests with the edge overlay's lock;
// the two appends take their shard locks in sequence. Concurrent first callers may both append (a duplicate is
// harmless, a missing entry is not). OOM-blocked: a refusal is reported only after the appends and bits are published.
// `edge_overlay` MUST be an EdgeOverlayIndex::BaseEdgeOverlay.
// @throw utils::OutOfMemoryException
inline void ListBaseEdgeDelete(OverlayIndex &index, storage::Gid from, storage::Gid to, storage::Edge *edge_overlay,
                               ListSide sides = kBothSides) {
  auto *edge = static_cast<EdgeOverlayIndex::BaseEdgeOverlay *>(edge_overlay);
  const auto want = static_cast<uint8_t>(sides & ~edge->listed_sides.load(std::memory_order_acquire));
  if (want == 0) return;
  utils::AtomicMemoryBlock([&] {
    if (want & kFromSide) index.AppendDeleteCandidate(from, storage::EdgeDirection::OUT, edge_overlay);
    if (want & kToSide) index.AppendDeleteCandidate(to, storage::EdgeDirection::IN, edge_overlay);
    edge->listed_sides.fetch_or(want, std::memory_order_release);
  });
}

}  // namespace memgraph::versioning
