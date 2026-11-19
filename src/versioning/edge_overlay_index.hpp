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
#include <functional>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <utility>
#include <vector>

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

// A branch's base-edge overlays: delta-less heap Edges keyed by base edge gid. Insert-only; owns the overlays, so
// declare it after the diff engine and the teardown arena scope (overlay delta chains point at these Edges).
class EdgeOverlayIndex {
 public:
  // Every Edge* this index hands out points at the base of one of these.
  struct BaseEdgeOverlay : storage::Edge {
    BaseEdgeOverlay(storage::Gid gid, std::optional<storage::EdgeRef> base_edge) noexcept
        : storage::Edge(gid, nullptr), base_edge(base_edge) {}

    std::optional<storage::EdgeRef> base_edge;

    // Whether this overlay has been appended to its endpoints' candidate lists (see ListBaseEdgeDelete).
    std::atomic<bool> listed{false};
  };

  // `overlay` must come from this index.
  static std::optional<storage::EdgeRef> BaseOf(const storage::Edge *overlay) noexcept {
    return static_cast<const BaseEdgeOverlay *>(overlay)->base_edge;
  }

  storage::Edge *Find(storage::Gid gid) const {
    const Shard &shard = ShardFor(gid);
    std::shared_lock guard{shard.mutex};
    const auto it = shard.map.find(gid);
    return it == shard.map.end() ? nullptr : it->second.get();
  }

  // The base edge `gid`'s overlay was created for, as main holds it. Empty if none or created without one.
  std::optional<storage::EdgeRef> FindBase(storage::Gid gid) const {
    const auto *overlay = Find(gid);
    return overlay == nullptr ? std::nullopt : BaseOf(overlay);
  }

  // Returns the overlay for `gid`, creating one if none exists; `base_edge` is recorded only by the creating call.
  // boost's strong guarantee means a throw publishes nothing.
  // @throw utils::OutOfMemoryException
  storage::Edge *GetOrCreate(storage::Gid gid, std::optional<storage::EdgeRef> base_edge = std::nullopt) {
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

// Records `edge_overlay` as a possibly-deleted base edge of both endpoints, before the caller marks it deleted, so a
// visible delete mark is always listed. A self-loop gets one entry in each list. Never nests with the edge overlay's
// lock. Both vertices MUST be OverlayIndex::BaseVertexOverlays and `edge_overlay` an EdgeOverlayIndex::BaseEdgeOverlay.
// Concurrent first callers may both append (a duplicate is harmless, a missing entry is not). OOM-blocked: a refusal is
// reported only after both entries are published.
// @throw utils::OutOfMemoryException
inline void ListBaseEdgeDelete(storage::Vertex *from_overlay, storage::Vertex *to_overlay,
                               storage::Edge *edge_overlay) {
  auto *edge = static_cast<EdgeOverlayIndex::BaseEdgeOverlay *>(edge_overlay);
  if (edge->listed.load(std::memory_order_acquire)) return;
  auto *from = static_cast<OverlayIndex::BaseVertexOverlay *>(from_overlay);
  auto *to = static_cast<OverlayIndex::BaseVertexOverlay *>(to_overlay);
  utils::AtomicMemoryBlock([&] {
    {
      std::unique_lock guard{from->lock};
      from->deleted_out.push_back(edge_overlay);
    }
    {
      std::unique_lock guard{to->lock};
      to->deleted_in.push_back(edge_overlay);
    }
    edge->listed.store(true, std::memory_order_release);
  });
}

// A copy of `overlay`'s candidates in `direction`, taken under the overlay's shared lock; the caller checks each edge
// overlay's deleted state after the lock is released (never nested). `overlay` MUST be a
// OverlayIndex::BaseVertexOverlay.
inline std::vector<storage::Edge *> BaseEdgeDeleteCandidates(storage::Vertex *overlay,
                                                             storage::EdgeDirection direction) {
  auto *vertex_overlay = static_cast<OverlayIndex::BaseVertexOverlay *>(overlay);
  std::shared_lock guard{vertex_overlay->lock};
  const auto &list =
      direction == storage::EdgeDirection::OUT ? vertex_overlay->deleted_out : vertex_overlay->deleted_in;
  if (list.empty()) return {};
  std::vector<storage::Edge *> out;
  out.reserve(list.size());
  for (auto *edge : list) out.push_back(edge);
  return out;
}

}  // namespace memgraph::versioning
