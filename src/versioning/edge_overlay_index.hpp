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
#include <cstdint>
#include <mutex>
#include <optional>
#include <shared_mutex>

#include "absl/container/flat_hash_map.h"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_ref.hpp"
#include "utils/atomic_memory_block.hpp"
#include "utils/db_aware_allocator.hpp"

namespace memgraph::versioning {

// A branch's base-edge overlays: heap Edge objects for base edges the branch changed, keyed by the base edge's gid.
// Delta-less and never recreated, like OverlayIndex's vertex overlays, so an aborted writer leaves nothing to retry and
// racing writers get the same object. Insert-only: pointers stay valid until ~EdgeOverlayIndex.
//
// Overlay delta chains point at these Edges, so the owner must declare this index after the diff engine and the
// teardown arena scope (destroyed after the engine, inside the scope). Base Vertex* held by overlay-based accessors
// stay valid because main's GC horizon and fast discard are clamped to the oldest fork pin.
class EdgeOverlayIndex {
 public:
  // Every Edge* this index hands out points at the base of one of these.
  struct BaseEdgeOverlay : storage::Edge {
    BaseEdgeOverlay(storage::Gid gid, std::optional<storage::EdgeRef> base_edge) noexcept
        : storage::Edge(gid, nullptr),
          base_edge{base_edge.value_or(storage::EdgeRef{static_cast<storage::Edge *>(nullptr)})},
          has_base{base_edge.has_value()} {}

    storage::EdgeRef base_edge;
    bool has_base;
  };

  // FindBase without a lookup: the base edge is fixed at creation, before the overlay is published.
  static std::optional<storage::EdgeRef> BaseOf(const storage::Edge *overlay) noexcept {
    const auto &owned = static_cast<const BaseEdgeOverlay &>(*overlay);
    if (!owned.has_base) return std::nullopt;
    return owned.base_edge;
  }

  storage::Edge *Find(uint64_t gid) const {
    const Shard &shard = ShardFor(gid);
    std::shared_lock guard{shard.mutex};
    const auto it = shard.map.find(gid);
    return it == shard.map.end() ? nullptr : static_cast<storage::Edge *>(it->second.overlay.get());
  }

  // The base edge `gid`'s overlay was created for, as main holds it (a pointer with properties_on_edges, else the gid).
  // Empty for an overlay created without one.
  std::optional<storage::EdgeRef> FindBase(uint64_t gid) const {
    const Shard &shard = ShardFor(gid);
    std::shared_lock guard{shard.mutex};
    const auto it = shard.map.find(gid);
    if (it == shard.map.end()) return std::nullopt;
    return BaseOf(it->second.overlay.get());
  }

  // Returns the overlay for `gid`, creating a delta-less one if none exists; `base_edge` is recorded only by the
  // creating call. The insert runs OOM-blocked (absl maps are not exception-safe), so a refused growth is reported only
  // after the entry is fully published.
  // @throw utils::OutOfMemoryException
  storage::Edge *GetOrCreate(uint64_t gid, std::optional<storage::EdgeRef> base_edge = std::nullopt) {
    if (auto *found = Find(gid); found != nullptr) return found;
    Shard &shard = ShardFor(gid);
    return utils::AtomicMemoryBlock([&] {
      std::unique_lock guard{shard.mutex};
      auto [it, inserted] = shard.map.try_emplace(gid);
      if (inserted) {
        try {
          it->second.overlay = memory::MakeDbAwareUnique<BaseEdgeOverlay>(storage::Gid::FromUint(gid), base_edge);
        } catch (...) {
          shard.map.erase(it);
          throw;
        }
      }
      return static_cast<storage::Edge *>(it->second.overlay.get());
    });
  }

  // Calls `fn(gid, edge)` for every overlay in unspecified order until it returns false. Concurrent creators may or may
  // not be visited. Returns true if every overlay was visited, false if `fn` stopped the walk.
  template <typename Fn>
    requires std::predicate<Fn &, uint64_t, storage::Edge *>
  bool ForEach(Fn &&fn) const {
    for (const Shard &shard : shards_) {
      std::shared_lock guard{shard.mutex};
      for (const auto &[gid, entry] : shard.map) {
        if (!fn(gid, static_cast<storage::Edge *>(entry.overlay.get()))) return false;
      }
    }
    return true;
  }

 private:
  static constexpr size_t kShards = 64;

  struct Entry {
    memory::ArenaAwareUniquePtr<BaseEdgeOverlay> overlay;
  };

  struct alignas(64) Shard {
    mutable std::shared_mutex mutex;
    absl::flat_hash_map<uint64_t, Entry> map;
  };

  Shard &ShardFor(uint64_t gid) noexcept { return shards_[gid % kShards]; }

  const Shard &ShardFor(uint64_t gid) const noexcept { return shards_[gid % kShards]; }

  std::array<Shard, kShards> shards_;
};

}  // namespace memgraph::versioning
