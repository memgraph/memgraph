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
#include <shared_mutex>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "storage/v2/id_types.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/atomic_memory_block.hpp"
#include "utils/db_aware_allocator.hpp"

namespace memgraph::versioning {

// A branch's base-vertex overlays: delta-less heap Vertex objects (outside the diff engine's vertices_), keyed by base
// gid. Insert-only; owns the overlays, so the owner must destroy the diff engine first (its deltas point at them).
class OverlayIndex {
 public:
  // Every Vertex* this index hands out points at the base of one of these.
  struct BaseVertexOverlay : storage::Vertex {
    BaseVertexOverlay(storage::Gid gid, storage::Vertex *base_vertex) noexcept
        : storage::Vertex(gid, nullptr), base_vertex(base_vertex) {}

    storage::Vertex *base_vertex;
  };

  struct Found {
    storage::Vertex *overlay{nullptr};
    storage::Vertex *base_vertex{nullptr};
  };

  storage::Vertex *FindOverlay(uint64_t gid) const noexcept { return Find(gid).overlay; }

  storage::Vertex *FindBase(uint64_t gid) const noexcept { return Find(gid).base_vertex; }

  Found Find(uint64_t gid) const noexcept {
    const Shard &shard = ShardFor(gid);
    std::shared_lock guard{shard.mutex};
    const auto it = shard.map.find(gid);
    if (it == shard.map.end()) return {};
    return {it->second.get(), it->second->base_vertex};
  }

  // Returns the overlay for `gid`, creating one if none exists; `base_vertex` is recorded only by the creating call.
  // The insert runs OOM-blocked (absl maps are not exception-safe), so a refusal is reported only after the entry is
  // published.
  // @throw utils::OutOfMemoryException, std::bad_alloc
  storage::Vertex *GetOrCreateOverlay(uint64_t gid, storage::Vertex *base_vertex) {
    if (auto *found = FindOverlay(gid); found != nullptr) return found;
    Shard &shard = ShardFor(gid);
    return utils::AtomicMemoryBlock([&]() -> storage::Vertex * {
      auto candidate = memory::MakeDbAwareUnique<BaseVertexOverlay>(storage::Gid::FromUint(gid), base_vertex);
      std::unique_lock guard{shard.mutex};
      return shard.map.try_emplace(gid, std::move(candidate)).first->second.get();
    });
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
    mutable std::shared_mutex mutex;
    absl::flat_hash_map<uint64_t, memory::ArenaAwareUniquePtr<BaseVertexOverlay>> map;
  };

  Shard &ShardFor(uint64_t gid) noexcept { return shards_[gid % kShards]; }

  const Shard &ShardFor(uint64_t gid) const noexcept { return shards_[gid % kShards]; }

  std::array<Shard, kShards> shards_;
};

}  // namespace memgraph::versioning
