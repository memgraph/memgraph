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
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "utils/atomic_memory_block.hpp"
#include "utils/db_aware_allocator.hpp"

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
  };

  // `overlay` must come from this index.
  static std::optional<storage::EdgeRef> BaseOf(const storage::Edge *overlay) noexcept {
    return static_cast<const BaseEdgeOverlay *>(overlay)->base_edge;
  }

  storage::Edge *Find(uint64_t gid) const {
    const Shard &shard = ShardFor(gid);
    std::shared_lock guard{shard.mutex};
    const auto it = shard.map.find(gid);
    return it == shard.map.end() ? nullptr : it->second.get();
  }

  // The base edge `gid`'s overlay was created for, as main holds it. Empty if none or created without one.
  std::optional<storage::EdgeRef> FindBase(uint64_t gid) const {
    const auto *overlay = Find(gid);
    return overlay == nullptr ? std::nullopt : BaseOf(overlay);
  }

  // Returns the overlay for `gid`, creating one if none exists; `base_edge` is recorded only by the creating call. The
  // insert runs OOM-blocked (absl maps are not exception-safe), so a refusal is reported only after the entry is
  // published.
  // @throw utils::OutOfMemoryException
  storage::Edge *GetOrCreate(uint64_t gid, std::optional<storage::EdgeRef> base_edge = std::nullopt) {
    if (auto *found = Find(gid); found != nullptr) return found;
    Shard &shard = ShardFor(gid);
    return utils::AtomicMemoryBlock([&]() -> storage::Edge * {
      auto candidate = memory::MakeDbAwareUnique<BaseEdgeOverlay>(storage::Gid::FromUint(gid), base_edge);
      std::unique_lock guard{shard.mutex};
      return shard.map.try_emplace(gid, std::move(candidate)).first->second.get();
    });
  }

 private:
  static constexpr size_t kShards = 64;

  struct alignas(64) Shard {
    mutable std::shared_mutex mutex;
    absl::flat_hash_map<uint64_t, memory::ArenaAwareUniquePtr<BaseEdgeOverlay>> map;
  };

  Shard &ShardFor(uint64_t gid) noexcept { return shards_[gid % kShards]; }

  const Shard &ShardFor(uint64_t gid) const noexcept { return shards_[gid % kShards]; }

  std::array<Shard, kShards> shards_;
};

}  // namespace memgraph::versioning
