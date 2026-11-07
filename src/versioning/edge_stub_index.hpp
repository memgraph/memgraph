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

#include "absl/container/flat_hash_map.h"
#include "storage/v2/edge.hpp"
#include "utils/atomic_memory_block.hpp"
#include "utils/db_aware_allocator.hpp"

namespace memgraph::versioning {

// A branch's fork-edge stubs: heap Edge objects for fork edges the branch changed, keyed by the fork edge's gid and
// found in O(1). Owned here, not by the diff engine's edges_.
//
// Delta-less and never recreated, like StubIndex's vertex stubs. An aborted writer therefore leaves nothing behind to
// retry against, and two writers racing to materialize the same edge get the same object. Stock edge creation would
// have to be retried after an abort and cannot be raced on one gid.
//
// Insert-only: pointers stay valid until ~EdgeStubIndex. The stubs' delta chains point at the Edge objects, so the
// owner must declare this index after the diff engine and the teardown arena scope: members are destroyed in reverse
// order, so it is then destroyed after the engine and inside the scope. The fork Vertex* in a stub's from/to stays
// valid because main's GC horizon and fast discard are clamped to the oldest fork pin.
class EdgeStubIndex {
 public:
  storage::Edge *Find(uint64_t gid) const {
    const Shard &shard = ShardFor(gid);
    std::shared_lock guard{shard.mutex};
    const auto it = shard.map.find(gid);
    return it == shard.map.end() ? nullptr : it->second.get();
  }

  // Returns the stub for `gid`, creating a delta-less one if none exists. Concurrent callers for one gid get the same
  // object. The insert runs OOM-blocked (absl maps are not exception-safe), so a refused growth is only reported
  // after the entry is fully published; a later call then finds it.
  // @throw utils::OutOfMemoryException
  storage::Edge *GetOrCreate(uint64_t gid) {
    if (auto *found = Find(gid); found != nullptr) return found;
    Shard &shard = ShardFor(gid);
    return utils::AtomicMemoryBlock([&] {
      std::unique_lock guard{shard.mutex};
      auto [it, inserted] = shard.map.try_emplace(gid);
      if (inserted) {
        try {
          it->second = memory::MakeDbAwareUnique<storage::Edge>(storage::Gid::FromUint(gid), nullptr);
        } catch (...) {
          shard.map.erase(it);
          throw;
        }
      }
      return it->second.get();
    });
  }

  // Calls `fn(gid, edge)` for every stub in unspecified order until it returns false. Concurrent creators may or may
  // not be visited. Returns true if every stub was visited, false if `fn` stopped the walk. For recovery.
  template <typename Fn>
    requires std::predicate<Fn &, uint64_t, storage::Edge *>
  bool ForEach(Fn &&fn) const {
    for (const Shard &shard : shards_) {
      std::shared_lock guard{shard.mutex};
      for (const auto &[gid, edge] : shard.map) {
        if (!fn(gid, edge.get())) return false;
      }
    }
    return true;
  }

 private:
  static constexpr size_t kShards = 64;

  struct alignas(64) Shard {
    mutable std::shared_mutex mutex;
    absl::flat_hash_map<uint64_t, memory::ArenaAwareUniquePtr<storage::Edge>> map;
  };

  Shard &ShardFor(uint64_t gid) noexcept { return shards_[gid % kShards]; }

  const Shard &ShardFor(uint64_t gid) const noexcept { return shards_[gid % kShards]; }

  std::array<Shard, kShards> shards_;
};

}  // namespace memgraph::versioning
