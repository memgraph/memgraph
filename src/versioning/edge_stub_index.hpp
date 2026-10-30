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
  // The stub object: an Edge that also carries the fork edge it was created for. Every Edge* this index hands out
  // points at the base of one of these.
  struct ForkEdgeStub : storage::Edge {
    ForkEdgeStub(storage::Gid gid, std::optional<storage::EdgeRef> fork_edge) noexcept
        : storage::Edge(gid, nullptr),
          fork{fork_edge.value_or(storage::EdgeRef{static_cast<storage::Edge *>(nullptr)})},
          has_fork{fork_edge.has_value()} {}

    storage::EdgeRef fork;
    bool has_fork;
  };

  // Like FindFork(gid) for a stub this index handed out, without a lookup: the fork edge is read off the stub, which
  // fixed it at creation (before the stub was published) and never changes it.
  static std::optional<storage::EdgeRef> ForkOf(const storage::Edge *stub) noexcept {
    const auto &owned = static_cast<const ForkEdgeStub &>(*stub);
    if (!owned.has_fork) return std::nullopt;
    return owned.fork;
  }

  storage::Edge *Find(uint64_t gid) const {
    const Shard &shard = ShardFor(gid);
    std::shared_lock guard{shard.mutex};
    const auto it = shard.map.find(gid);
    return it == shard.map.end() ? nullptr : static_cast<storage::Edge *>(it->second.stub.get());
  }

  // The fork edge `gid`'s stub was created for, as main holds it (a pointer with properties_on_edges, else the gid), so
  // a stub, or a marker entry that holds it, reaches its fork edge without a walk of the fork vertex's adjacency. Empty
  // for a stub created without one.
  std::optional<storage::EdgeRef> FindFork(uint64_t gid) const {
    const Shard &shard = ShardFor(gid);
    std::shared_lock guard{shard.mutex};
    const auto it = shard.map.find(gid);
    if (it == shard.map.end()) return std::nullopt;
    return ForkOf(it->second.stub.get());
  }

  // Returns the stub for `gid`, creating a delta-less one if none exists. Concurrent callers for one gid get the same
  // object. The insert runs OOM-blocked (absl maps are not exception-safe), so a refused growth is only reported
  // after the entry is fully published; a later call then finds it. `fork` is recorded by the call that creates the
  // stub.
  // @throw utils::OutOfMemoryException
  storage::Edge *GetOrCreate(uint64_t gid, std::optional<storage::EdgeRef> fork = std::nullopt) {
    if (auto *found = Find(gid); found != nullptr) return found;
    Shard &shard = ShardFor(gid);
    return utils::AtomicMemoryBlock([&] {
      std::unique_lock guard{shard.mutex};
      auto [it, inserted] = shard.map.try_emplace(gid);
      if (inserted) {
        try {
          it->second.stub = memory::MakeDbAwareUnique<ForkEdgeStub>(storage::Gid::FromUint(gid), fork);
        } catch (...) {
          shard.map.erase(it);
          throw;
        }
      }
      return static_cast<storage::Edge *>(it->second.stub.get());
    });
  }

  // Calls `fn(gid, edge)` for every stub in unspecified order until it returns false. Concurrent creators may or may
  // not be visited. Returns true if every stub was visited, false if `fn` stopped the walk. For recovery.
  template <typename Fn>
    requires std::predicate<Fn &, uint64_t, storage::Edge *>
  bool ForEach(Fn &&fn) const {
    for (const Shard &shard : shards_) {
      std::shared_lock guard{shard.mutex};
      for (const auto &[gid, entry] : shard.map) {
        if (!fn(gid, static_cast<storage::Edge *>(entry.stub.get()))) return false;
      }
    }
    return true;
  }

 private:
  static constexpr size_t kShards = 64;

  struct Entry {
    memory::ArenaAwareUniquePtr<ForkEdgeStub> stub;
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
