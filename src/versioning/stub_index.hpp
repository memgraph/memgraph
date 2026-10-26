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
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <shared_mutex>

#include "absl/container/flat_hash_map.h"
#include "storage/v2/vertex.hpp"
#include "utils/rw_spin_lock.hpp"

namespace memgraph::versioning {

// Gid-keyed table of a branch's fork-vertex stubs: for each fork gid that has a stub, the stub's diff-engine Vertex*
// and the fork (main) Vertex* it shadows. Replaces a skiplist lookup per read with one hash probe.
//
// Memory is proportional to the number of recorded gids (~24 B per entry plus open-addressing slack), independent of
// the graph size or how the changed gids are spread. Keys are spread over kShards independently locked
// absl::flat_hash_maps so concurrent COW inserts and reads rarely contend; reads take the shard lock shared, inserts
// take it unique. Shards are cache-line aligned to avoid false sharing between locks.
//
// Both pointers are stable for the owning BranchCore's lifetime: stubs are never erased from the diff engine, and a
// fork vertex visible at fork_ts cannot be freed while the fork pin is held. Entries are never removed or changed, so
// Insert is idempotent and a reader sees either null or the final pointer.
//
// A null result only means "not recorded here"; callers must fall back to the skiplist, which stays authoritative.
class StubIndex {
 public:
  StubIndex() = default;

  StubIndex(const StubIndex &) = delete;
  StubIndex &operator=(const StubIndex &) = delete;
  StubIndex(StubIndex &&) = delete;
  StubIndex &operator=(StubIndex &&) = delete;
  ~StubIndex() = default;

  // Records non-null pointers for `gid`. Concurrent-safe.
  // @throw std::bad_alloc
  void Insert(uint64_t gid, storage::Vertex *stub, storage::Vertex *fork) {
    if (stub == nullptr && fork == nullptr) return;
    Shard &shard = ShardFor(gid);
    std::unique_lock guard{shard.lock};
    Entry &entry = shard.map.try_emplace(gid).first->second;
    if (fork != nullptr) entry.fork = fork;
    if (stub != nullptr) entry.stub = stub;
  }

  storage::Vertex *FindStub(uint64_t gid) const noexcept {
    const Shard &shard = ShardFor(gid);
    std::shared_lock guard{shard.lock};
    const auto it = shard.map.find(gid);
    return it == shard.map.end() ? nullptr : it->second.stub;
  }

  storage::Vertex *FindFork(uint64_t gid) const noexcept {
    const Shard &shard = ShardFor(gid);
    std::shared_lock guard{shard.lock};
    const auto it = shard.map.find(gid);
    return it == shard.map.end() ? nullptr : it->second.fork;
  }

 private:
  static constexpr unsigned kShardBits = 6;
  static constexpr size_t kShards = size_t{1} << kShardBits;

  struct Entry {
    storage::Vertex *stub{nullptr};
    storage::Vertex *fork{nullptr};
  };

  struct alignas(64) Shard {
    mutable utils::RWSpinLock lock;
    absl::flat_hash_map<uint64_t, Entry> map;
  };

  // Fibonacci hash, shard from the high bits so sequential gids spread across shards.
  static constexpr size_t ShardIndex(uint64_t gid) noexcept {
    return static_cast<size_t>((gid * 0x9E3779B97F4A7C15ULL) >> (64 - kShardBits));
  }

  Shard &ShardFor(uint64_t gid) noexcept { return shards_[ShardIndex(gid)]; }

  const Shard &ShardFor(uint64_t gid) const noexcept { return shards_[ShardIndex(gid)]; }

  std::array<Shard, kShards> shards_;
};

}  // namespace memgraph::versioning
