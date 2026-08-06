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

#include "dbms/inmemory/two_pc_commit_cache.hpp"

#include "storage/v2/inmemory/storage.hpp"

#include <utility>

namespace memgraph::dbms {

// Backing storage for the slot: the cached accessor plus metadata to validate and scope it. See
// TwoPCCommitCache's class comment (two_pc_commit_cache.hpp) for the lifetime discipline.
struct TwoPCCommitCache::Record {
  std::unique_ptr<storage::ReplicationAccessor> commit_accessor_;
  uint64_t durability_commit_timestamp_{};
  // Captured from storage->uuid() by Store, never re-derived from commit_accessor_->uuid() --
  // see Store's declaration comment in two_pc_commit_cache.hpp for why.
  utils::UUID uuid_{};

  Record() = default;
  Record(Record &&) = default;
  Record(Record const &) = delete;
  // Deleted deliberately: implicit member-wise move-assignment could release a future
  // lifetime-owning member out of order relative to commit_accessor_'s destruction.
  Record &operator=(Record &&) = delete;
  Record &operator=(Record const &) = delete;
};

auto TwoPCCommitCache::Slot() -> utils::Synchronized<Record, std::mutex> & {
  static auto *slot = new utils::Synchronized<Record, std::mutex>{};
  return *slot;
}

auto TwoPCCommitCache::Instance() -> TwoPCCommitCache & {
  static auto *instance = new TwoPCCommitCache{};
  return *instance;
}

void TwoPCCommitCache::Store(std::unique_ptr<storage::ReplicationAccessor> accessor,
                             uint64_t durability_commit_timestamp, utils::UUID uuid) {
  // Extract any pre-existing accessor instead of move-assigning over it in place: destroying it
  // under the lock would run ~InMemoryAccessor's Abort(), which takes engine_lock_.
  auto stale = Slot().WithLock([&](Record &cache) {
    auto stale = std::move(cache.commit_accessor_);
    cache.commit_accessor_ = std::move(accessor);
    cache.durability_commit_timestamp_ = durability_commit_timestamp;
    cache.uuid_ = uuid;
    return stale;
  });
}

auto TwoPCCommitCache::TakeMatching(uint64_t durability_commit_timestamp) -> Taken {
  return Slot().WithLock([&](Record &cache) -> Taken {
    if (!cache.commit_accessor_) {
      return {};
    }
    if (durability_commit_timestamp != cache.durability_commit_timestamp_) {
      return {.mismatched_durability_commit_timestamp = cache.durability_commit_timestamp_};
    }
    return {.accessor = std::move(cache.commit_accessor_)};
  });
}

auto TwoPCCommitCache::TakeForTenant(utils::UUID const &uuid) -> std::unique_ptr<storage::ReplicationAccessor> {
  return Slot().WithLock([&](Record &cache) -> std::unique_ptr<storage::ReplicationAccessor> {
    if (cache.commit_accessor_ && cache.uuid_ == uuid) {
      return std::move(cache.commit_accessor_);
    }
    return nullptr;
  });
}

auto TwoPCCommitCache::TakeAny() -> std::unique_ptr<storage::ReplicationAccessor> {
  return Slot().WithLock(
      [](Record &cache) -> std::unique_ptr<storage::ReplicationAccessor> { return std::move(cache.commit_accessor_); });
}

}  // namespace memgraph::dbms
