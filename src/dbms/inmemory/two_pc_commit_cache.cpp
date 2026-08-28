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
#include "utils/logging.hpp"

#include <utility>

namespace memgraph::dbms {

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

utils::Synchronized<TwoPCCommitCache::Record, std::mutex> *TwoPCCommitCache::installed_slot_ = nullptr;

auto TwoPCCommitCache::Slot() -> utils::Synchronized<Record, std::mutex> & {
  if (installed_slot_ != nullptr) return *installed_slot_;
  // No Owner installed (unit tests, embedded use): fall back to a lazily-created, leaked singleton --
  // the lifetime the whole slot used to have. Production installs an Owner in main() before any
  // replica RPC can populate the slot, so this branch is never taken there.
  static auto *fallback = new utils::Synchronized<Record, std::mutex>{};
  return *fallback;
}

TwoPCCommitCache::Owner::Owner() {
  MG_ASSERT(installed_slot_ == nullptr, "TwoPCCommitCache::Owner constructed more than once");
  installed_slot_ = new utils::Synchronized<Record, std::mutex>{};
}

TwoPCCommitCache::Owner::~Owner() {
  auto *slot = std::exchange(installed_slot_, nullptr);
  if (slot == nullptr) return;
  // The ordered ~Database drain (dbms/database.cpp) should have emptied the slot before we get here.
  // If anything is somehow still cached its storage is already gone, so DESTROYING the accessor would
  // be a use-after-free -- detach and leak it (release()) instead, exactly as the old never-freed slot
  // did implicitly. Only then free the now-empty Synchronized shell.
  slot->WithLock([](Record &cache) {
    if (cache.commit_accessor_) {
      spdlog::error(
          "TwoPCCommitCache::~Owner: slot still populated at shutdown -- leaking the accessor rather than aborting "
          "it against freed storage.");
      (void)cache.commit_accessor_.release();
    }
  });
  delete slot;
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
