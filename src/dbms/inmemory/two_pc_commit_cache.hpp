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

#include "storage/v2/inmemory/storagefwd.hpp"

#include "utils/synchronized.hpp"
#include "utils/uuid.hpp"

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>

namespace memgraph::dbms {

// Single global slot holding the in-flight 2PC commit accessor between PrepareCommitRpc and
// FinalizeCommitRpc (see InMemoryReplicationHandlers::AbortTwoPCForTenant's declaration comment
// in dbms/inmemory/replication_handlers.hpp for why it is a single slot rather than one per tenant).
//
// Guards the slot with a private mutex. This lock is load-bearing, not prophylactic: ~Database
// calls AbortTwoPCForTenant, and it runs on whichever thread destroys the Database -- including
// DeferDelete's background defer_pool_ thread (dbms/handler.hpp), which can be tearing down tenant B
// while the replica RPC thread services tenant A. Discipline: EXTRACT the accessor out of the slot
// while holding the lock, then run AbortAndResetCommitTs()/FinalizeCommitPhase()/destruction on the
// extracted local OUTSIDE the lock -- those walk the whole transaction's deltas and take
// engine_lock_, and this lock must never be held across that. Every public method below returns
// with the lock released and never touches the accessor itself, so callers get this discipline for
// free.
//
// The slot (Slot(), below) is heap-allocated and deliberately never freed -- one slot for the whole
// process, holding at most one accessor, so the leak is bounded. Two reasons this matters, both
// load-bearing:
//  1. No static destructor. A static object here that is still populated at process exit would be
//     destroyed after main() returns, i.e. after every Database/InMemoryStorage is already gone,
//     and destroying the cached ReplicationAccessor then dereferences freed storage
//     (~InMemoryAccessor calls Abort()/FinalizeTransaction(); ~ResourceLockGuard unlocks a freed
//     main_lock_). main's shutdown path already clears this slot in order while the storages are
//     alive; leaking the slot only matters for paths that bypass that ordered clear, where not
//     aborting is strictly safer than aborting against freed memory.
//  2. No cross-TU destruction-order dependency. ~Database (dbms/database.cpp) calls
//     AbortTwoPCForTenant, and destruction order across translation units is unspecified, so a
//     Database with static storage duration could otherwise reach a static slot here after it had
//     already been destroyed.
class TwoPCCommitCache {
 public:
  // Static-only: all state lives in the process-wide Slot() (below), so the type is never instantiated.
  TwoPCCommitCache() = delete;

  // Populates the slot, capturing the tenant uuid from storage->uuid() rather than deriving it
  // later from accessor->uuid() (storage_->uuid(), storage.hpp:846). This decouples the uuid
  // checks in TakeForTenant/TakeMatching from relying on cross-file destroy-before-extract
  // ordering holding at every call site -- robustness, not a fix for a currently-reachable
  // use-after-free.
  static void Store(std::unique_ptr<storage::ReplicationAccessor> accessor, uint64_t durability_commit_timestamp,
                    utils::UUID uuid);

  struct Taken {
    std::unique_ptr<storage::ReplicationAccessor> accessor;          // non-null only when the ldt matched
    std::optional<uint64_t> mismatched_durability_commit_timestamp;  // set only when the slot stayed populated
  };

  // FinalizeCommitHandler's read: takes the accessor out iff `durability_commit_timestamp` matches
  // the cached one. On mismatch the slot is deliberately LEFT POPULATED and the cached value is
  // reported via mismatched_durability_commit_timestamp -- unlike the "missing" case (both fields
  // empty), a mismatch is not treated as terminal by the caller.
  [[nodiscard]] static auto TakeMatching(uint64_t durability_commit_timestamp) -> Taken;

  // Takes the accessor out iff it belongs to `uuid` (the uuid captured by Store, not one
  // re-derived from the accessor -- see Store's comment for why). No-op (returns nullptr)
  // otherwise, so a pending 2PC for a different tenant is not wrongly dropped.
  [[nodiscard]] static auto TakeForTenant(utils::UUID const &uuid) -> std::unique_ptr<storage::ReplicationAccessor>;

  // Takes whatever is cached, regardless of tenant.
  [[nodiscard]] static auto TakeAny() -> std::unique_ptr<storage::ReplicationAccessor>;

 private:
  // Defined in two_pc_commit_cache.cpp, where storage::ReplicationAccessor is a complete type.
  // Kept incomplete here so this header does not have to pull in the full storage definition.
  struct Record;

  // The Synchronized<Record> backing the cache, heap-allocated and never freed -- see the class
  // comment above for why. Returning it via a function (rather than a member field) lets Record
  // stay incomplete in this header.
  static auto Slot() -> utils::Synchronized<Record, std::mutex> &;
};

}  // namespace memgraph::dbms
