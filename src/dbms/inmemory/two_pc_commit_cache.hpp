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

// Process-wide single slot holding the in-flight 2PC commit accessor between PrepareCommitRpc and
// FinalizeCommitRpc; single slot rather than per-tenant, per AbortTwoPCForTenant's declaration
// comment (dbms/inmemory/replication_handlers.hpp).
//
// Lock discipline (load-bearing): every method EXTRACTS the accessor under the slot lock and returns
// with the lock released; callers run AbortAndResetCommitTs()/FinalizeCommitPhase()/destruction on
// the extracted local OUTSIDE the lock. Those take engine_lock_, and the slot lock must never be held
// across that. Concurrent because ~Database -> AbortTwoPCForTenant may run on DeferDelete's
// defer_pool_ thread (dbms/handler.hpp) tearing down tenant B while the RPC thread serves tenant A.
//
// Slot() is heap-allocated and deliberately never freed. No static destructor: a populated static
// slot would be destroyed after main() returns, once every Database/InMemoryStorage is gone, so
// destroying the cached accessor would dereference freed storage. main's shutdown clears the slot in
// order while storages are alive; on paths that bypass that, leaking (not aborting) is strictly
// safer. Also avoids a cross-TU destruction-order dependency with ~Database (dbms/database.cpp).
class TwoPCCommitCache {
 public:
  // Static-only utility; all state lives in Slot().
  TwoPCCommitCache() = delete;

  // Captures the tenant uuid from storage->uuid() rather than re-deriving it from accessor->uuid()
  // later, so TakeForTenant/TakeMatching don't depend on destroy-before-extract ordering at the call
  // sites (robustness, not a fix for a reachable use-after-free).
  static void Store(std::unique_ptr<storage::ReplicationAccessor> accessor, uint64_t durability_commit_timestamp,
                    utils::UUID uuid);

  struct Taken {
    std::unique_ptr<storage::ReplicationAccessor> accessor;          // non-null only when the timestamp matched
    std::optional<uint64_t> mismatched_durability_commit_timestamp;  // set only when the slot stayed populated
  };

  // Takes the accessor out iff `durability_commit_timestamp` matches the cached one. On mismatch the
  // slot is LEFT POPULATED and the cached value returned via mismatched_durability_commit_timestamp --
  // distinct from the "missing" case (both fields empty), which the caller treats as terminal.
  [[nodiscard]] static auto TakeMatching(uint64_t durability_commit_timestamp) -> Taken;

  // Takes the accessor out iff it belongs to `uuid` (the uuid captured by Store; see Store).
  // No-op returning nullptr otherwise, so a pending 2PC for a different tenant is not wrongly dropped.
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
