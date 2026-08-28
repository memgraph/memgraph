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
// FinalizeCommitRpc.
//
// Lock discipline (load-bearing): every method extracts the accessor under the slot lock and runs
// AbortAndResetCommitTs()/FinalizeCommitPhase()/destruction on it OUTSIDE the lock -- those take
// engine_lock_, which must never be held under the slot lock. ~Database -> AbortTwoPCForTenant can
// run on the defer_pool_ thread (dbms/handler.hpp) concurrently with an RPC handler, so the slot
// lock is what serialises them.
//
// The slot is owned by an RAII Owner (below) held in main(), freed before static destruction -- the
// same pattern as utils::PageCacheReleaser. With no Owner installed (unit tests) Slot() uses a leaked
// fallback.
class TwoPCCommitCache {
 public:
  TwoPCCommitCache() = delete;

  // RAII owner of the process-wide slot; construct exactly one in main() BEFORE the DbmsHandler, so
  // the slot outlives every Database and is freed only after ~DbmsHandler has drained it (~Database
  // empties each tenant's entry while its storage is still alive).
  class Owner {
   public:
    Owner();
    ~Owner();
    Owner(Owner const &) = delete;
    Owner &operator=(Owner const &) = delete;
    Owner(Owner &&) = delete;
    Owner &operator=(Owner &&) = delete;
  };

  // Captures the tenant uuid at populate time rather than re-deriving it from the accessor later, so
  // Take* never depend on destroy-before-extract ordering (robustness, not a reachable-UAF fix).
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

  // The live slot: the Owner-installed one when present, else a leaked fallback (see the class
  // comment). A member function rather than a data member so Record can stay incomplete here.
  static auto Slot() -> utils::Synchronized<Record, std::mutex> &;

  // Installed by Owner's constructor, cleared by its destructor; nullptr when no Owner exists, in
  // which case Slot() uses the fallback. Pointer-to-incomplete is fine.
  static utils::Synchronized<Record, std::mutex> *installed_slot_;
};

}  // namespace memgraph::dbms
