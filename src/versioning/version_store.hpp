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

#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "utils/spin_lock.hpp"

namespace memgraph::versioning {

class BranchCore;

// `number` is never reused, even across drops (main is implicit version 1, the first branch is 2).
// `fork_ts` is the GC fork-pin timestamp, held until the branch is dropped.
struct BranchInfo {
  uint64_t number;
  std::string parent;
  uint64_t fork_ts;
  std::optional<std::string> description;
};

// Per-Database in-memory registry of branches. Ephemeral: a restart starts empty.
//
// The GC fork pin held for each branch is injected as callbacks (Database binds them to
// InMemoryStorage::RegisterForkPin/ReleaseForkPin), so tests need no storage engine.
//
// Structural validation only (unknown parent, duplicate name): callers MUST run name validation
// (ValidateBranchName) and gate/privilege checks before CreateBranch/DropBranch.
class VersionStore {
 public:
  // The callbacks must stay bound to the owning storage's fork-pin primitive for this VersionStore's lifetime.
  VersionStore(std::function<uint64_t()> acquire_pin, std::function<void(uint64_t)> release_pin);

  VersionStore(const VersionStore &) = delete;
  VersionStore &operator=(const VersionStore &) = delete;
  VersionStore(VersionStore &&) = delete;
  VersionStore &operator=(VersionStore &&) = delete;
  ~VersionStore() = default;

  // `name` must be new and `parent` must be "main" or an existing branch. On success acquires the fork pin; on
  // failure returns a user-facing message and changes no state (no pin acquired).
  std::expected<BranchInfo, std::string> CreateBranch(std::string name, std::string parent,
                                                      std::optional<std::string> description);

  // Returns false, changing nothing, if `name` doesn't exist or has an attached session. Otherwise releases the fork
  // pin and retires the branch's core.
  bool DropBranch(std::string_view name);

  std::optional<BranchInfo> Get(std::string_view name) const;

  // Ordered by ascending branch number (creation order).
  std::vector<std::pair<std::string, BranchInfo>> List() const;

  bool Exists(std::string_view name) const;

  // True iff no branches exist (`main` is not counted). Storage-global destructive ops (STORAGE MODE, RECOVER
  // SNAPSHOT, DROP GRAPH) are rejected while any branch exists.
  bool Empty() const;

  // Attaches a session to `name`; any number may attach, all sharing the branch's BranchCore. Returns the BranchInfo
  // captured atomically with the attach (no race with drop + same-name re-create). A branch with an attached session
  // cannot be dropped. Every successful attach MUST be paired with exactly one ReleaseCheckout.
  std::expected<BranchInfo, std::string> TryAcquireCheckout(std::string_view name);

  // No-op if `name` has no attached session.
  void ReleaseCheckout(std::string_view name);

  size_t CheckoutCount(std::string_view name) const;

  // One live BranchCore per branch number. Held here, not in dbms::Database, so cores are destroyed with the
  // VersionStore, strictly before the storage they point into.
  using CoreBuilder = std::function<std::expected<std::shared_ptr<BranchCore>, std::string>()>;

  // Returns the live core of `number`, building it on first use. `build` runs without the registry mutex, so racing
  // first checkouts may both build and the loser is discarded. Caller must hold an attach, which keeps DropBranch
  // from retiring the core mid-build.
  std::expected<std::shared_ptr<BranchCore>, std::string> GetOrBuildCore(uint64_t number, const CoreBuilder &build);

 private:
  mutable utils::SpinLock lock_;
  std::map<std::string, BranchInfo, std::less<>> branches_;
  // Attached sessions per branch; entries are erased at zero.
  std::map<std::string, size_t, std::less<>> checked_out_;
  size_t total_checkouts_{0};
  uint64_t next_number_{2};
  std::function<uint64_t()> acquire_pin_;
  std::function<void(uint64_t)> release_pin_;

  // Destroys the core outside cores_mutex_ (its destructor tears down a storage engine). Call WITHOUT lock_ held.
  void RetireCore(uint64_t number);

  // Guards cores_ only; never nested with lock_.
  mutable std::mutex cores_mutex_;
  std::map<uint64_t, std::shared_ptr<BranchCore>> cores_;
};

}  // namespace memgraph::versioning
