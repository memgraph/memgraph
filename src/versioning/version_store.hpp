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
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "utils/spin_lock.hpp"
#include "versioning/branch_info.hpp"
#include "versioning/branch_ledger.hpp"

namespace memgraph::versioning {

class BranchCore;

// Per-Database registry of branches. Starts empty; Database restores it from the ledger at startup (Restore/
// ResetLedger). Once a BranchLedger is attached, every CREATE/DROP (and the first gid-floor record per branch) is also
// persisted to it.
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

  // Called with the durable timestamp the branches still need recoverable (oldest recorded fork_ts - 1), or nullopt
  // when no branch exists: after Restore, after a failed CREATE (its pin is released) and after a DROP. Set before use.
  // Never called with lock_ held; may be called with the ledger mutex held, so it must not call back into this store.
  void SetRetentionFloorCallback(std::function<void(std::optional<uint64_t>)> callback);

  // Starts persisting to `file`. Call before the first CreateBranch; does not read or write the file itself.
  void AttachLedger(std::filesystem::path file);

  // Startup only, before any other call. Installs `contents` as the registry without writing the ledger: the branch at
  // index i gets `pins[i]` as its runtime fork_ts (already registered with the storage by the caller; DropBranch
  // releases it) and keeps its recorded fork_ts as `recorded_fork_ts`, so later ledger writes persist the recorded
  // value. Restores next_number. `pins` must have exactly one entry per branch.
  void Restore(const LedgerContents &contents, std::span<const uint64_t> pins);

  // Startup only, before any other call. Discards unrecoverable branches: persists an empty registry that keeps
  // `next_number` so branch numbers are still never reused. A write failure is logged, not fatal.
  void ResetLedger(uint64_t next_number);

  // `name` must be new and `parent` must be "main" or an existing branch. On success acquires the fork pin; on
  // failure (including a ledger write failure) returns a user-facing message and changes no state (no pin held).
  std::expected<BranchInfo, std::string> CreateBranch(std::string name, std::string parent,
                                                      std::optional<std::string> description);

  // Returns false, changing nothing, if `name` doesn't exist or has an attached session. Otherwise releases the fork
  // pin and retires the branch's core. A ledger write failure is reported as an error (nothing is dropped).
  std::expected<bool, std::string> DropBranch(std::string_view name);

  // Records the branch's gid floors in the ledger the first time they are known; later calls are no-ops. Fails
  // only on a ledger write failure. Unknown `number` is a no-op.
  std::expected<void, std::string> RecordGidFloors(uint64_t number, uint64_t vertex_gid_floor, uint64_t edge_gid_floor);

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
  std::function<void(std::optional<uint64_t>)> retention_floor_callback_;

  // Serializes registry mutations that touch the ledger so the (blocking) ledger write runs without lock_ held.
  // Lock order: ledger_mutex_ then lock_. Guards ledger_.
  std::mutex ledger_mutex_;
  std::optional<BranchLedger> ledger_;
  // Branches whose DROP is writing the ledger; TryAcquireCheckout refuses them. Guarded by lock_.
  std::set<std::string, std::less<>> dropping_;

  // Call with ledger_mutex_ held and lock_ NOT held. No-op if no ledger is attached.
  std::expected<void, std::string> WriteLedger(const LedgerContents &contents);
  // Call with lock_ held.
  LedgerContents SnapshotLocked() const;

  // Reports the retention floor of the current branches. Call WITHOUT lock_ held. No-op without a callback.
  void PublishRetentionFloor() const;

  // Destroys the core outside cores_mutex_ (its destructor tears down a storage engine). Call WITHOUT lock_ held.
  void RetireCore(uint64_t number);

  // Guards cores_ only; never nested with lock_.
  mutable std::mutex cores_mutex_;
  std::map<uint64_t, std::shared_ptr<BranchCore>> cores_;
};

}  // namespace memgraph::versioning
