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

// Per-Database registry of branches. Starts empty; Database restores it at startup (Restore/ResetLedger). Once a
// BranchLedger is attached, every CREATE/DROP and the first gid-floor record per branch is persisted to it.
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
  // Withdraws this store's branches and checkouts from the process-wide active gauges.
  ~VersionStore();

  // Receives the durable timestamp branches still need recoverable (oldest recorded_fork_ts - 1), or nullopt when no
  // branch exists; invoked after Restore, a failed CREATE and a DROP. Set before use. Never called with lock_ held, but
  // may be with ledger_mutex_ held, so it must not call back into this store.
  void SetRetentionFloorCallback(std::function<void(std::optional<uint64_t>)> callback);

  // Starts persisting to `file`. Call before the first CreateBranch; does not read or write the file itself.
  void AttachLedger(std::filesystem::path file);

  // Startup only, before any other call. Installs `contents` without writing the ledger: branch i gets `pins[i]` as its
  // runtime fork_ts (already registered with the storage by the caller; DropBranch releases it) and keeps the recorded
  // value in `recorded_fork_ts`. `pins` must have exactly one entry per branch.
  void Restore(const LedgerContents &contents, std::span<const uint64_t> pins);

  // Startup only, before any other call, instead of Restore when versioning is off. Keeps `contents` as hidden
  // branches: they are not listed, checked out or droppable, but they stay in every ledger write, count against Empty()
  // and keep the retention floor at their oldest fork point, so the next start with versioning on restores them.
  // Does not write the ledger.
  void RestoreHidden(const LedgerContents &contents);

  // Startup only, before any other call. Discards unrecoverable branches: persists an empty registry that keeps
  // `next_number` so branch numbers are still never reused. A write failure is logged, not fatal.
  void ResetLedger(uint64_t next_number);

  // `name` must be new and `parent` must be "main" or an existing branch. On success acquires the fork pin; on
  // failure (including a ledger write failure) returns a user-facing message and changes no state (no pin held).
  std::expected<BranchInfo, std::string> CreateBranch(std::string name, std::string parent);

  // Returns false, changing nothing, if `name` doesn't exist or has an attached session. Otherwise releases the fork
  // pin, retires the branch's core and, with `remove_files`, deletes its directory (recovery keeps the files of a
  // branch it could not restore). A ledger write failure is reported as an error (nothing is dropped).
  std::expected<bool, std::string> DropBranch(std::string_view name, bool remove_files = true);

  // As DropBranch, but drops although sessions are attached. Their attaches stay valid until released (keyed by branch
  // number, so a branch re-created under the same name is unaffected) and their cores live until they let go.
  std::expected<bool, std::string> ForceDropBranch(std::string_view name);

  // Replica demotion: refuses new branches and checkouts from here on (until Unfreeze), then durably drops every
  // branch, including those with attached sessions (their ReleaseCheckout stays valid and keeps the gauges balanced).
  // Branch numbering is kept. Idempotent. A ledger write failure is returned, leaving the store unchanged (including
  // its frozen state).
  std::expected<void, std::string> FreezeAndDropAll();

  void Unfreeze();

  bool Frozen() const;

  // Persists the branch's gid floors the first time they are known; later calls and unknown `number` are no-ops.
  // Fails only on a ledger write failure.
  std::expected<void, std::string> RecordGidFloors(uint64_t number, uint64_t vertex_gid_floor, uint64_t edge_gid_floor);

  std::optional<BranchInfo> Get(std::string_view name) const;

  // Ordered by ascending branch number (creation order).
  std::vector<std::pair<std::string, BranchInfo>> List() const;

  bool Exists(std::string_view name) const;

  // True iff `name` is a live branch with this `number` (false after a drop, even if the name was re-created).
  bool IsLive(std::string_view name, uint64_t number) const;

  // True iff no branches exist, hidden ones included (`main` is not counted). Storage-global destructive ops (STORAGE
  // MODE, RECOVER SNAPSHOT, DROP GRAPH) are rejected while any branch exists.
  bool Empty() const;

  // Attaches a session to `name`; any number may attach, all sharing the branch's BranchCore. Returns the BranchInfo
  // captured atomically with the attach (no race with drop + same-name re-create). A branch with an attached session
  // cannot be dropped without ForceDropBranch. Every successful attach MUST be paired with exactly one
  // ReleaseCheckout(info.number).
  std::expected<BranchInfo, std::string> TryAcquireCheckout(std::string_view name);

  // Releases one attach of the branch `number`, also after that branch was dropped. No-op without an attach.
  void ReleaseCheckout(uint64_t number);

  // Attaches of the live branch `name`; 0 if there is none.
  size_t CheckoutCount(std::string_view name) const;

  // One live BranchCore per branch number. Held here, not in dbms::Database, so cores are destroyed with the
  // VersionStore, strictly before the storage they point into.
  using CoreBuilder = std::function<std::expected<std::shared_ptr<BranchCore>, std::string>()>;

  // Returns the live core of `number`, building it on first use; racing first checkouts wait for one build. Caller must
  // hold an attach, which keeps DropBranch from retiring the core mid-build. Fails, keeping no core, if `number` is not
  // a live branch (dropped, e.g. by FreezeAndDropAll, which does not wait for attached sessions).
  std::expected<std::shared_ptr<BranchCore>, std::string> GetOrBuildCore(uint64_t number, const CoreBuilder &build);

 private:
  mutable utils::SpinLock lock_;
  std::map<std::string, BranchInfo, std::less<>> branches_;
  // Branches kept only in the ledger while versioning is off (RestoreHidden). Guarded by lock_.
  std::vector<LedgerBranch> hidden_;
  // Attached sessions per branch number; entries are erased at zero.
  std::map<uint64_t, size_t> checked_out_;
  size_t total_checkouts_{0};
  uint64_t next_number_{2};
  std::function<uint64_t()> acquire_pin_;
  std::function<void(uint64_t)> release_pin_;
  std::function<void(std::optional<uint64_t>)> retention_floor_callback_;

  // Serializes registry mutations that touch the ledger so the (blocking) ledger write runs without lock_ held.
  // Lock order: ledger_mutex_ then lock_. Guards ledger_.
  std::mutex ledger_mutex_;
  std::optional<BranchLedger> ledger_;
  // Parent of the ledger file: holds one directory per branch number. Empty without a ledger. Guarded by ledger_mutex_.
  std::filesystem::path branches_root_;
  // Branches whose DROP is writing the ledger; TryAcquireCheckout refuses them. Guarded by lock_.
  std::set<std::string, std::less<>> dropping_;
  // Set by FreezeAndDropAll under ledger_mutex_; CreateBranch and TryAcquireCheckout refuse. Guarded by lock_.
  bool frozen_{false};

  // Call with ledger_mutex_ held and lock_ NOT held. No-op if no ledger is attached.
  std::expected<void, std::string> WriteLedger(const LedgerContents &contents);

  std::expected<bool, std::string> DropBranchImpl(std::string_view name, bool remove_files, bool force);
  LedgerContents SnapshotLocked() const;

  // Call WITHOUT lock_ held (it takes lock_ itself).
  void PublishRetentionFloor() const;

  // Destroys the core outside cores_mutex_ (its destructor tears down a storage engine). Call WITHOUT lock_ held.
  // Returns a weak reference that is non-expired iff another holder still keeps the core (and its directory) alive.
  std::weak_ptr<BranchCore> RetireCore(uint64_t number);

  // Removes the branch directory now, or queues it until the core is gone and no session of `number` is attached.
  // Call without lock_, cores_mutex_ or pending_mutex_ held; the caller holds ledger_mutex_ (reads branches_root_).
  void RemoveOrDefer(uint64_t number, std::weak_ptr<BranchCore> remaining);
  // Removes queued directories whose core is gone and whose branch has no attached session. Call without lock_;
  // file removal runs unlocked.
  void SweepPendingRemovals();
  static void RemoveDirectory(const std::filesystem::path &directory);

  // Takes lock_; call without it.
  bool IsLiveNumber(uint64_t number) const;

  // Serializes builds so one branch directory is never built twice; order: build_mutex_ -> cores_mutex_.
  std::mutex build_mutex_;
  // Guards cores_ only; never nested with lock_.
  mutable std::mutex cores_mutex_;
  std::map<uint64_t, std::shared_ptr<BranchCore>> cores_;

  struct PendingRemoval {
    std::weak_ptr<BranchCore> core;
    uint64_t number;
    std::filesystem::path directory;
  };

  // Dropped branches whose directory awaits the last core holder. Taken after lock_ when both are needed; nothing is
  // acquired under it.
  std::mutex pending_mutex_;
  std::vector<PendingRemoval> pending_removals_;
};

}  // namespace memgraph::versioning
