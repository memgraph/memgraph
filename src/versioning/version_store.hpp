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
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "utils/spin_lock.hpp"
#include "versioning/branch_info.hpp"
#include "versioning/branch_ledger.hpp"

namespace memgraph::versioning {

class BranchCore;

// Outcome of a DROP that may be refused.
enum class DropResult : uint8_t { kDropped, kNotFound, kInUse };

// Per-Database registry of branches. Starts empty; Database restores it at startup (Restore/RestoreHidden). Once a
// BranchLedger is attached, every CREATE/DROP and the first gid-floor record per branch is persisted to it.
//
// The GC fork pin held for each branch is injected as callbacks (Database binds them to
// InMemoryStorage::RegisterBranchForkPin/ReleaseForkPin, the acquire also reading main's approximate counts), so tests
// need no storage engine.
//
// Structural validation only (duplicate name): callers MUST run name validation (ValidateBranchName) and
// gate/privilege checks before CreateBranch/DropBranch.
class VersionStore {
 public:
  using CoreResult = std::expected<std::shared_ptr<BranchCore>, std::string>;
  using CoreBuilder = std::function<CoreResult()>;

  // The callbacks must stay bound to the owning storage's fork-pin primitive for this VersionStore's lifetime.
  VersionStore(std::function<ForkPoint()> acquire_pin, std::function<void(uint64_t)> release_pin);

  VersionStore(const VersionStore &) = delete;
  VersionStore &operator=(const VersionStore &) = delete;
  VersionStore(VersionStore &&) = delete;
  VersionStore &operator=(VersionStore &&) = delete;
  // Withdraws this store's branches and checkouts from the process-wide active gauges.
  ~VersionStore();

  // Receives the durable timestamp branches still need recoverable (RecoverableBelow of the oldest branch), or nullopt
  // when no branch exists; invoked after Restore, a failed CREATE and a DROP. Set before use. Called with no lock of
  // this store held except the mutation serializer, so it must not call back into this store.
  void SetRetentionFloorCallback(std::function<void(std::optional<uint64_t>)> callback);

  // Starts persisting to `file`. Call before the first CreateBranch; does not read or write the file itself.
  void AttachLedger(std::filesystem::path file);

  // Startup only, before any other call. Installs `contents` without writing the ledger. Each branch's info.fork_ts is
  // its runtime fork pin, already registered with the storage by the caller (DropBranch releases it); its
  // recorded_fork_ts is the value the ledger keeps.
  void Restore(const LedgerContents &contents);

  // Startup only, instead of Restore when versioning is off. Keeps `contents` as hidden branches: not listed, checked
  // out or droppable, but still in every ledger write, in Empty() and in the retention floor. Does not write the
  // ledger.
  void RestoreHidden(const LedgerContents &contents);

  // Creates `name` forked from main. `name` must be new. On success acquires the fork pin; on failure (including a
  // ledger write failure) returns a user-facing message and changes no state (no pin held).
  std::expected<BranchInfo, std::string> CreateBranch(std::string name);

  // Drops `name` unless a session is attached to it. `own_checkout` is the number of the branch the caller itself is
  // attached to (and leaves afterwards): that one attach does not count if it is `name`'s. Releases the fork pin,
  // retires the branch's core and deletes its directory once no core holder or session of it is left. A ledger write
  // failure is reported as an error (nothing is dropped). A crash between the ledger write and the directory removal
  // leaves the directory behind; its number is never reused.
  std::expected<DropResult, std::string> DropBranch(std::string_view name,
                                                    std::optional<uint64_t> own_checkout = std::nullopt);

  // As DropBranch, but drops although sessions are attached (never kInUse). Their attaches stay valid until released
  // (keyed by branch number, so a branch re-created under the same name is unaffected) and their cores live until they
  // let go.
  std::expected<DropResult, std::string> ForceDropBranch(std::string_view name);

  // Replica demotion: refuses new branches and checkouts from here on (until Unfreeze), then durably drops every
  // branch, including those with attached sessions (their ReleaseCheckout stays valid and keeps the gauges balanced).
  // Every core is retired. Branch numbering is kept. Idempotent; a store with nothing to drop is only frozen (no
  // ledger write). Returns the dropped names, hidden included. A ledger write failure is returned, leaving the store
  // unchanged (including its frozen state).
  std::expected<std::vector<std::string>, std::string> FreezeAndDropAll();

  void Unfreeze();

  bool Frozen() const;

  std::optional<BranchInfo> Get(std::string_view name) const;

  // Persists the branch's gid floors the first time they are known; later calls and unknown `number` are no-ops.
  // Fails only on a ledger write failure.
  std::expected<void, std::string> RecordGidFloors(uint64_t number, uint64_t vertex_gid_floor, uint64_t edge_gid_floor);

  // Ordered by ascending branch number (creation order).
  std::vector<std::pair<std::string, BranchInfo>> List() const;

  bool Exists(std::string_view name) const;

  // True iff `name` is a live branch with this `number` (false after a drop, even if the name was re-created).
  bool IsLive(std::string_view name, uint64_t number) const;

  // True iff no branches exist, hidden ones included (`main` is not counted). Storage-global destructive ops (STORAGE
  // MODE, RECOVER SNAPSHOT, DROP GRAPH) are rejected while any branch exists.
  bool Empty() const;

  // Keeps CreateBranch and every drop out while held, so an Empty() store stays empty: a storage-global operation
  // needs it across its whole run. Take it before any storage lock (CreateBranch takes its fork pin under it).
  [[nodiscard]] std::unique_lock<std::mutex> LockRegistry() { return std::unique_lock{ledger_mutex_}; }

  // Attaches a session to `name`; any number may attach, all sharing the branch's BranchCore. Returns the BranchInfo
  // captured atomically with the attach (no race with drop + same-name re-create). A branch with an attached session
  // cannot be dropped without ForceDropBranch. Every successful attach MUST be paired with exactly one
  // ReleaseCheckout(info.number).
  std::expected<BranchInfo, std::string> TryAcquireCheckout(std::string_view name);

  // Releases one attach of the branch `number`, also after that branch was dropped. No-op without an attach.
  void ReleaseCheckout(uint64_t number);

  // Attaches of the live branch `name`; 0 if there is none.
  size_t CheckoutCount(std::string_view name) const;

  // Returns the live core of `number`, building it on first use; racing first checkouts of one branch wait for a single
  // build, other branches build in parallel. Caller must hold an attach, which keeps DropBranch from retiring the core
  // mid-build. Fails, keeping no core, if `number` is not a live branch (dropped, e.g. by FreezeAndDropAll, which does
  // not wait for attached sessions). Cores are held here, not in dbms::Database, so they are destroyed with the
  // VersionStore, strictly before the storage they point into.
  CoreResult GetOrBuildCore(uint64_t number, const CoreBuilder &build);

 private:
  // A branch's core: ready once its first build finished. Shared by racing first checkouts.
  using CoreSlot = std::shared_future<CoreResult>;

  struct PendingRemoval {
    std::weak_ptr<BranchCore> core;
    uint64_t number;
    std::filesystem::path directory;
  };

  // What a mutation hands over to run after the ledger is released: core teardown and directory removal.
  struct Retired {
    std::vector<std::pair<uint64_t, CoreSlot>> cores;
    std::filesystem::path branches_root;
  };

  // Lock order: ledger_mutex_ -> lock_. Nothing else is acquired under lock_, and no callback, core destruction or
  // file I/O runs under it. Readers (IsLive runs on every statement of a branch session) take only lock_, briefly. A
  // copy-on-write registry behind std::atomic<std::shared_ptr> would not drop it: that atomic is not lock-free in
  // libstdc++, and checkouts and core slots still need a mutable home.
  mutable utils::SpinLock lock_;
  std::map<std::string, BranchInfo, std::less<>> branches_;
  // Branches kept only in the ledger while versioning is off (RestoreHidden).
  std::vector<LedgerBranch> hidden_;
  // Attached sessions per branch number; entries are erased at zero.
  std::map<uint64_t, size_t> checked_out_;
  size_t total_checkouts_{0};
  uint64_t next_number_{2};
  // Branches whose DROP is writing the ledger; TryAcquireCheckout refuses them.
  std::set<std::string, std::less<>> dropping_;
  // Set by FreezeAndDropAll; CreateBranch and TryAcquireCheckout refuse.
  bool frozen_{false};
  // One slot per live branch whose core is built or being built.
  std::map<uint64_t, CoreSlot> cores_;
  // Dropped branches whose directory awaits the last core holder and attached session.
  std::vector<PendingRemoval> pending_removals_;

  std::function<ForkPoint()> acquire_pin_;
  std::function<void(uint64_t)> release_pin_;
  std::function<void(std::optional<uint64_t>)> retention_floor_callback_;

  // Serializes registry mutations so the (blocking) ledger write runs without lock_ held. Guards ledger_ and
  // branches_root_.
  std::mutex ledger_mutex_;
  std::optional<BranchLedger> ledger_;
  // Parent of the ledger file: holds one directory per branch number. Empty without a ledger.
  std::filesystem::path branches_root_;

  // Call with ledger_mutex_ held and lock_ NOT held. No-op if no ledger is attached.
  std::expected<void, std::string> WriteLedger(const LedgerContents &contents);

  std::expected<DropResult, std::string> DropBranchImpl(std::string_view name, bool force,
                                                        std::optional<uint64_t> own_checkout);
  LedgerContents SnapshotLocked() const;
  bool IsLiveNumberLocked(uint64_t number) const;
  // Moves the core slot of `number` into `retired`; it is destroyed by Finish, outside every lock.
  void RetireCoreLocked(uint64_t number, Retired &retired);
  // Takes the queued directories that no core holder or attached session keeps any more.
  std::vector<std::filesystem::path> TakeRemovableLocked();

  // Call WITHOUT lock_ held (it takes lock_ itself).
  void PublishRetentionFloor() const;

  // Destroys the retired cores, then removes each directory or queues it until its last core holder and session are
  // gone. Call without any lock held.
  void Finish(Retired retired);
  static void RemoveDirectory(const std::filesystem::path &directory);
};

// FreezeAndDropAll for an instance that is or becomes a REPLICA (a REPLICA has no branches): the one automatic drop of
// branch data. Logs the dropped branches as a warning and a failure as an error; false on failure.
bool DropBranchesOnReplica(VersionStore &store, std::string_view database, std::string_view occasion);

}  // namespace memgraph::versioning
