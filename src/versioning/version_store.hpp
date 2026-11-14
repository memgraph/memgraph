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

// Gids a branch may take natively are reserved in the ledger in blocks of this many, vertices and edges apiece.
inline constexpr uint64_t kGidReserveChunk = 1U << 20;

enum class DropResult : uint8_t { kDropped, kNotFound, kInUse };

// Per-Database registry of branches, persisted to an attached BranchLedger. Each branch's GC fork pin is injected as
// callbacks (Database binds InMemoryStorage::RegisterBranchForkPin/ReleaseForkPin) so tests need no storage engine.
// Structural validation only (duplicate name): callers MUST run ValidateBranchName and gate/privilege checks first.
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

  // Receives the durable timestamp branches still need recoverable (nullopt when no branch exists) after Restore, a
  // failed CREATE and a DROP. Set before use. Runs with only the mutation serializer held: must not call back in.
  void SetRetentionFloorCallback(std::function<void(std::optional<uint64_t>)> callback);

  // Starts persisting to `file`. Call before the first CreateBranch; does not read or write the file itself.
  void AttachLedger(std::filesystem::path file);

  // Startup only, before any other call. Installs `contents` without writing the ledger. info.fork_ts is the runtime
  // fork pin, already registered by the caller (DropBranch releases it); recorded_fork_ts is what the ledger keeps.
  void Restore(const LedgerContents &contents);

  // Startup only, instead of Restore when versioning is off. Keeps `contents` as hidden branches: not listed, checked
  // out or droppable, but still in ledger writes, Empty() and the retention floor. Writes nothing.
  void RestoreHidden(const LedgerContents &contents);

  // Startup only, after Restore. Sets the branch `number` aside: it stays registered (pin, retention, ledger entry) but
  // cannot be checked out, with `reason` shown to its user; DROP BRANCH removes it. Unknown `number`: no-op.
  void MarkUnavailable(uint64_t number, std::string reason);

  // Startup only, instead of Restore when the ledger could not be read: create, drop and checkout all fail with
  // `message`, and Empty() is false so storage-global operations stay refused. The ledger is never replaced.
  void RestoreLedgerUnreadable(std::string message);

  std::optional<std::string> LedgerUnreadable() const;

  // Creates `name` forked from main. `name` must be new. On success acquires the fork pin; on failure (including a
  // ledger write failure) returns a user-facing message and changes no state (no pin held).
  std::expected<BranchInfo, std::string> CreateBranch(std::string name);

  // Drops `name` unless a session is attached; `own_checkout` is the caller's own attached branch number, which does
  // not count (it leaves afterwards). Releases the fork pin; the directory goes once no core holder or session is
  // left. A ledger write failure drops nothing; a crash before directory removal leaves it behind, its number never
  // reused.
  std::expected<DropResult, std::string> DropBranch(std::string_view name,
                                                    std::optional<uint64_t> own_checkout = std::nullopt);

  // As DropBranch, but drops although sessions are attached (never kInUse). Their attaches stay valid until released
  // (keyed by branch number, so a branch re-created under the same name is unaffected) and their cores live until they
  // let go.
  std::expected<DropResult, std::string> ForceDropBranch(std::string_view name);

  // Replica demotion: refuses new branches and checkouts until Unfreeze, and durably drops every branch, hidden and
  // attached included (ReleaseCheckout stays valid). Idempotent; with nothing to drop it only freezes (no ledger
  // write). Returns the dropped names; a ledger write failure leaves the store unchanged, frozen state included.
  std::expected<std::vector<std::string>, std::string> FreezeAndDropAll();

  void Unfreeze();

  bool Frozen() const;

  std::optional<BranchInfo> Get(std::string_view name) const;

  // Persists the branch's gid floors the first time they are known; later calls and unknown `number` are no-ops.
  // Fails only on a ledger write failure.
  std::expected<void, std::string> RecordGidFloors(uint64_t number, uint64_t vertex_gid_floor, uint64_t edge_gid_floor);

  // Makes every native gid of `kind` below `through` durably the branch's: raises its ledger ceiling to `through` +
  // kGidReserveChunk (fsynced) unless it already covers it. Returns the ceiling in force; a gid is usable only below
  // it. A dropped branch returns `through`. Fails only on a ledger write failure, leaving the ceiling as it was.
  std::expected<uint64_t, std::string> ReserveGids(uint64_t number, GidKind kind, uint64_t through);

  // ReserveGids for branch `number`, for BranchCore::SetGidReserver. Valid as long as this store is.
  GidReserver GidReserverOf(uint64_t number);

  // Ordered by ascending branch number (creation order).
  std::vector<std::pair<std::string, BranchInfo>> List() const;

  bool Exists(std::string_view name) const;

  // True iff `name` is a live branch with this `number` (false after a drop, even if the name was re-created).
  bool IsLive(std::string_view name, uint64_t number) const;

  // True iff no branches exist, hidden ones included (`main` is not counted). Gates storage-global destructive ops.
  bool Empty() const;

  // Attaches a session to `name` (any number may, sharing the branch's BranchCore); returns the BranchInfo captured
  // atomically with the attach, so a drop + same-name re-create cannot interleave. Every success MUST be paired with
  // exactly one ReleaseCheckout(info.number).
  std::expected<BranchInfo, std::string> TryAcquireCheckout(std::string_view name);

  // Releases one attach of the branch `number`, also after that branch was dropped. No-op without an attach.
  void ReleaseCheckout(uint64_t number);

  size_t CheckoutCount(std::string_view name) const;

  // Returns the live core of `number`, building it on first use: racing first checkouts share one build, other
  // branches build in parallel. Caller must hold an attach, which keeps DropBranch from retiring the core mid-build.
  // Fails if `number` is not live (e.g. FreezeAndDropAll does not wait for sessions). Cores live here, not in
  // dbms::Database, so they die with the VersionStore, before the storage they point into.
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
  // file I/O runs under it. Readers (IsLive, per statement of a branch session) take only lock_, briefly. A
  // std::atomic<std::shared_ptr> registry would not drop it: not lock-free in libstdc++, and checkouts need a home.
  mutable utils::SpinLock lock_;
  std::map<std::string, BranchInfo, std::less<>> branches_;
  // Branches kept only in the ledger while versioning is off (RestoreHidden).
  std::vector<LedgerBranch> hidden_;
  // Set by RestoreLedgerUnreadable: the registry is unknown and every branch operation fails with this message.
  std::optional<std::string> ledger_unreadable_;
  // Attached sessions per branch number; entries are erased at zero.
  std::map<uint64_t, size_t> checked_out_;
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

  // Serializes registry mutations so the (blocking) ledger write runs without lock_ held. Guards ledger_;
  // branches_root_ is written under this and lock_, so either one reads it.
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

  // Call without lock_ held: it computes the floor under lock_ and applies it after releasing it, so callers must
  // hold ledger_mutex_ (or be single-threaded startup) to keep two publishes from applying out of order.
  void PublishRetentionFloor() const;

  // Destroys the retired cores, then removes each directory or queues it until its last core holder and session are
  // gone. Call without any lock held.
  void Finish(Retired retired);
  static void RemoveDirectory(const std::filesystem::path &directory);
};

}  // namespace memgraph::versioning
