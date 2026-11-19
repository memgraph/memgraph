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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "versioning/branch_info.hpp"
#include "versioning/branch_ledger.hpp"

namespace memgraph::versioning {

class BranchCore;

using CoreResult = std::expected<std::shared_ptr<BranchCore>, std::string>;
using CoreBuilder = std::function<CoreResult()>;

enum class DropResult : uint8_t { kDropped, kNotFound, kInUse };

// A registered branch: its metadata, fork pin and lazily built core. Shared between the store and checked-out sessions;
// the last holder releases the pin and, if the branch was dropped, removes its directory. Sessions must let go before
// the storage the pin callback is bound to dies.
class Branch {
 public:
  // `fork_ts` is the live MVCC fork pin, released on destruction.
  Branch(std::string name, BranchInfo info, uint64_t fork_ts, std::filesystem::path directory,
         std::function<void(uint64_t)> release_pin);

  Branch(const Branch &) = delete;
  Branch &operator=(const Branch &) = delete;
  Branch(Branch &&) = delete;
  Branch &operator=(Branch &&) = delete;
  ~Branch();

  const std::string &name() const { return name_; }

  const BranchInfo &info() const { return info_; }

  // This process's MVCC fork pin, held until the branch is gone; drives visibility and GC.
  uint64_t fork_ts() const { return fork_ts_; }

  // Why the branch's files could not be recovered at startup; runtime only. Such a branch is kept as it is (pin,
  // retention, ledger entry) but cannot be used; DROP BRANCH removes it.
  const std::optional<std::string> &unavailable_reason() const { return unavailable_reason_; }

  bool dropped() const { return dropped_.load(std::memory_order_acquire); }

  // Returns the core, building it on first use. Concurrent first callers share one build; a failed build is not cached.
  CoreResult GetOrBuildCore(const CoreBuilder &build);

  // Startup only, before the branch is shared.
  void SetUnavailable(std::string reason) { unavailable_reason_ = std::move(reason); }

 private:
  friend class VersionStore;

  std::string name_;
  BranchInfo info_;
  uint64_t fork_ts_;
  std::optional<std::string> unavailable_reason_;
  std::filesystem::path directory_;
  std::function<void(uint64_t)> release_pin_;
  std::atomic<bool> dropped_{false};
  std::mutex build_mutex_;
  std::shared_ptr<BranchCore> core_;
};

// Per-Database registry of branches, persisted to an attached BranchLedger. Each branch's GC fork pin and the retention
// floor are injected as callbacks (Database binds InMemoryStorage::RegisterBranchForkPin/ReleaseForkPin/
// SetRetentionFloor) so tests need no storage engine.
// Structural validation only (duplicate name): callers MUST run ValidateBranchName and gate/privilege checks first.
class VersionStore {
 public:
  struct Listing {
    std::string name;
    BranchInfo info;
    uint64_t fork_ts;
    std::optional<std::string> unavailable_reason;
    // Checkouts currently held.
    size_t sessions;
  };

  // The callbacks must stay bound to the owning storage for this VersionStore's lifetime. `set_retention_floor`
  // receives the durable timestamp branches still need recoverable (kNoRetentionFloor when no branch exists) after
  // Restore, a failed CREATE and a DROP; empty means nobody listens. It runs under the store's mutex: no callback may
  // call back in.
  VersionStore(std::function<ForkPoint()> acquire_pin, std::function<void(uint64_t)> release_pin,
               std::function<void(uint64_t)> set_retention_floor = {});

  VersionStore(const VersionStore &) = delete;
  VersionStore &operator=(const VersionStore &) = delete;
  VersionStore(VersionStore &&) = delete;
  VersionStore &operator=(VersionStore &&) = delete;
  // Withdraws this store's branches from the process-wide active gauge.
  ~VersionStore();

  // Starts persisting to `ledger`; `branches_root` holds one directory per branch id. Call before Restore and
  // CreateBranch; does not read or write the ledger itself.
  void AttachLedger(BranchLedger ledger, std::filesystem::path branches_root);

  // Startup only, before any other call but AttachLedger. Installs `contents` without writing the ledger.
  // `fork_ts[i]` is the runtime fork pin of contents.branches[i], already registered by the caller on the rebuilt
  // history; the store owns it from here (released when the branch is gone). Same size as contents.branches.
  void Restore(const LedgerContents &contents, std::span<const uint64_t> fork_ts);

  // Startup only, after Restore. Sets the branch `id` aside: it stays registered (pin, retention, ledger entry) but
  // cannot be checked out, with `reason` shown to its user; DROP BRANCH removes it. Unknown `id`: no-op.
  void MarkUnavailable(uint64_t id, std::string reason);

  // Creates `name` forked from main. `name` must be new. On success holds the fork pin; on failure (including a ledger
  // write failure) returns a user-facing message and changes no state (no pin held).
  std::expected<BranchInfo, std::string> CreateBranch(std::string name);

  // Drops `name`: kInUse while a session holds it unless `force`. Forced, sessions keep working on the dropped branch
  // until they let go. The pin and directory go with the last holder. A ledger write failure drops nothing; a crash
  // before directory removal leaves it behind, its id never reused.
  std::expected<DropResult, std::string> DropBranch(std::string_view name, bool force = false);

  std::optional<Listing> Get(std::string_view name) const;

  // Ordered by ascending branch id (creation order).
  std::vector<Listing> List() const;

  bool Exists(std::string_view name) const;

  // True iff no branches exist (`main` is not counted). Gates storage-global destructive ops.
  bool Empty() const;

  // Attaches a session to `name`; any number may. The session leaves by dropping the returned pointer. Refuses unknown
  // and unavailable branches.
  std::expected<std::shared_ptr<Branch>, std::string> Checkout(std::string_view name);

 private:
  // Guarded by mutex_. Returns the error, or nothing once durable. No-op without a ledger.
  std::expected<void, std::string> WriteLedger(const LedgerContents &contents);
  LedgerContents SnapshotLocked() const;
  void PublishRetentionFloorLocked() const;
  static Listing DescribeLocked(const std::string &name, const std::shared_ptr<Branch> &branch);

  std::function<ForkPoint()> acquire_pin_;
  std::function<void(uint64_t)> release_pin_;
  std::function<void(uint64_t)> set_retention_floor_;

  // The only lock; held across the ledger write and the callbacks, never across Branch destruction.
  mutable std::mutex mutex_;
  std::map<std::string, std::shared_ptr<Branch>, std::less<>> branches_;
  uint64_t next_branch_id_{2};
  std::optional<BranchLedger> ledger_;
  // One directory per branch id. Empty without a ledger.
  std::filesystem::path branches_root_;
};

}  // namespace memgraph::versioning
