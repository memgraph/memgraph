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
#include <shared_mutex>
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

// A registered branch: its metadata, fork pin and lazily built core. Owned by the VersionStore; sessions reach it
// through a BranchCheckout. Destroying it releases the pin and, if the branch was dropped, removes its directory.
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

  // The runtime MVCC pin registered in this process; drives visibility and GC. Equals info().fork.durable_fork_ts only
  // for a branch created in this process.
  uint64_t fork_ts() const { return fork_ts_; }

  // Why the branch's files could not be recovered at startup; runtime only. Set only by startup recovery (main
  // recovered short of the fork, or the branch's WAL failed to replay) and never persisted. Such a branch is kept as it
  // is (pin, retention, ledger entry) but cannot be used; DROP BRANCH removes it.
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
  // Checkouts held. Changed under VersionStore::mutex_ (shared); compared against zero under it exclusively.
  std::atomic<size_t> sessions_{0};
  std::mutex build_mutex_;
  std::shared_ptr<BranchCore> core_;
};

class VersionStore;

// A session's hold on a branch: the branch stays alive, even if force-dropped, until the handle is destroyed.
class BranchCheckout {
 public:
  BranchCheckout() = default;

  BranchCheckout(const BranchCheckout &) = delete;
  BranchCheckout &operator=(const BranchCheckout &) = delete;

  BranchCheckout(BranchCheckout &&other) noexcept
      : store_{std::exchange(other.store_, nullptr)}, branch_{std::exchange(other.branch_, nullptr)} {}

  BranchCheckout &operator=(BranchCheckout &&other) noexcept {
    if (this != &other) {
      reset();
      store_ = std::exchange(other.store_, nullptr);
      branch_ = std::exchange(other.branch_, nullptr);
    }
    return *this;
  }

  ~BranchCheckout() { reset(); }

  // Gives the branch back; the handle is empty afterwards.
  void reset();

  Branch *get() const { return branch_; }

  Branch *operator->() const { return branch_; }

  Branch &operator*() const { return *branch_; }

  explicit operator bool() const { return branch_ != nullptr; }

 private:
  friend class VersionStore;

  BranchCheckout(VersionStore *store, Branch *branch) : store_{store}, branch_{branch} {}

  VersionStore *store_{nullptr};
  Branch *branch_{nullptr};
};

// Per-Database registry of branches, persisted to a BranchLedger. Each branch's GC fork pin and the retention floor are
// injected as callbacks (Database binds InMemoryStorage::RegisterBranchForkPin/ReleaseForkPin/SetRetentionFloor) so
// tests need no storage engine.
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

  // A branch loaded from the ledger with the runtime pin the caller registered for it on the rebuilt history.
  struct RestoredBranch {
    LedgerBranch branch;
    uint64_t runtime_pin;
  };

  // All arguments are required. The callbacks must stay bound to the owning storage for this VersionStore's lifetime.
  // `set_retention_floor` receives the durable timestamp branches still need recoverable (kNoRetentionFloor when no
  // branch exists) after Restore, a failed CREATE and a DROP. It runs under the store's mutex: no callback may call
  // back in. `branches_root` holds one directory per branch id.
  VersionStore(std::function<ForkPoint()> acquire_pin, std::function<void(uint64_t)> release_pin,
               std::function<void(uint64_t)> set_retention_floor, BranchLedger ledger,
               std::filesystem::path branches_root);

  VersionStore(const VersionStore &) = delete;
  VersionStore &operator=(const VersionStore &) = delete;
  VersionStore(VersionStore &&) = delete;
  VersionStore &operator=(VersionStore &&) = delete;
  // Withdraws this store's branches from the process-wide active gauge. All checkouts must be released.
  ~VersionStore();

  // Startup only, before any other call. Installs the branches without writing the ledger. The store owns each
  // `runtime_pin` from here (released when the branch is gone).
  void Restore(uint64_t next_branch_id, std::span<const RestoredBranch> branches);

  // Startup only, after Restore. Sets the branch `id` aside: it stays registered (pin, retention, ledger entry) but
  // cannot be checked out, with `reason` shown to its user; DROP BRANCH removes it. Unknown `id`: no-op.
  void MarkUnavailable(uint64_t id, std::string reason);

  // Creates `name` forked from main. `name` must be new. Replaces any directory left under the new id with one holding
  // an empty `wal/`. On success holds the fork pin; on failure (including a directory or ledger write failure) returns
  // a user-facing message and changes no state (no pin held).
  std::expected<BranchInfo, std::string> CreateBranch(std::string name);

  // Drops `name`: kInUse while a session holds it unless `force`. Forced, sessions keep working on the dropped branch
  // until they let go. The pin and directory go with the last checkout. A ledger write failure drops nothing; a crash
  // before directory removal leaves it behind, its id never reused.
  std::expected<DropResult, std::string> DropBranch(std::string_view name, bool force = false);

  std::optional<Listing> Get(std::string_view name) const;

  // Ordered by ascending branch id (creation order).
  std::vector<Listing> List() const;

  bool Exists(std::string_view name) const;

  // True iff no branches exist (`main` is not counted). Gates storage-global destructive ops.
  bool Empty() const;

  // Holds the registry exclusively, no branch can be created, dropped or checked out meanwhile, iff no branches exist;
  // otherwise an unowned lock. Do not call Empty() on this thread while it is held.
  std::unique_lock<std::shared_mutex> LockIfEmpty();

  // Attaches a session to `name`; any number may. The session leaves by destroying the handle, which must happen before
  // the store is destroyed. Refuses unknown and unavailable branches.
  std::expected<BranchCheckout, std::string> Checkout(std::string_view name);

 private:
  friend class BranchCheckout;

  using BranchMap = std::map<std::string, Branch, std::less<>>;
  using BranchNode = BranchMap::node_type;

  // Internal helpers: call with mutex_ held.
  // Writes the registry as the maps now hold it. Any failure, including a throw, is returned as an error so the caller
  // can revert its map change.
  std::expected<void, std::string> PersistLedger_();
  LedgerContents CreateSnapshot_() const;
  void PublishRetentionFloorLocked() const;
  static Listing DescribeLocked(const std::string &name, const Branch &branch);
  // BranchCheckout's release. A dropped branch is destroyed here once its last checkout is gone.
  void Release(Branch *branch);

  std::function<ForkPoint()> acquire_pin_;
  std::function<void(uint64_t)> release_pin_;
  std::function<void(uint64_t)> set_retention_floor_;
  BranchLedger ledger_;
  // One directory per branch id.
  std::filesystem::path branches_root_;

  // The only lock; held across the ledger write and the callbacks, never across Branch destruction. Shared for
  // checkout/release/reads, exclusive for anything that changes the registry.
  mutable std::shared_mutex mutex_;
  BranchMap branches_;
  // Force-dropped branches that still have checkouts. Extracted nodes keep their address, so checkouts stay valid.
  std::vector<BranchNode> dropped_;
  uint64_t next_branch_id_{2};
};

// Drops every branch of `store`, for an instance that is or becomes a REPLICA (a REPLICA has no branches): the one
// automatic drop of branches. Forced, so attached sessions are not waited for. False (logged) if any drop failed; the
// branches that could be dropped are gone.
bool DropBranchesOnReplica(VersionStore &store, std::string_view database, std::string_view occasion);

}  // namespace memgraph::versioning
