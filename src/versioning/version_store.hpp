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
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "kvstore/kvstore.hpp"
#include "utils/spin_lock.hpp"

namespace memgraph::versioning {

class BranchCore;

// Shared with dbms::Database's restart pre-read (database.cpp), which must accept/reject the
// SAME persisted branch records this class's own ctor (version_store.cpp) would -- see the
// restart note below. Keeping a single definition here (rather than one private copy per
// translation unit) is a correctness requirement, not just deduplication: if the two ever drifted
// independently, database.cpp's pre-read could accept (or reject) a record VersionStore itself
// disagrees on, under/over-seeding the GC fork-pin count and eventually tripping
// InMemoryStorage::ReleaseForkPin's MG_ASSERT.
//
// When the format of the persisted branch record changes, bump this (mirrors query::TriggerStore's
// kVersion, src/query/trigger.cpp). Only one version exists today; this is the dispatch point a
// future format bump would branch on (see TriggerStore::RestoreTrigger / MigrateTriggerData for
// the analogous pattern).
inline constexpr uint64_t kVersion{1};

// Reserved kvstore key for the never-reused branch-number counter. Spec S4.1 forbids branch names
// from starting with '.', so this can never collide with an actual branch record.
inline constexpr std::string_view kNextNumberKey{".next_number"};

// A single entry in the branch registry. `number` is a monotonically increasing, never-reused
// identifier (main is the implicit, un-persisted version 1; the first created branch is 2, and so
// on even across drops). `fork_ts` is the storage-engine timestamp the branch was forked at --
// owned by the GC fork-pin mechanism (chunk 2A's InMemoryStorage::RegisterForkPin/ReleaseForkPin).
struct BranchInfo {
  uint64_t number;
  std::string parent;
  uint64_t fork_ts;
  std::optional<std::string> description;
};

// Per-Database durable registry of branches ("versions"), mirroring query::TriggerStore's
// kvstore-backed persistence pattern (src/query/trigger.hpp).
//
// Deliberately decoupled from storage::Storage: the GC fork-pin that must be held for the
// lifetime of a branch is injected as two callbacks (`acquire_pin`/`release_pin`) rather than a
// Storage*/InMemoryStorage*, so this class -- and its unit tests -- never need a real storage
// engine instance. The Database wires these callbacks to InMemoryStorage::RegisterForkPin /
// ::ReleaseForkPin (chunk 2A) when it constructs a VersionStore for an IN_MEMORY_TRANSACTIONAL db.
//
// STRUCTURAL validation only: unknown parent, duplicate name, drop-with-children. Full semantic
// name validation (versioning::ValidateBranchName, src/versioning/name.hpp) belongs to the
// grammar/AST layer (chunk 1); full end-to-end validation (gate checks, privileges, etc.) is
// chunk 7. Callers MUST run those checks before invoking CreateBranch/DropBranch here.
//
// Restart note: this class's ctor never re-registers a loaded branch against the GC fork pin. Branches are
// ephemeral, so `dbms::Database`'s ctor (database.cpp) deletes the whole versioning/ directory before storage and this
// store are constructed, and no fork pin is ever seeded across a restart.
//
// Numbering note: the never-reused branch-number counter is persisted independently of any single
// branch's record (reserved kvstore key ".next_number", see version_store.cpp) precisely because
// DropBranch deletes the dropped branch's own record outright -- deriving the counter purely from
// surviving records would let a dropped branch's number be handed out again after a restart.
class VersionStore {
 public:
  // Opens (or creates) the durable kvstore at `directory` and loads any previously persisted
  // branches into memory (see restart note above). `acquire_pin`/`release_pin` must be bound to
  // the owning storage engine's fork-pin primitive for the lifetime of this VersionStore.
  VersionStore(std::filesystem::path directory, std::function<uint64_t()> acquire_pin,
               std::function<void(uint64_t)> release_pin);

  VersionStore(const VersionStore &) = delete;
  VersionStore &operator=(const VersionStore &) = delete;
  VersionStore(VersionStore &&) = delete;
  VersionStore &operator=(VersionStore &&) = delete;
  ~VersionStore() = default;

  // Structural checks only:
  //   - `name` must not already be a registered branch.
  //   - `parent` must be "main" or an existing branch.
  // On success, acquires a GC fork pin (fork_ts = acquire_pin()), assigns the next monotonic
  // number, persists the record, and returns the resulting BranchInfo. On failure, returns an
  // end-user-facing error message and leaves all state (including any pin) untouched.
  std::expected<BranchInfo, std::string> CreateBranch(std::string name, std::string parent,
                                                      std::optional<std::string> description);

  // Structural check: a branch that is the parent of another branch cannot be dropped (mirrors
  // the spec's merge/drop "no children" rule). Returns false without changing any state if `name`
  // doesn't exist, or has children. On success, releases
  // the GC fork pin, removes the durable record, and returns true.
  bool DropBranch(std::string_view name);

  std::optional<BranchInfo> Get(std::string_view name) const;

  // All registered branches, ordered by ascending branch number (i.e. creation order).
  std::vector<std::pair<std::string, BranchInfo>> List() const;

  bool Exists(std::string_view name) const;

  // True iff no branches are currently registered (the un-persisted implicit `main` is never
  // counted). Used by the R17 mode-guards (interpreter.cpp) to reject storage-global-destructive
  // ops -- STORAGE MODE / RECOVER SNAPSHOT / DROP GRAPH -- whenever any branch exists, since those
  // would strand every branch's fork-point base.
  bool Empty() const;

  // Whether `name` currently has any child branch (some other branch's `parent == name`).
  // Used by DROP BRANCH as a fail-fast pre-check; DropBranch itself also refuses a branch with children.
  bool HasChildren(std::string_view name) const;

  // Attaches a session to `name`: any number of sessions may be attached to one branch at once, all sharing the
  // branch's BranchCore (below). Returns the branch's BranchInfo, captured atomically with the attach so the caller
  // never races a drop + same-name re-create. Fails (no state changed) if `name` does not exist.
  // While attached, the branch cannot be dropped. The caller MUST pair a successful attach
  // with exactly one ReleaseCheckout -- CurrentDB::ClearBranchContext (query/interpreter.hpp) is the sole caller of
  // ReleaseCheckout. Never persisted: attachments are live-session state and do not survive a restart.
  std::expected<BranchInfo, std::string> TryAcquireCheckout(std::string_view name);

  // Detaches one session previously attached via TryAcquireCheckout. Idempotent no-op if `name` has no attached
  // session (defensive; should not happen given the caller contract above).
  void ReleaseCheckout(std::string_view name);

  // Number of sessions currently attached to `name` (0 if none or unknown).
  size_t CheckoutCount(std::string_view name) const;

  // Shared per-branch core registry (one live BranchCore per branch number, never persisted). Lives here, not in
  // dbms::Database, so it is destroyed with the VersionStore -- strictly before the storage the cores point into.
  using CoreBuilder = std::function<std::expected<std::shared_ptr<BranchCore>, std::string>()>;

  // Returns the live core of branch `number`, building it with `build` on first use. `build` runs WITHOUT the registry
  // mutex held (two racing first checkouts may both build; the loser is discarded). The caller must hold an attach
  // (TryAcquireCheckout), which is what keeps DropBranch from retiring the core mid-build.
  std::expected<std::shared_ptr<BranchCore>, std::string> GetOrBuildCore(uint64_t number, const CoreBuilder &build);

 private:
  // Caller must already hold lock_.
  bool HasChildrenLocked(std::string_view name) const;

  mutable utils::SpinLock lock_;
  std::map<std::string, BranchInfo, std::less<>> branches_;
  // Number of sessions currently attached to each branch (see TryAcquireCheckout). Entries are erased at zero. Never
  // persisted: attachments are live-session state.
  std::map<std::string, size_t, std::less<>> checked_out_;
  size_t total_checkouts_{0};
  uint64_t next_number_{2};  // main is the implicit, un-persisted version 1
  kvstore::KVStore storage_;
  std::function<uint64_t()> acquire_pin_;
  std::function<void(uint64_t)> release_pin_;

  // Removes the core of a dropped branch. The core is moved out under cores_mutex_ and destroyed after the
  // mutex is released (its destructor tears down a whole storage engine). Must be called WITHOUT lock_ held.
  void RetireCore(uint64_t number);

  // Guards cores_ only; never held while building, pinning, waiting on a session or destroying a core, and never
  // nested with lock_.
  mutable std::mutex cores_mutex_;
  std::map<uint64_t, std::shared_ptr<BranchCore>> cores_;
};

}  // namespace memgraph::versioning
