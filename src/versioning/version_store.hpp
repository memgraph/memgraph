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
// Restart note: this class's OWN ctor (below) never re-registers a loaded branch against the GC
// fork pin -- VersionStore is deliberately storage-engine-agnostic (see the class comment above)
// and never calls `acquire_pin_` on load, only on a fresh CreateBranch. The pin itself IS
// re-seeded across a restart, just from a different call site: `dbms::Database`'s ctor
// (database.cpp) pre-reads this same versioning/ directory BEFORE constructing storage, and
// threads every persisted branch's fork_ts into `storage::Config::Durability::
// recover_fork_timestamps` (a multiset -- one entry per branch, preserving fork_ts multiplicity
// for branches that share a fork point); `InMemoryStorage`'s recovery ctor sequence then calls
// `AddForkPinAt` once per entry (storage.cpp) -- gated only on `recover_oldest_fork_ts`, NOT on
// `recover_on_startup`, so DROP/MERGE release always finds a pin regardless of whether WAL replay
// ran -- BEFORE `gc_runner_` starts, so by the time THIS class's ctor runs (constructed after storage_, see
// dbms::Database::Database), every branch it loads already has a live, correctly-multiplied pin
// backing its fork_ts. See `recover_fork_timestamps`'s doc-comment (config.hpp) for why this must
// be a multiset, not a set.
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
  //   - `parent` must not currently be BeginMerge'd (see below) -- otherwise a branch could be
  //     forked off a parent mid-merge, only for that parent to vanish out from under it once
  //     FinishMerge completes (CHUNK 7a HIGH-2 fix).
  // On success, acquires a GC fork pin (fork_ts = acquire_pin()), assigns the next monotonic
  // number, persists the record, and returns the resulting BranchInfo. On failure, returns an
  // end-user-facing error message and leaves all state (including any pin) untouched.
  std::expected<BranchInfo, std::string> CreateBranch(std::string name, std::string parent,
                                                      std::optional<std::string> description);

  // Structural check: a branch that is the parent of another branch cannot be dropped (mirrors
  // the spec's merge/drop "no children" rule). Returns false without changing any state if `name`
  // doesn't exist, has children, or is currently BeginMerge'd (see below). On success, releases
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
  // CHUNK 7 (interpreter dispatch) needs this as a fail-fast pre-check before MERGE: DropBranch
  // already refuses internally when a branch has children, but by the time MERGE would discover
  // that via a failed DropBranch, it has ALREADY replayed the change-log onto main -- there is no
  // way back from that. Checking HasChildren before ever starting the merge avoids ever reaching
  // that state (spec's "Cannot merge/drop '<x>': it has child branches" errors are both fail-fast,
  // not post-hoc). Minimal, additive public accessor over the existing private/locked check below;
  // does not change DropBranch's own behavior or contract.
  bool HasChildren(std::string_view name) const;

  // CHUNK 7a HIGH-2 fix: atomic "begin a merge" -- the merge itself (versioning::MergeBranch)
  // takes a while (opens its own transaction against main) and runs OUTSIDE any lock this class
  // holds, so a bare "HasChildren check, then later DropBranch" pair (as chunk 7a originally did)
  // has a TOCTOU window: a concurrent CreateBranch(parent=name) can land in between, making the
  // post-merge DropBranch fail -- which would leave `name` a legal, un-merged-looking branch
  // whose change-log a SECOND `MERGE BRANCH name` would replay again, duplicating every CREATE
  // op (D3 only catches MODIFY conflicts, see merge.cpp). BeginMerge closes that window by doing
  // the existence + no-children + not-already-merging check AND recording `name` as "merging" in
  // one atomic, lock-held step; CreateBranch (above) refuses to fork off a merging parent, and a
  // second concurrent BeginMerge/DropBranch on the same name is refused too. The caller MUST
  // follow a successful BeginMerge with exactly one of FinishMerge (merge committed) or
  // AbortMerge (merge failed/rejected) -- there is no other way to clear the "merging" marker.
  //
  // Returns the branch's (structurally validated) BranchInfo -- the SAME data Get() would have
  // returned, captured atomically with the "merging" mark so the caller never needs a separate,
  // separately-racy Get() call for fork_ts/parent. Returns an end-user-facing error message
  // (mirrors CreateBranch's own contract) if `name` doesn't exist, has children, or a merge on it
  // is already in progress.
  std::expected<BranchInfo, std::string> BeginMerge(std::string_view name);

  // Completes a merge BeginMerge'd on `name`: removes the branch's registry entry and releases
  // its GC fork pin (same net effect as DropBranch), and clears the "merging" marker. Guaranteed
  // to find `name` present and childless -- BeginMerge already excluded both for the entire
  // window since it was called -- so, unlike DropBranch, this cannot fail. `name` must have a
  // BeginMerge in progress (caller contract; see BeginMerge above).
  void FinishMerge(std::string_view name);

  // Aborts a merge BeginMerge'd on `name` without touching the branch or its fork pin: clears the
  // "merging" marker so `name` becomes an ordinary, mergeable/droppable branch again. Used when
  // the merge itself failed or was rejected (e.g. a D3 conflict) -- mirrors MergeBranch's own
  // contract that a failed merge leaves main AND the branch byte-unchanged, retryable.
  void AbortMerge(std::string_view name);

  // Attaches a session to `name`: any number of sessions may be attached to one branch at once, all sharing the
  // branch's BranchCore (below). Returns the branch's BranchInfo, captured atomically with the attach so the caller
  // never races a drop + same-name re-create. Fails (no state changed) if `name` does not exist or is currently
  // BeginMerge'd. While attached, the branch cannot be dropped or merged. The caller MUST pair a successful attach
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
  // Names currently between a BeginMerge and its matching FinishMerge/AbortMerge (CHUNK 7a
  // HIGH-2 fix, see BeginMerge's own doc-comment). Never persisted -- a "merge in progress"
  // marker cannot legitimately survive a restart (the in-flight merge transaction itself
  // wouldn't have), so an empty set on load is always correct.
  std::set<std::string, std::less<>> merging_;
  // Number of sessions currently attached to each branch (see TryAcquireCheckout). Entries are erased at zero. Never
  // persisted, for the same reason `merging_` isn't.
  std::map<std::string, size_t, std::less<>> checked_out_;
  size_t total_checkouts_{0};
  uint64_t next_number_{2};  // main is the implicit, un-persisted version 1
  kvstore::KVStore storage_;
  std::function<uint64_t()> acquire_pin_;
  std::function<void(uint64_t)> release_pin_;

  // Removes the core of a dropped/merged branch. The core is moved out under cores_mutex_ and destroyed after the
  // mutex is released (its destructor tears down a whole storage engine). Must be called WITHOUT lock_ held.
  void RetireCore(uint64_t number);

  // Guards cores_ only; never held while building, pinning, waiting on a session or destroying a core, and never
  // nested with lock_.
  mutable std::mutex cores_mutex_;
  std::map<uint64_t, std::shared_ptr<BranchCore>> cores_;
};

}  // namespace memgraph::versioning
