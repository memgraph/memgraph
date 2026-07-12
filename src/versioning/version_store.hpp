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

#include <cstdint>
#include <expected>
#include <filesystem>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "kvstore/kvstore.hpp"
#include "utils/spin_lock.hpp"

namespace memgraph::versioning {

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
// Restart note: on load, previously-persisted branches are NOT re-registered against the GC fork
// pin -- the pin is purely in-memory storage-engine state (InMemoryStorage::version_fork_pins_)
// that does not survive a restart, and chunk 9a (bounded recover-to-fork_ts) has not landed yet.
// A branch reloaded from disk therefore carries its historical fork_ts but holds no live pin, so
// main's GC is free to have reclaimed history back past fork_ts across the restart. This is a
// known, deliberate limitation of this chunk, not a bug.
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

  // Graph Versioning v1 (lazy diff-context, slice E-1): exclusive single-writer checkout. A
  // checked-out branch is served by a private per-session BranchContext (see
  // versioning/branch_engine.hpp) -- its diff engine is where COW'd/branch-native writes land --
  // if two sessions checked out the same branch concurrently, each would hold a DIFFERENT diff
  // engine and their writes would silently diverge with no way to reconcile. TryAcquireCheckout
  // enforces "at most one live checkout per branch" the exact same way `merging_` enforces "at
  // most one live merge per branch" (see BeginMerge above): a bare membership set guarded by
  // lock_, never persisted (purely live-session state; does not survive a restart, exactly like
  // merging_).
  //
  // Returns false (no state changed) if `name` is already checked out by some session, or is
  // currently BeginMerge'd (a session should not be able to start writing to a branch that is
  // mid-merge onto its parent). The caller MUST pair a successful TryAcquireCheckout with exactly
  // one later ReleaseCheckout -- CurrentDB::ClearBranchContext (query/interpreter.hpp) is the sole
  // caller of ReleaseCheckout, invoked whenever the session's branch pointer is cleared or moves
  // to a different branch/database.
  bool TryAcquireCheckout(std::string_view name);

  // Releases a checkout previously acquired via TryAcquireCheckout. Idempotent no-op if `name`
  // isn't currently checked out (defensive; should not happen given the caller contract above).
  void ReleaseCheckout(std::string_view name);

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
  // Names currently checked out by some live session's BranchContext (see TryAcquireCheckout's own
  // doc-comment above). Never persisted, for the same reason `merging_` isn't: an exclusive
  // checkout is a live-session concept that cannot survive a restart.
  std::set<std::string, std::less<>> checked_out_;
  uint64_t next_number_{2};  // main is the implicit, un-persisted version 1
  kvstore::KVStore storage_;
  std::function<uint64_t()> acquire_pin_;
  std::function<void(uint64_t)> release_pin_;
};

}  // namespace memgraph::versioning
