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
  // On success, acquires a GC fork pin (fork_ts = acquire_pin()), assigns the next monotonic
  // number, persists the record, and returns the resulting BranchInfo. On failure, returns an
  // end-user-facing error message and leaves all state (including any pin) untouched.
  std::expected<BranchInfo, std::string> CreateBranch(std::string name, std::string parent,
                                                      std::optional<std::string> description);

  // Structural check: a branch that is the parent of another branch cannot be dropped (mirrors
  // the spec's merge/drop "no children" rule). Returns false without changing any state if `name`
  // doesn't exist or has children. On success, releases the GC fork pin, removes the durable
  // record, and returns true.
  bool DropBranch(std::string_view name);

  std::optional<BranchInfo> Get(std::string_view name) const;

  // All registered branches, ordered by ascending branch number (i.e. creation order).
  std::vector<std::pair<std::string, BranchInfo>> List() const;

  bool Exists(std::string_view name) const;

 private:
  // Caller must already hold lock_.
  bool HasChildren(std::string_view name) const;

  mutable utils::SpinLock lock_;
  std::map<std::string, BranchInfo, std::less<>> branches_;
  uint64_t next_number_{2};  // main is the implicit, un-persisted version 1
  kvstore::KVStore storage_;
  std::function<uint64_t()> acquire_pin_;
  std::function<void(uint64_t)> release_pin_;
};

}  // namespace memgraph::versioning
