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

#include "db_arena.hpp"

#include <stdexcept>

#include "utils/logging.hpp"

#if USE_JEMALLOC

namespace memgraph::memory {

namespace {

// ---------------------------------------------------------------------------
// Extent hook callbacks for per-DB arenas.
// Each callback receives back the exact `extent_hooks_t *` pointer that was
// installed via mallctl, which is `&DbArenaHooks::hooks` (the first field of
// the `DbArenaHooks` struct). We reinterpret-cast it to `DbArenaHooks *` to
// retrieve the tracker and the default (base) hooks to call through.
// ---------------------------------------------------------------------------

void *db_arena_alloc(extent_hooks_t *hooks, void *new_addr, size_t size, size_t alignment, bool *zero, bool *commit,
                     unsigned arena_ind) {
  auto *dh = reinterpret_cast<DbArenaHooks *>(hooks);
  const bool requested_commit = *commit;
  // Pre-track if commit was requested (mandatory — base hook must return committed or fail).
  if (requested_commit) {
    if (!dh->tracker->Alloc(static_cast<int64_t>(size))) return nullptr;
  }
  void *ptr = dh->base_hooks->alloc(dh->base_hooks, new_addr, size, alignment, zero, commit, arena_ind);
  if (ptr == nullptr) {
    if (requested_commit) dh->tracker->Free(static_cast<int64_t>(size));
    return nullptr;
  }
  // *commit is an out-parameter: the base hook may have committed pages even if we didn't ask.
  if (*commit && !requested_commit) {
    const utils::MemoryTracker::OutOfMemoryExceptionBlocker blocker;
    dh->tracker->Alloc(static_cast<int64_t>(size));
  }
  return ptr;
}

bool db_arena_dalloc(extent_hooks_t *hooks, void *addr, size_t size, bool committed, unsigned arena_ind) {
  auto *dh = reinterpret_cast<DbArenaHooks *>(hooks);
  const bool err = dh->base_hooks->dalloc(dh->base_hooks, addr, size, committed, arena_ind);
  if (!err && committed) {
    dh->tracker->Free(static_cast<int64_t>(size));
  }
  return err;
}

void db_arena_destroy(extent_hooks_t *hooks, void *addr, size_t size, bool committed, unsigned arena_ind) {
  auto *dh = reinterpret_cast<DbArenaHooks *>(hooks);
  if (committed) {
    dh->tracker->Free(static_cast<int64_t>(size));
  }
  dh->base_hooks->destroy(dh->base_hooks, addr, size, committed, arena_ind);
}

bool db_arena_commit(extent_hooks_t *hooks, void *addr, size_t size, size_t offset, size_t length, unsigned arena_ind) {
  auto *dh = reinterpret_cast<DbArenaHooks *>(hooks);
  const bool err = dh->base_hooks->commit(dh->base_hooks, addr, size, offset, length, arena_ind);
  if (!err) {
    // Pages are already committed by the OS — we cannot undo the commit here.
    // OutOfMemoryExceptionBlocker makes MemoryTrackerCanThrow() return false on this thread,
    // which means Alloc() will never enter its rollback path: the fetch_add is permanent and
    // the entire tracker chain returns true unconditionally. Tracking is therefore guaranteed.
    const utils::MemoryTracker::OutOfMemoryExceptionBlocker blocker;
    dh->tracker->Alloc(static_cast<int64_t>(length));
  }
  return err;
}

bool db_arena_decommit(extent_hooks_t *hooks, void *addr, size_t size, size_t offset, size_t length,
                       unsigned arena_ind) {
  auto *dh = reinterpret_cast<DbArenaHooks *>(hooks);
  const bool err = dh->base_hooks->decommit(dh->base_hooks, addr, size, offset, length, arena_ind);
  if (!err) {
    dh->tracker->Free(static_cast<int64_t>(length));
  }
  return err;
}

bool db_arena_purge_forced(extent_hooks_t *hooks, void *addr, size_t size, size_t offset, size_t length,
                           unsigned arena_ind) {
  auto *dh = reinterpret_cast<DbArenaHooks *>(hooks);
  if (dh->base_hooks->purge_forced == nullptr) return true;
  const bool err = dh->base_hooks->purge_forced(dh->base_hooks, addr, size, offset, length, arena_ind);
  if (!err) {
    dh->tracker->Free(static_cast<int64_t>(length));
  }
  return err;
}

}  // namespace

void InitDbArenaHooks(DbArenaHooks &h, utils::MemoryTracker *tracker, extent_hooks_t *base_hooks) {
  h.tracker = tracker;
  h.base_hooks = base_hooks;
  h.hooks = extent_hooks_t{
      .alloc = &db_arena_alloc,
      .dalloc = &db_arena_dalloc,
      .destroy = &db_arena_destroy,
      .commit = &db_arena_commit,
      .decommit = &db_arena_decommit,
      .purge_lazy = base_hooks->purge_lazy,  // pass-through; no tracking needed
      .purge_forced = &db_arena_purge_forced,
      .split = base_hooks->split,
      .merge = base_hooks->merge,
  };
}

DbArena::DbArena(utils::MemoryTracker *tracker) {
  // Create the first arena for backwards compatibility
  arena_handles_.emplace_back(ArenaPool::Instance().Acquire());
  first_arena_idx_ = arena_handles_.back().idx();
  const unsigned arena_idx = first_arena_idx_;

  const std::string arena_key = "arena." + std::to_string(arena_idx);

  // Read the default (or previously-restored) hooks on the arena so we can call through.
  const std::string hooks_key = arena_key + ".extent_hooks";
  extent_hooks_t *base_hooks = nullptr;
  size_t hooks_sz = sizeof(extent_hooks_t *);
  int err = je_mallctl(hooks_key.c_str(), static_cast<void *>(&base_hooks), &hooks_sz, nullptr, 0);
  if (err != 0 || base_hooks == nullptr) {
    throw std::runtime_error(fmt::format("Failed to read default hooks for DB arena {} (err={})", arena_idx, err));
  }

  // Populate and install our custom hooks.
  InitDbArenaHooks(hooks_, tracker, base_hooks);
  const extent_hooks_t *new_hooks = &hooks_.hooks;
  err = je_mallctl(hooks_key.c_str(),
                   nullptr,
                   nullptr,
                   static_cast<void *>(const_cast<extent_hooks_t **>(&new_hooks)),
                   sizeof(extent_hooks_t *));
  if (err != 0) {
    throw std::runtime_error(fmt::format("Failed to install custom hooks on DB arena {} (err={})", arena_idx, err));
  }
}

DbArena::~DbArena() {
  // Purge all per-thread arenas and restore their hooks
  for (auto &handle : arena_handles_) {
    if (!handle) continue;
    const unsigned arena_idx = handle.idx();
    const std::string arena_key = "arena." + std::to_string(arena_idx);

    // Purge all dirty/muzzy pages back to the OS FIRST, while our custom hooks are
    // still installed.
    if (int perr = je_mallctl((arena_key + ".purge").c_str(), nullptr, nullptr, nullptr, 0); perr != 0) {
      spdlog::error("DbArena {}: purge failed (err={}); MemoryTracker may drift before hook restore", arena_idx, perr);
    }

    // Restore the default hooks AFTER purging
    const extent_hooks_t *base = hooks_.base_hooks;
    int err = je_mallctl((arena_key + ".extent_hooks").c_str(),
                         nullptr,
                         nullptr,
                         static_cast<void *>(const_cast<extent_hooks_t **>(&base)),
                         sizeof(extent_hooks_t *));
    if (err != 0) {
      spdlog::error("DbArena {}: failed to restore default hooks (err={}); hooks_ may outlive arena", arena_idx, err);
    }
  }
  // arena_handles_ destructor will release indices to pool
}

unsigned DbArena::idx() const noexcept { return first_arena_idx_; }

unsigned DbArena::AcquireThreadArena() {
  const auto tid = std::this_thread::get_id();

  // Fast path: check if already in map
  {
    std::lock_guard<std::mutex> lock(arena_map_mux_);
    auto it = thread_arena_map_.find(tid);
    if (it != thread_arena_map_.end()) {
      return it->second;
    }
  }

  // Cold path: create new arena for this thread
  arena_handles_.emplace_back(ArenaPool::Instance().Acquire());
  unsigned arena_idx = arena_handles_.back().idx();

  // Install hooks on the new arena
  const std::string arena_key = "arena." + std::to_string(arena_idx);
  const std::string hooks_key = arena_key + ".extent_hooks";

  extent_hooks_t *base_hooks = nullptr;
  size_t hooks_sz = sizeof(extent_hooks_t *);
  int err = je_mallctl(hooks_key.c_str(), static_cast<void *>(&base_hooks), &hooks_sz, nullptr, 0);
  if (err != 0 || base_hooks == nullptr) {
    spdlog::error("Failed to read default hooks for per-thread arena {} (err={})", arena_idx, err);
    // Return the first arena as fallback
    return first_arena_idx_;
  }

  // Install hooks (reuse the same hooks_ struct - all arenas report to same tracker)
  const extent_hooks_t *new_hooks = &hooks_.hooks;
  err = je_mallctl(hooks_key.c_str(),
                   nullptr,
                   nullptr,
                   static_cast<void *>(const_cast<extent_hooks_t **>(&new_hooks)),
                   sizeof(extent_hooks_t *));
  if (err != 0) {
    spdlog::error("Failed to install custom hooks on per-thread arena {} (err={})", arena_idx, err);
    return first_arena_idx_;
  }

  // Insert into map
  {
    std::lock_guard<std::mutex> lock(arena_map_mux_);
    thread_arena_map_[tid] = arena_idx;
  }

  return arena_idx;
}

// DbArenaScope implementation (legacy constructor only - Database* constructor in database.cpp)
DbArenaScope::DbArenaScope(unsigned arena_idx) noexcept : prev_arena_(tls_db_arena_state.arena) {
  tls_db_arena_state.arena = arena_idx;
}

DbArenaScope::~DbArenaScope() noexcept {
  // Restore previous arena
  tls_db_arena_state.arena = prev_arena_;
}

}  // namespace memgraph::memory

#endif  // USE_JEMALLOC
