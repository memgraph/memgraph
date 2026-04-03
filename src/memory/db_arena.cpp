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
  if (*commit) {
    if (!dh->tracker->Alloc(static_cast<int64_t>(size))) return nullptr;
  }
  void *ptr = dh->base_hooks->alloc(dh->base_hooks, new_addr, size, alignment, zero, commit, arena_ind);
  if (ptr == nullptr && *commit) {
    dh->tracker->Free(static_cast<int64_t>(size));
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
  // Acquire an arena index — reuse a recycled one if available, otherwise create fresh.
  arena_idx_ = ArenaPool::Instance().Acquire();

  // New arenas inherit opt.dirty_decay_ms / opt.muzzy_decay_ms from the global jemalloc
  // configuration (set via MALLOC_CONF at startup). No need to override here — recycled
  // arenas retain the same values since we never modify decay on release.
  const std::string arena_key = "arena." + std::to_string(arena_idx_);

  // Read the default (or previously-restored) hooks on the arena so we can call through.
  const std::string hooks_key = arena_key + ".extent_hooks";
  extent_hooks_t *base_hooks = nullptr;
  size_t hooks_sz = sizeof(extent_hooks_t *);
  int err = je_mallctl(hooks_key.c_str(), static_cast<void *>(&base_hooks), &hooks_sz, nullptr, 0);
  MG_ASSERT(
      err == 0 && base_hooks != nullptr, "Failed to read default hooks for DB arena {} (err={})", arena_idx_, err);

  // Populate and install our custom hooks.
  InitDbArenaHooks(hooks_, tracker, base_hooks);
  const extent_hooks_t *new_hooks = &hooks_.hooks;
  err = je_mallctl(hooks_key.c_str(),
                   nullptr,
                   nullptr,
                   static_cast<void *>(const_cast<extent_hooks_t **>(&new_hooks)),
                   sizeof(extent_hooks_t *));
  MG_ASSERT(err == 0, "Failed to install custom hooks on DB arena {} (err={})", arena_idx_, err);
}

DbArena::~DbArena() {
  if (arena_idx_ == 0) return;
  const std::string arena_key = "arena." + std::to_string(arena_idx_);

  // Purge all dirty/muzzy pages back to the OS FIRST, while our custom hooks are
  // still installed. This ensures dalloc/decommit callbacks fire through our tracker
  // so the DB and global MemoryTrackers are correctly decremented before we unlink them.
  // Important: there is no "tracker handoff" after this point. DB-owned allocations are
  // expected to be destroyed before ~DbArena runs, and the DB MemoryTrackers outlive this
  // purge so any final hook callbacks still propagate into the DB/global hierarchy here.
  // Note: we intentionally skip arena.N.destroy — jemalloc's arena destruction is
  // experimental and arena indices are explicitly recycled via ArenaPool instead.
  je_mallctl((arena_key + ".purge").c_str(), nullptr, nullptr, nullptr, 0);

  // Restore the default hooks AFTER purging so jemalloc's background thread cannot
  // call our hooks after `hooks_` is destroyed (use-after-free / null-tracker SIGSEGV).
  // mallctl atomically swaps the hooks pointer; once it returns no new invocations
  // of db_arena_alloc/dalloc/... will be dispatched for this arena.
  const extent_hooks_t *base = hooks_.base_hooks;
  je_mallctl((arena_key + ".extent_hooks").c_str(),
             nullptr,
             nullptr,
             static_cast<void *>(const_cast<extent_hooks_t **>(&base)),
             sizeof(extent_hooks_t *));

  // Return the index to the pool so the next DbArena can reuse it instead of
  // creating a new one. Hooks are already restored so the arena is safe to reuse.
  ArenaPool::Instance().Release(arena_idx_);
}

}  // namespace memgraph::memory

#endif  // USE_JEMALLOC
