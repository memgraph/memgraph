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

#include <atomic>
#include <stdexcept>

#include "utils/logging.hpp"

#if USE_JEMALLOC

namespace memgraph::memory {

namespace testing {

namespace {
std::atomic<ArenaPoolFailureInjection> arena_pool_failure_injection{ArenaPoolFailureInjection::None};
}

void SetArenaPoolFailureInjection(ArenaPoolFailureInjection failure) { arena_pool_failure_injection.store(failure); }

}  // namespace testing

namespace {

bool ConsumeFailureInjection(testing::ArenaPoolFailureInjection failure) {
  auto expected = failure;
  return testing::arena_pool_failure_injection.compare_exchange_strong(expected,
                                                                       testing::ArenaPoolFailureInjection::None);
}

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

namespace {

bool InstallDbArenaHooks(unsigned arena_idx, DbArenaHooks &hooks, std::string_view error_context) {
  const std::string arena_key = "arena." + std::to_string(arena_idx);
  const std::string hooks_key = arena_key + ".extent_hooks";

  extent_hooks_t *base_hooks = nullptr;
  size_t hooks_sz = sizeof(extent_hooks_t *);
  int err = je_mallctl(hooks_key.c_str(), static_cast<void *>(&base_hooks), &hooks_sz, nullptr, 0);
  if (err != 0 || base_hooks == nullptr) {
    spdlog::error("Failed to read default hooks for {} arena {} (err={})", error_context, arena_idx, err);
    return false;
  }

  const extent_hooks_t *new_hooks = &hooks.hooks;
  err = je_mallctl(hooks_key.c_str(),
                   nullptr,
                   nullptr,
                   static_cast<void *>(const_cast<extent_hooks_t **>(&new_hooks)),
                   sizeof(extent_hooks_t *));
  if (err != 0) {
    spdlog::error("Failed to install custom hooks on {} arena {} (err={})", error_context, arena_idx, err);
    return false;
  }

  return true;
}

class PendingArena {
 public:
  explicit PendingArena(unsigned arena_idx) noexcept : arena_idx_(arena_idx) {}

  ~PendingArena() {
    if (arena_idx_ == 0) return;

    if (hooks_installed_) {
      const std::string hooks_key = "arena." + std::to_string(arena_idx_) + ".extent_hooks";
      const extent_hooks_t *base = base_hooks_;
      const int err = je_mallctl(hooks_key.c_str(),
                                 nullptr,
                                 nullptr,
                                 static_cast<void *>(const_cast<extent_hooks_t **>(&base)),
                                 sizeof(extent_hooks_t *));
      if (err != 0) {
        spdlog::error(
            "ArenaPool {}: failed to restore default hooks during construction cleanup (err={})", arena_idx_, err);
      }
    }

    GlobalArenaPool::Instance().Release(arena_idx_);
  }

  PendingArena(const PendingArena &) = delete;
  PendingArena &operator=(const PendingArena &) = delete;
  PendingArena(PendingArena &&) = delete;
  PendingArena &operator=(PendingArena &&) = delete;

  void MarkHooksInstalled() noexcept { hooks_installed_ = true; }

  void SetBaseHooks(extent_hooks_t *base_hooks) noexcept { base_hooks_ = base_hooks; }

  void Commit() noexcept { arena_idx_ = 0; }

 private:
  unsigned arena_idx_{0};
  extent_hooks_t *base_hooks_{nullptr};
  bool hooks_installed_{false};
};

}  // namespace

ArenaPool::ArenaPool(utils::MemoryTracker *tracker) {
  // Create the first arena for backwards compatibility
  first_arena_idx_ = GlobalArenaPool::Instance().Acquire();
  const unsigned arena_idx = first_arena_idx_;
  PendingArena pending_arena{arena_idx};

  const std::string arena_key = "arena." + std::to_string(arena_idx);

  // Read the default (or previously-restored) hooks on the arena so we can call through.
  const std::string hooks_key = arena_key + ".extent_hooks";
  extent_hooks_t *base_hooks = nullptr;
  size_t hooks_sz = sizeof(extent_hooks_t *);
  int err = je_mallctl(hooks_key.c_str(), static_cast<void *>(&base_hooks), &hooks_sz, nullptr, 0);
  if (err != 0 || base_hooks == nullptr) {
    throw std::runtime_error(fmt::format("Failed to read default hooks for DB arena {} (err={})", arena_idx, err));
  }
  pending_arena.SetBaseHooks(base_hooks);

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
  pending_arena.MarkHooksInstalled();

  if (ConsumeFailureInjection(testing::ArenaPoolFailureInjection::ConstructorPublish)) {
    throw std::runtime_error("Injected ArenaPool constructor publish failure");
  }

  arenas_.push_back(arena_idx);
  free_count_ = arenas_.size();
  pending_arena.Commit();
}

ArenaPool::~ArenaPool() {
  DMG_ASSERT(free_count_ == arenas_.size(), "Destroying DB ArenaPool while some arenas are still in use");
  DMG_ASSERT(first_arena_use_count_ == 0, "Destroying DB ArenaPool while the first arena is still in use");

  for (const auto arena_idx : arenas_) {
    const std::string arena_key = "arena." + std::to_string(arena_idx);

    // Purge all dirty/muzzy pages back to the OS FIRST, while our custom hooks are
    // still installed.
    if (int perr = je_mallctl((arena_key + ".purge").c_str(), nullptr, nullptr, nullptr, 0); perr != 0) {
      spdlog::error(
          "ArenaPool {}: purge failed (err={}); MemoryTracker may drift before hook restore", arena_idx, perr);
    }

    // Restore the default hooks AFTER purging
    const extent_hooks_t *base = hooks_.base_hooks;
    int err = je_mallctl((arena_key + ".extent_hooks").c_str(),
                         nullptr,
                         nullptr,
                         static_cast<void *>(const_cast<extent_hooks_t **>(&base)),
                         sizeof(extent_hooks_t *));
    if (err != 0) {
      spdlog::error("ArenaPool {}: failed to restore default hooks (err={}); hooks_ may outlive arena", arena_idx, err);
    }

    GlobalArenaPool::Instance().Release(arena_idx);
  }
}

unsigned ArenaPool::idx() const noexcept { return first_arena_idx_; }

unsigned ArenaPool::Acquire() {
  const std::lock_guard<std::mutex> lock(arena_mux_);

  if (free_count_ != 0) {
    --free_count_;
    if (arenas_[free_count_] == first_arena_idx_) {
      ++first_arena_use_count_;
    }
    return arenas_[free_count_];
  }

  // Reserve before hook installation so publishing the installed arena cannot
  // throw and return a hooked arena to the reusable pool without restoration.
  arenas_.reserve(arenas_.size() + 1);

  unsigned arena_idx = 0;
  try {
    if (ConsumeFailureInjection(testing::ArenaPoolFailureInjection::AcquireArenaCreate)) {
      throw std::runtime_error("Injected ArenaPool arena create failure");
    }
    arena_idx = GlobalArenaPool::Instance().Acquire();
  } catch (const std::exception &e) {
    spdlog::error("ArenaPool: failed to acquire per-thread DB arena ({}); falling back to base arena {}",
                  e.what(),
                  first_arena_idx_);
    ++first_arena_use_count_;
    return first_arena_idx_;
  }

  if (!InstallDbArenaHooks(arena_idx, hooks_, "per-thread DB")) {
    GlobalArenaPool::Instance().Release(arena_idx);
    ++first_arena_use_count_;
    return first_arena_idx_;
  }

  arenas_.push_back(arena_idx);
  return arena_idx;
}

void ArenaPool::Release(unsigned arena_idx) {
  if (arena_idx == 0) return;
  const std::lock_guard<std::mutex> lock(arena_mux_);
  if (arena_idx == first_arena_idx_) {
    if (first_arena_use_count_ == 0) {
      DMG_ASSERT(false, "Attempting to release the first arena when it is not in use");
      return;
    }
    --first_arena_use_count_;
    if (first_arena_use_count_ != 0) return;
  }
  for (auto i = free_count_; i < arenas_.size(); ++i) {
    if (arenas_[i] == arena_idx) {
      std::swap(arenas_[i], arenas_[free_count_]);
      ++free_count_;
      return;
    }
  }
  // Only the first arena can be reused by multiple threads
  DMG_ASSERT(arena_idx == first_arena_idx_, "Attempting to release an arena that is not in use by this ArenaPool");
}

bool ArenaPool::Owns(unsigned arena_idx) const {
  if (arena_idx == 0) return false;
  const std::lock_guard<std::mutex> lock(arena_mux_);
  for (const auto owned_arena_idx : arenas_) {
    if (owned_arena_idx == arena_idx) return true;
  }
  return false;
}

}  // namespace memgraph::memory

#endif  // USE_JEMALLOC

#if USE_JEMALLOC
void *JeNew(size_t size, int flags);
void JeFree(void *ptr, std::size_t size, int flags) noexcept;
#endif

namespace memgraph::memory {

unsigned ArenaPoolBaseIdx(ArenaPool *arena) noexcept {
#if USE_JEMALLOC
  return arena ? arena->idx() : 0;
#else
  (void)arena;
  return 0;
#endif
}

#if USE_JEMALLOC
#if defined(DEBUG_ARENA_VERIFICATION)
unsigned GetPointerArena(void *ptr) {
  if (!ptr) return 0;
  unsigned arena_idx = 0;
  size_t len = sizeof(arena_idx);
  je_mallctl("arenas.lookup", &arena_idx, &len, &ptr, sizeof(ptr));
  return arena_idx;
}
#endif
#endif

void *DbAllocateBytes(std::size_t bytes, unsigned idx, std::size_t alignment) {
#if USE_JEMALLOC
  if (idx != 0) {
    int flags = MALLOCX_ARENA(idx) | MALLOCX_TCACHE_NONE;
    if (alignment > alignof(std::max_align_t)) {
      flags |= MALLOCX_ALIGN(alignment);
    }
    return ::JeNew(bytes, flags);
  }
#endif
  return ::operator new(bytes, std::align_val_t{alignment});
}

void DbDeallocateBytes(void *p, std::size_t bytes, std::size_t alignment) noexcept {
#if USE_JEMALLOC
  int flags = MALLOCX_TCACHE_NONE;
  if (alignment > alignof(std::max_align_t)) {
    flags |= MALLOCX_ALIGN(alignment);
  }
  ::JeFree(p, bytes, flags);
#else
  ::operator delete(p, bytes, std::align_val_t{alignment});
#endif
}

DbArenaScope::DbArenaScope() noexcept : prev_arena_(tls_db_arena_state.arena) {
  prev_arena_pool_ = tls_db_arena_state.arena_pool;
  tls_db_arena_state.arena = 0U;
  tls_db_arena_state.arena_pool = nullptr;
  DMG_ASSERT(prev_arena_ == 0U, "Erasing DB scope!");
}

DbArenaScope::DbArenaScope(ArenaPool *arena_pool, DbArenaScope::Type type)
    : arena_pool_(arena_pool), prev_arena_pool_(tls_db_arena_state.arena_pool), prev_arena_(tls_db_arena_state.arena) {
#if USE_JEMALLOC
  if (prev_arena_pool_ == arena_pool_) {
    arena_idx_ = prev_arena_;
    arena_pool_ = nullptr;  // Disable release, we are borrowing the arena from the same pool
    return;
  }
  DMG_ASSERT(type != Type::FORCE && prev_arena_pool_ == nullptr, "Crashing into a different DB arena pool!");
  arena_idx_ = arena_pool_ ? arena_pool_->Acquire() : 0U;
  tls_db_arena_state.arena = arena_idx_;
  tls_db_arena_state.arena_pool = arena_pool_;
#else
  (void)arena_pool;
  tls_db_arena_state.arena = 0U;
  tls_db_arena_state.arena_pool = nullptr;
  DMG_ASSERT(prev_arena_ == 0U, "Erasing DB scope!");
#endif
}

DbArenaScope::~DbArenaScope() noexcept {
  // Restore previous arena
  tls_db_arena_state.arena = prev_arena_;
  tls_db_arena_state.arena_pool = prev_arena_pool_;
#if USE_JEMALLOC
  if (arena_pool_) {
    arena_pool_->Release(arena_idx_);
  }
#endif
}

}  // namespace memgraph::memory
