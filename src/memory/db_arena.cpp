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

#include <algorithm>
#include <atomic>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>

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
    if (!dh->tracker->Alloc(static_cast<int64_t>(size))) {
      return nullptr;
    }
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

void LogDestructorCleanupFailure(std::string_view owner, unsigned arena_idx, std::string_view operation,
                                 std::string_view error) noexcept {
  try {
    spdlog::error("{} {}: {} failed during destructor cleanup: {}", owner, arena_idx, operation, error);
  } catch (...) {
    (void)0;  // clang-tidy
  }
}

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

  DMG_ASSERT(hooks.base_hooks == nullptr || hooks.base_hooks == base_hooks,
             "Inconsistent jemalloc base hooks across ArenaPool arenas");

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

// RAII helper to ensure arena release and hook restoration on failure.
class PendingArena {
 public:
  explicit PendingArena(unsigned arena_idx) noexcept : arena_idx_(arena_idx) {}

  ~PendingArena() noexcept {
    try {
      if (arena_idx_ == 0) return;

      if (hooks_installed_ && base_hooks_) {
        const std::string hooks_key = "arena." + std::to_string(arena_idx_) + ".extent_hooks";
        const extent_hooks_t *base = base_hooks_;
        const int err = je_mallctl(hooks_key.c_str(),
                                   nullptr,
                                   nullptr,
                                   static_cast<void *>(const_cast<extent_hooks_t **>(&base)),
                                   sizeof(extent_hooks_t *));
        if (err != 0) {
          LogDestructorCleanupFailure("PendingArena",
                                      arena_idx_,
                                      "restore hooks",
                                      fmt::format("err={} (arena leaked from GlobalArenaPool reuse)", err));
          return;
        }
      }
      GlobalArenaPool::Instance().Release(arena_idx_);
    } catch (const std::exception &e) {
      LogDestructorCleanupFailure("PendingArena", arena_idx_, "release", e.what());
    } catch (...) {
      LogDestructorCleanupFailure("PendingArena", arena_idx_, "release", "unknown exception");
    }
  }

  void MarkHooksInstalled(extent_hooks_t *base) noexcept {
    hooks_installed_ = true;
    base_hooks_ = base;
  }

  void Commit() noexcept { arena_idx_ = 0; }

 private:
  unsigned arena_idx_{0};
  extent_hooks_t *base_hooks_{nullptr};
  bool hooks_installed_{false};
};

}  // namespace

ArenaPool::ArenaPool(utils::MemoryTracker *tracker) {
  const std::lock_guard<std::mutex> lock(arena_mux_);

  first_arena_idx_ = GlobalArenaPool::Instance().Acquire();
  PendingArena pending(first_arena_idx_);

  const std::string arena_key = "arena." + std::to_string(first_arena_idx_);
  const std::string hooks_key = arena_key + ".extent_hooks";

  extent_hooks_t *base_hooks = nullptr;
  size_t hooks_sz = sizeof(extent_hooks_t *);
  if (int err = je_mallctl(hooks_key.c_str(), static_cast<void *>(&base_hooks), &hooks_sz, nullptr, 0);
      err != 0 || base_hooks == nullptr) {
    throw std::runtime_error(fmt::format("Failed to read default hooks for arena {} (err={})", first_arena_idx_, err));
  }

  InitDbArenaHooks(hooks_, tracker, base_hooks);
  const extent_hooks_t *new_hooks = &hooks_.hooks;
  if (int err = je_mallctl(hooks_key.c_str(),
                           nullptr,
                           nullptr,
                           static_cast<void *>(const_cast<extent_hooks_t **>(&new_hooks)),
                           sizeof(extent_hooks_t *));
      err != 0) {
    throw std::runtime_error(fmt::format("Failed to install hooks for arena {} (err={})", first_arena_idx_, err));
  }

  pending.MarkHooksInstalled(base_hooks);

  if (ConsumeFailureInjection(testing::ArenaPoolFailureInjection::ConstructorPublish)) {
    throw std::runtime_error("Injected ArenaPool constructor publish failure");
  }

  arenas_.push_back(first_arena_idx_);
  free_count_ = arenas_.size();
  pending.Commit();
}

ArenaPool::~ArenaPool() noexcept {
  try {
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
        spdlog::error(
            "ArenaPool {}: failed to restore default hooks (err={}); hooks_ may outlive arena", arena_idx, err);
        spdlog::error("ArenaPool {}: leaking arena from GlobalArenaPool reuse after failed hook restore", arena_idx);
        continue;
      }

      try {
        GlobalArenaPool::Instance().Release(arena_idx);
      } catch (const std::exception &e) {
        LogDestructorCleanupFailure("ArenaPool", arena_idx, "release", e.what());
      } catch (...) {
        LogDestructorCleanupFailure("ArenaPool", arena_idx, "release", "unknown exception");
      }
    }

    DestroyAllTcaches();
  } catch (const std::exception &e) {
    LogDestructorCleanupFailure("ArenaPool", first_arena_idx_, "cleanup", e.what());
  } catch (...) {
    LogDestructorCleanupFailure("ArenaPool", first_arena_idx_, "cleanup", "unknown exception");
  }
}

unsigned ArenaPool::idx() const noexcept { return first_arena_idx_; }

unsigned ArenaPool::Acquire() {
  const std::lock_guard<std::mutex> lock(arena_mux_);

  // 1. Reuse existing arena
  if (free_count_ != 0) {
    const unsigned idx = arenas_[--free_count_];
    if (idx == first_arena_idx_) {
      ++first_arena_use_count_;
    }
    return idx;
  }

  // 2. Prepare capacity before external calls
  arenas_.reserve(arenas_.size() + 1);

  // 3. Try create new arena
  unsigned new_idx = 0;
  try {
    if (ConsumeFailureInjection(testing::ArenaPoolFailureInjection::AcquireArenaCreate)) {
      throw std::runtime_error("Injected ArenaPool arena create failure");
    }
    new_idx = GlobalArenaPool::Instance().Acquire();
  } catch (...) {
    spdlog::trace("Failed to acquire arena from the global pool. Fallback to first arena...");
    ++first_arena_use_count_;
    return first_arena_idx_;
  }

  // 4. Install hooks with RAII protection
  PendingArena pending(new_idx);
  if (!InstallDbArenaHooks(new_idx, hooks_, "per-thread")) {
    spdlog::trace("Failed to install hooks on arena. Fallback to first arena...");
    ++first_arena_use_count_;
    return first_arena_idx_;  // pending dtor releases new_idx
  }

  pending.MarkHooksInstalled(hooks_.base_hooks);
  arenas_.push_back(new_idx);
  pending.Commit();
  return new_idx;
}

void ArenaPool::Release(unsigned arena_idx) {
  try {
    if (arena_idx == 0) return;
    const std::lock_guard<std::mutex> lock(arena_mux_);

    if (arena_idx == first_arena_idx_) {
      DMG_ASSERT(first_arena_use_count_ != 0, "Release: first arena not in use");
      if (--first_arena_use_count_ > 0) return;
      // If count reaches 0, fall through to move it to the free section of the vector
    }

    // Find the arena in the 'in-use' section [free_count_, size)
    for (size_t i = free_count_; i < arenas_.size(); ++i) {
      if (arenas_[i] == arena_idx) {
        std::swap(arenas_[i], arenas_[free_count_]);
        ++free_count_;
        return;
      }
    }
    DMG_ASSERT(false, "Trying to release an areana that is not under the current pool.");
  } catch (...) {
    // silently fail
  }
}

bool ArenaPool::Owns(unsigned arena_idx) const {
  if (arena_idx == 0) return false;
  const std::lock_guard<std::mutex> lock(arena_mux_);
  return std::ranges::any_of(arenas_, [arena_idx](const auto id) { return id == arena_idx; });
}

void ArenaPool::PurgeAllArenas() const {
  std::vector<unsigned> arena_indices;
  {
    const std::lock_guard<std::mutex> lock(arena_mux_);
    arena_indices = arenas_;
  }

  for (const auto arena_idx : arena_indices) {
    if (arena_idx == 0) continue;
    je_mallctl(("arena." + std::to_string(arena_idx) + ".purge").c_str(), nullptr, nullptr, nullptr, 0);
  }
}

unsigned ArenaPool::AcquireTcache() {
  {
    const std::lock_guard<std::mutex> lock(tcache_mutex_);
    if (!tcaches_.empty()) {
      const unsigned tcache_id = tcaches_.back();
      tcaches_.pop_back();
      return tcache_id;
    }
  }

  unsigned tcache_id = 0;
  size_t sz = sizeof(unsigned);
  if (je_mallctl("tcache.create", &tcache_id, &sz, nullptr, 0) != 0) {
    return 0;
  }
  return tcache_id;
}

void ArenaPool::ReleaseTcache(unsigned tcache_id) {
  try {
    if (tcache_id == 0) return;
    const std::lock_guard<std::mutex> lock(tcache_mutex_);
    tcaches_.push_back(tcache_id);
  } catch (...) {
    // silently fail
  }
}

void ArenaPool::DestroyAllTcaches() {
  const std::lock_guard<std::mutex> lock(tcache_mutex_);
  for (const unsigned tcache_id : tcaches_) {
    size_t sz = sizeof(unsigned);
    je_mallctl("tcache.destroy", nullptr, nullptr, const_cast<unsigned *>(&tcache_id), sz);
  }
  tcaches_.clear();
}

}  // namespace memgraph::memory

#endif  // USE_JEMALLOC

namespace memgraph::memory {

#if !USE_JEMALLOC

ArenaPool::ArenaPool(utils::MemoryTracker * /*tracker*/) {}

ArenaPool::~ArenaPool() noexcept = default;

unsigned ArenaPool::idx() const noexcept { return 0U; }

unsigned ArenaPool::Acquire() { return 0U; }

void ArenaPool::Release(unsigned /*arena_idx*/) {}

bool ArenaPool::Owns(unsigned /*arena_idx*/) const { return false; }

void ArenaPool::PurgeAllArenas() const {}

unsigned ArenaPool::AcquireTcache() { return 0U; }

void ArenaPool::ReleaseTcache(unsigned /*tcache_id*/) {}

void ArenaPool::DestroyAllTcaches() {}

#endif

}  // namespace memgraph::memory

#if USE_JEMALLOC
void *JeNew(size_t size, int flags);
void JeFree(void *ptr, std::size_t size, int flags) noexcept;
#endif

namespace memgraph::memory {

void *DbAllocateBytes(std::size_t bytes, unsigned idx, std::size_t alignment) {
#if USE_JEMALLOC
  if (idx != 0) {
    int flags = MALLOCX_ARENA(idx);
    if (tls_db_arena_state.tcache != 0) {
      flags |= MALLOCX_TCACHE(tls_db_arena_state.tcache);
    } else {
      flags |= MALLOCX_TCACHE_NONE;
    }
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
  int flags = 0;
  if (tls_db_arena_state.tcache != 0) {
    flags |= MALLOCX_TCACHE(tls_db_arena_state.tcache);
  } else {
    flags |= MALLOCX_TCACHE_NONE;
  }
  if (alignment > alignof(std::max_align_t)) {
    flags |= MALLOCX_ALIGN(alignment);
  }
  ::JeFree(p, bytes, flags);
#else
  ::operator delete(p, bytes, std::align_val_t{alignment});
#endif
}

DbArenaScope::DbArenaScope(ArenaPool *arena_pool, DbArenaScope::Type type) : prev_state_(tls_db_arena_state) {
  cur_state_.arena_pool = arena_pool;
#if USE_JEMALLOC

  // Fast path: nested scope — same pool already active in TLS.
  if (prev_state_.arena_pool == arena_pool) {
    if (prev_state_.arena != 0) {  // Borrow
      cur_state_.arena = prev_state_.arena;
      cur_state_.tcache = prev_state_.tcache;
    } else {  // Acquire from pool
      cur_state_.arena = arena_pool ? arena_pool->Acquire() : 0U;
      cur_state_.tcache = arena_pool ? arena_pool->AcquireTcache() : 0U;
      tls_db_arena_state.arena = cur_state_.arena;
      tls_db_arena_state.tcache = cur_state_.tcache;
    }
    return;
  }

  DMG_ASSERT(type == Type::FORCE || prev_state_.arena_pool == nullptr, "Crashing into a different DB arena pool!");

  // Acquire from pool (first time or different pool).
  cur_state_.arena = arena_pool ? arena_pool->Acquire() : 0U;
  cur_state_.tcache = arena_pool ? arena_pool->AcquireTcache() : 0U;
  tls_db_arena_state = cur_state_;
#else
  (void)arena_pool;
  tls_db_arena_state.arena = 0U;
  tls_db_arena_state.arena_pool = nullptr;
  DMG_ASSERT(prev_state_.arena == 0U, "Erasing DB scope!");
#endif
}

DbArenaScope::~DbArenaScope() noexcept {
  bool const borrowed = prev_state_.arena_pool == cur_state_.arena_pool && prev_state_.arena != 0;

  tls_db_arena_state = prev_state_;

  if (!borrowed) {
    if (cur_state_.arena_pool && cur_state_.arena != 0) {
      cur_state_.arena_pool->Release(cur_state_.arena);
    }
    if (cur_state_.arena_pool && cur_state_.tcache != 0) {
      cur_state_.arena_pool->ReleaseTcache(cur_state_.tcache);
    }
  }
}

}  // namespace memgraph::memory
