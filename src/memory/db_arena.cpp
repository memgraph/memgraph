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

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <exception>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "utils/logging.hpp"

#if USE_JEMALLOC

namespace memgraph::memory {

namespace testing {

namespace {
// Thread-local so a test's injection is consumed only by the thread that armed it.
// A live Database has background threads (storage GC, async indexer) that also
// construct/Acquire/destroy per-DB arenas; with a process-global flag one of them
// could steal the single-shot injection between a test's Set and its own Acquire,
// making the fallback-arena assertions in the Database-backed ArenaPool tests
// nondeterministic (fallback returned a freshly-created arena instead of the first).
// Being thread-local, this is only ever set and consumed on the same thread, so a
// plain value (no atomic) is sufficient.
thread_local ArenaPoolFailureInjection arena_pool_failure_injection{ArenaPoolFailureInjection::None};
}  // namespace

void SetArenaPoolFailureInjection(ArenaPoolFailureInjection failure) { arena_pool_failure_injection = failure; }

}  // namespace testing

namespace {

bool ConsumeFailureInjection(testing::ArenaPoolFailureInjection failure) {
  if (testing::arena_pool_failure_injection != failure) return false;
  testing::arena_pool_failure_injection = testing::ArenaPoolFailureInjection::None;
  return true;
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
  // null only if hooks_ was leaked on the hook-restore-failure path; skip tracking, still proxy to base hooks.
  auto *tracker = dh->tracker.load(std::memory_order_relaxed);
  const bool requested_commit = *commit;
  // Pre-track if commit was requested (mandatory — base hook must return committed or fail).
  if (requested_commit) {
    if (tracker && !tracker->Alloc(static_cast<int64_t>(size))) {
      return nullptr;
    }
  }
  void *ptr = dh->base_hooks->alloc(dh->base_hooks, new_addr, size, alignment, zero, commit, arena_ind);
  if (ptr == nullptr) {
    if (requested_commit && tracker) tracker->Free(static_cast<int64_t>(size));
    return nullptr;
  }
  // *commit is an out-parameter: the base hook may have committed pages even if we didn't ask.
  if (*commit && !requested_commit) {
    if (tracker) {
      const utils::MemoryTracker::OutOfMemoryExceptionBlocker blocker;
      tracker->Alloc(static_cast<int64_t>(size));
    }
  }
  return ptr;
}

bool db_arena_dalloc(extent_hooks_t *hooks, void *addr, size_t size, bool committed, unsigned arena_ind) {
  auto *dh = reinterpret_cast<DbArenaHooks *>(hooks);
  auto *tracker = dh->tracker.load(std::memory_order_relaxed);
  const bool err = dh->base_hooks->dalloc(dh->base_hooks, addr, size, committed, arena_ind);
  if (!err && committed) {
    if (tracker) tracker->Free(static_cast<int64_t>(size));
  }
  return err;
}

void db_arena_destroy(extent_hooks_t *hooks, void *addr, size_t size, bool committed, unsigned arena_ind) {
  auto *dh = reinterpret_cast<DbArenaHooks *>(hooks);
  auto *tracker = dh->tracker.load(std::memory_order_relaxed);
  if (committed) {
    if (tracker) tracker->Free(static_cast<int64_t>(size));
  }
  dh->base_hooks->destroy(dh->base_hooks, addr, size, committed, arena_ind);
}

bool db_arena_commit(extent_hooks_t *hooks, void *addr, size_t size, size_t offset, size_t length, unsigned arena_ind) {
  auto *dh = reinterpret_cast<DbArenaHooks *>(hooks);
  auto *tracker = dh->tracker.load(std::memory_order_relaxed);
  const bool err = dh->base_hooks->commit(dh->base_hooks, addr, size, offset, length, arena_ind);
  if (!err) {
    if (tracker) {
      // Pages are already committed by the OS — we cannot undo the commit here.
      // OutOfMemoryExceptionBlocker makes MemoryTrackerCanThrow() return false on this thread,
      // which means Alloc() will never enter its rollback path: the fetch_add is permanent and
      // the entire tracker chain returns true unconditionally. Tracking is therefore guaranteed.
      const utils::MemoryTracker::OutOfMemoryExceptionBlocker blocker;
      tracker->Alloc(static_cast<int64_t>(length));
    }
  }
  return err;
}

bool db_arena_decommit(extent_hooks_t *hooks, void *addr, size_t size, size_t offset, size_t length,
                       unsigned arena_ind) {
  auto *dh = reinterpret_cast<DbArenaHooks *>(hooks);
  auto *tracker = dh->tracker.load(std::memory_order_relaxed);
  const bool err = dh->base_hooks->decommit(dh->base_hooks, addr, size, offset, length, arena_ind);
  if (!err) {
    if (tracker) tracker->Free(static_cast<int64_t>(length));
  }
  return err;
}

bool db_arena_purge_forced(extent_hooks_t *hooks, void *addr, size_t size, size_t offset, size_t length,
                           unsigned arena_ind) {
  auto *dh = reinterpret_cast<DbArenaHooks *>(hooks);
  auto *tracker = dh->tracker.load(std::memory_order_relaxed);
  if (dh->base_hooks->purge_forced == nullptr) return true;
  const bool err = dh->base_hooks->purge_forced(dh->base_hooks, addr, size, offset, length, arena_ind);
  if (!err) {
    if (tracker) tracker->Free(static_cast<int64_t>(length));
  }
  return err;
}

}  // namespace

const std::vector<unsigned> &EnsureCpuArenaCoverage() {
  // Magic static: thread-safe; the first caller does the work, later callers get the cached result.
  static const std::vector<unsigned> coverage = [] {
    std::vector<unsigned> created;

    // Only relevant under percpu_arena: without it arena_choose() never ranges
    // past the automatic arenas, so no CPU id can select a per-DB arena.
    const char *percpu_mode = nullptr;
    size_t sz = sizeof(percpu_mode);
    if (je_mallctl("opt.percpu_arena", static_cast<void *>(&percpu_mode), &sz, nullptr, 0) != 0 ||
        percpu_mode == nullptr || std::string_view{percpu_mode} == "disabled") {
      return created;
    }

    // Possible (boot-time configured) CPUs bound every id sched_getcpu() can
    // return, including CPUs that are currently offline or may be hotplugged.
    const long n_conf = sysconf(_SC_NPROCESSORS_CONF);
    if (n_conf <= 0) {
      spdlog::warn("CPU arena coverage: sysconf(_SC_NPROCESSORS_CONF) failed; skipping");
      return created;
    }
    const auto max_cpu_id = static_cast<unsigned>(n_conf) - 1;

    unsigned narenas = 0;
    sz = sizeof(narenas);
    if (je_mallctl("arenas.narenas", &narenas, &sz, nullptr, 0) != 0) {
      spdlog::warn("CPU arena coverage: failed to read arenas.narenas; skipping");
      return created;
    }

    // arenas.create appends at the previous total, so indices come back
    // strictly increasing; stop once the index space covers max_cpu_id (or on
    // error, e.g. at MALLOCX_ARENA_LIMIT).
    while (narenas <= max_cpu_id) {
      unsigned idx = 0;
      size_t isz = sizeof(idx);
      if (const int err = je_mallctl("arenas.create", &idx, &isz, nullptr, 0); err != 0) {
        spdlog::error("CPU arena coverage: arenas.create failed (err={}) at {} arenas; CPU ids {}..{} stay uncovered",
                      err,
                      narenas,
                      narenas,
                      max_cpu_id);
        break;
      }
      created.push_back(idx);
      narenas = idx + 1;
    }

    if (!created.empty()) {
      spdlog::info(
          "Created {} sacrificial jemalloc arenas ({}..{}) so threads on CPU ids up to {} can never bind a per-DB "
          "arena (online CPU count {} < possible CPU count {})",
          created.size(),
          created.front(),
          created.back(),
          max_cpu_id,
          sysconf(_SC_NPROCESSORS_ONLN),
          n_conf);
    }
    return created;
  }();
  return coverage;
}

void InitDbArenaHooks(DbArenaHooks &h, utils::MemoryTracker *tracker, extent_hooks_t *base_hooks) {
  // Publish ordering comes from jemalloc's RELEASE store of extent_hooks at mallctl install time.
  h.tracker.store(tracker, std::memory_order_relaxed);
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

template <typename... Args>
void SafeLog(fmt::format_string<Args...> fmt, Args &&...args) noexcept {
  try {
    spdlog::error(fmt, std::forward<Args>(args)...);
  } catch (...) {
    (void)0;  // clang-tidy
  }
}

void LogDestructorCleanupFailure(std::string_view owner, unsigned arena_idx, std::string_view operation,
                                 std::string_view error) noexcept {
  SafeLog("{} {}: {} failed during destructor cleanup: {}", owner, arena_idx, operation, error);
}

bool InstallDbArenaHooks(unsigned arena_idx, DbArenaHooks &hooks, std::string_view error_context) {
  const std::string hooks_key = fmt::format("arena.{}.extent_hooks", arena_idx);

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

// RAII guard: returns the arena index to the GlobalArenaPool unless Commit()ed.
// Note this guard never needs to restore extent hooks: both call sites are
// structured so that no statement between a successful hook install and
// Commit() can throw (vector capacity is reserved up front), and a failed
// install mallctl never publishes the hooks to jemalloc in the first place.
// INVARIANT for future edits: do NOT insert any allocating/throwing statement
// between the hook-install mallctl write and Commit() at either call site —
// that would release an arena with live custom hooks into the global pool.
class PendingArena {
 public:
  explicit PendingArena(unsigned arena_idx) noexcept : arena_idx_(arena_idx) {}

  ~PendingArena() noexcept {
    if (arena_idx_ == 0) return;
    try {
      GlobalArenaPool::Instance().Release(arena_idx_);
    } catch (const std::exception &e) {
      LogDestructorCleanupFailure("PendingArena", arena_idx_, "release", e.what());
    } catch (...) {
      LogDestructorCleanupFailure("PendingArena", arena_idx_, "release", "unknown exception");
    }
  }

  void Commit() noexcept { arena_idx_ = 0; }

 private:
  unsigned arena_idx_{0};
};

}  // namespace

ArenaPool::ArenaPool(utils::MemoryTracker *tracker) {
  const std::lock_guard<std::mutex> lock(arena_mux_);

  first_arena_idx_ = GlobalArenaPool::Instance().Acquire();
  PendingArena pending(first_arena_idx_);

  // Everything that can throw must happen BEFORE the hook install below, so a
  // constructor failure never has to un-install hooks (see PendingArena).
  arenas_.reserve(1);

  if (ConsumeFailureInjection(testing::ArenaPoolFailureInjection::ConstructorThrow)) {
    throw std::runtime_error("Injected ArenaPool constructor publish failure");
  }

  const std::string hooks_key = fmt::format("arena.{}.extent_hooks", first_arena_idx_);

  extent_hooks_t *base_hooks = nullptr;
  size_t hooks_sz = sizeof(extent_hooks_t *);
  if (int err = je_mallctl(hooks_key.c_str(), static_cast<void *>(&base_hooks), &hooks_sz, nullptr, 0);
      err != 0 || base_hooks == nullptr) {
    throw std::runtime_error(fmt::format("Failed to read default hooks for arena {} (err={})", first_arena_idx_, err));
  }

  hooks_ = std::make_unique<DbArenaHooks>();
  InitDbArenaHooks(*hooks_, tracker, base_hooks);
  const extent_hooks_t *new_hooks = &hooks_->hooks;
  if (int err = je_mallctl(hooks_key.c_str(),
                           nullptr,
                           nullptr,
                           static_cast<void *>(const_cast<extent_hooks_t **>(&new_hooks)),
                           sizeof(extent_hooks_t *));
      err != 0) {
    // A failed mallctl write never installs the hooks, so letting hooks_ die
    // during unwinding is safe — jemalloc holds no reference to it.
    throw std::runtime_error(fmt::format("Failed to install hooks for arena {} (err={})", first_arena_idx_, err));
  }

  // No-throw from here on (capacity reserved above).
  arenas_.push_back(first_arena_idx_);
  free_count_ = arenas_.size();
  pending.Commit();
}

ArenaPool::~ArenaPool() noexcept {
  try {
    DMG_ASSERT(free_count_ == arenas_.size(), "Destroying DB ArenaPool while some arenas are still in use");
    DMG_ASSERT(first_arena_use_count_ == 0, "Destroying DB ArenaPool while the first arena is still in use");

    // First release all the tcached memory back to the arenas, then purge them
    DestroyAllTcaches();

    // All arenas in the pool share the single `hooks_` struct. If restoring the
    // default hooks fails for ANY arena, that arena keeps these hooks installed,
    // so the struct must outlive the pool. Defer the leak until AFTER the loop:
    // releasing `hooks_` mid-loop would null it and crash the next iteration's
    // `hooks_->base_hooks` read.
    bool restore_failed = false;
    for (const auto arena_idx : arenas_) {
      // Purge all dirty/muzzy pages back to the OS FIRST, while our custom hooks are
      // still installed.
      if (int perr = je_mallctl(fmt::format("arena.{}.purge", arena_idx).c_str(), nullptr, nullptr, nullptr, 0);
          perr != 0) {
        spdlog::error(
            "ArenaPool {}: purge failed (err={}); MemoryTracker may drift before hook restore", arena_idx, perr);
      }

      // Restore the default hooks AFTER purging
      const extent_hooks_t *base = hooks_->base_hooks;
      int err = 0;
      if (ConsumeFailureInjection(testing::ArenaPoolFailureInjection::DestructorRestore)) {
        err = EFAULT;  // simulate arenas[ind] nulled by arena.<i>.destroy
      } else {
        err = je_mallctl(fmt::format("arena.{}.extent_hooks", arena_idx).c_str(),
                         nullptr,
                         nullptr,
                         static_cast<void *>(const_cast<extent_hooks_t **>(&base)),
                         sizeof(extent_hooks_t *));
      }
      if (err != 0) {
        spdlog::error(
            "ArenaPool {}: failed to restore default hooks (err={}); leaking hooks_ and the arena to "
            "avoid a use-after-free on this orphaned arena",
            arena_idx,
            err);
        restore_failed = true;
        // Do NOT release the arena back to GlobalArenaPool: it still has our hooks
        // installed, so reusing the index would install a second hooks struct on top.
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

    if (restore_failed) {
      // Defense in depth: the only realistic trigger is arenas[ind] nulled by arena.<i>.destroy
      // (jemalloc 5.2.1 ctl.c) — Memgraph never calls it. If it ever fires, the arena keeps our
      // hooks, so leak
      // hooks_ (~64 B) and null the tracker so late background-thread callbacks no-op instead of
      // touching the destroyed Database's tracker.
      if (hooks_) {
        hooks_->tracker.store(nullptr, std::memory_order_relaxed);
        [[maybe_unused]] auto *leaked = hooks_.release();
      }
    }
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

  // 4. Install hooks; pending releases new_idx if the install fails
  PendingArena pending(new_idx);
  if (!InstallDbArenaHooks(new_idx, *hooks_, "per-thread")) {
    spdlog::trace("Failed to install hooks on arena. Fallback to first arena...");
    ++first_arena_use_count_;
    return first_arena_idx_;  // pending dtor releases new_idx
  }

  // No-throw from here on (capacity reserved above).
  arenas_.push_back(new_idx);
  pending.Commit();
  return new_idx;
}

void ArenaPool::Release(unsigned arena_idx) noexcept {
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
  } catch (const std::exception &e) {
    SafeLog("Exception while releasing arena {}: {}", arena_idx, e.what());
  } catch (...) {
    SafeLog("Exception while releasing arena {}: unknown exception", arena_idx);
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
    je_mallctl(fmt::format("arena.{}.purge", arena_idx).c_str(), nullptr, nullptr, nullptr, 0);
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

void ArenaPool::ReleaseTcache(unsigned tcache_id) noexcept {
  if (tcache_id == 0) return;
  try {
    const std::lock_guard<std::mutex> lock(tcache_mutex_);
    tcaches_.push_back(tcache_id);
  } catch (const std::exception &e) {
    const size_t sz = sizeof(unsigned);
    je_mallctl("tcache.destroy", nullptr, nullptr, &tcache_id, sz);
    SafeLog("Exception while releasing tcache {}: {}", tcache_id, e.what());
  } catch (...) {
    const size_t sz = sizeof(unsigned);
    je_mallctl("tcache.destroy", nullptr, nullptr, &tcache_id, sz);
    SafeLog("Exception while releasing tcache {}: unknown exception", tcache_id);
  }
}

void ArenaPool::DestroyAllTcaches() {
  const std::lock_guard<std::mutex> lock(tcache_mutex_);
  for (unsigned tcache_id : tcaches_) {
    const size_t sz = sizeof(unsigned);
    je_mallctl("tcache.destroy", nullptr, nullptr, &tcache_id, sz);
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

void ArenaPool::Release(unsigned /*arena_idx*/) noexcept {}

bool ArenaPool::Owns(unsigned /*arena_idx*/) const { return false; }

void ArenaPool::PurgeAllArenas() const {}

unsigned ArenaPool::AcquireTcache() { return 0U; }

void ArenaPool::ReleaseTcache(unsigned /*tcache_id*/) noexcept {}

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
#ifndef NDEBUG
  // Debug-only: verify the pointer belongs to the current DB's arena pool.
  // A mismatch means we are about to free memory via the wrong tcache or
  // arena, which can corrupt jemalloc state.
  if (p != nullptr && tls_db_arena_state.arena_pool != nullptr) {
    unsigned ptr_arena = 0;
    size_t sz = sizeof(ptr_arena);
    if (je_mallctl("arenas.lookup", &ptr_arena, &sz, static_cast<void *>(&p), sizeof(p)) == 0) {
      if (!tls_db_arena_state.arena_pool->Owns(ptr_arena)) {
        LOG_FATAL(
            "DbDeallocateBytes: pointer {} belongs to arena {} which is NOT owned by current ArenaPool", p, ptr_arena);
      }
    }
  }
#endif
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

DbArenaScope::DbArenaScope(ArenaPool *arena_pool) : prev_state_(tls_db_arena_state) {
  cur_state_.arena_pool = arena_pool;
#if USE_JEMALLOC

  auto acquire = [this]() {
    cur_state_.arena = cur_state_.arena_pool ? cur_state_.arena_pool->Acquire() : 0U;
    cur_state_.tcache = cur_state_.arena_pool ? cur_state_.arena_pool->AcquireTcache() : 0U;
    tls_db_arena_state = cur_state_;
  };

  // Fast path: nested scope — same pool already active in TLS.
  if (prev_state_.arena_pool == arena_pool) {
    if (prev_state_.arena != 0) {  // Borrow
      borrowed_ = true;
      cur_state_.arena = prev_state_.arena;
      cur_state_.tcache = prev_state_.tcache;
    } else {  // Acquire from pool
      acquire();
    }
    return;
  }

  DMG_ASSERT(prev_state_.arena_pool == nullptr, "Crashing into a different DB arena pool!");

  // Acquire from pool (first time or different pool).
  acquire();
#else
  (void)arena_pool;
  tls_db_arena_state.arena = 0U;
  tls_db_arena_state.arena_pool = nullptr;
  DMG_ASSERT(prev_state_.arena == 0U, "Erasing DB scope!");
#endif
}

DbArenaScope::~DbArenaScope() noexcept {
  // Restore pre state
  tls_db_arena_state = prev_state_;
  // Cleanup
  if (!borrowed_) {
    if (cur_state_.arena_pool && cur_state_.arena != 0) {
      cur_state_.arena_pool->Release(cur_state_.arena);
    }
    if (cur_state_.arena_pool && cur_state_.tcache != 0) {
      cur_state_.arena_pool->ReleaseTcache(cur_state_.tcache);
    }
  }
}

}  // namespace memgraph::memory
