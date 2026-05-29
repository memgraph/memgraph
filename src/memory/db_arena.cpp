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
#include <exception>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>

#ifndef NDEBUG
#include <execinfo.h>
#include <pthread.h>
#include <unistd.h>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unordered_map>
#include <unordered_set>
#endif

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

#ifndef NDEBUG
// ---------------------------------------------------------------------------
// Debug-only instrumentation for the per-DB extent hooks.
//
// The crash under investigation is `db_arena_dalloc` being invoked from a
// tantivy `merge_thread_N`'s `tcache_destroy` at thread exit, dereferencing a
// `DbArenaHooks::tracker` that points at an already-destroyed per-DB
// MemoryTracker. These checks pin down which of two failure modes is happening:
//
//   * LIFETIME  — the DbArenaHooks struct itself was freed/restored while a
//                 foreign thread still had the arena's extent cached/bound. The
//                 live-hooks registry catches this: a hook firing with a `dh`
//                 not in the registry is a use-after-free of the hooks struct.
//   * CROSS-TALK — a *live* per-DB hook fires from a thread that is NOT under
//                 the owning DbArenaScope (tls_db_arena_state.arena_pool ==
//                 nullptr), e.g. a tantivy merge thread or a jemalloc bg
//                 thread decaying the per-DB arena.
//
// Everything here is allocation-free (snprintf to a stack buffer + write(2))
// so it is safe to call from inside an extent hook, which jemalloc may invoke
// during tcache flush / arena decay where re-entering the allocator (spdlog,
// fmt) could deadlock.
// ---------------------------------------------------------------------------
namespace {
std::mutex g_live_hooks_mutex;
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::unordered_set<const DbArenaHooks *> g_live_hooks;

void RegisterLiveHooks(const DbArenaHooks *h) {
  const std::lock_guard lock(g_live_hooks_mutex);
  g_live_hooks.insert(h);
}

void UnregisterLiveHooks(const DbArenaHooks *h) {
  const std::lock_guard lock(g_live_hooks_mutex);
  g_live_hooks.erase(h);
}

bool IsLiveHooks(const DbArenaHooks *h) {
  const std::lock_guard lock(g_live_hooks_mutex);
  return g_live_hooks.contains(h);
}

// --- Runtime toggles (read once) ----------------------------------------------
// All instrumentation defaults ON in Debug. The alloc-origin capture below is
// the only piece heavy enough to perturb the shutdown race; set
// MG_DBARENA_NO_ALLOC_ORIGIN=1 to drop it while keeping the (cheap) escape
// detector and hook backtraces.
bool BacktraceEnabled() {
  static const bool v = (std::getenv("MG_DBARENA_NO_BACKTRACE") == nullptr);
  return v;
}

bool AllocOriginEnabled() {
  static const bool v = (std::getenv("MG_DBARENA_NO_ALLOC_ORIGIN") == nullptr);
  return v;
}

// Allocation-free backtrace dump (backtrace_symbols_fd is async-signal-safe and
// does NOT call malloc, unlike backtrace_symbols). Safe to call from inside an
// extent hook where re-entering the allocator could deadlock.
void DumpBacktrace(const char *tag) {
  if (!BacktraceEnabled()) return;
  void *frames[32];
  const int n = backtrace(frames, 32);
  char hdr[64];
  const int hn = snprintf(hdr, sizeof(hdr), "[db_arena %s] --- backtrace (%d) ---\n", tag, n);
  if (hn > 0) {
    ssize_t ignored = write(STDERR_FILENO, hdr, static_cast<size_t>(hn));
    (void)ignored;
  }
  backtrace_symbols_fd(frames, n, STDERR_FILENO);
}

// --- Live per-DB arena index snapshot (lock-free hot-path read) ----------------
// Holds the arena indices that currently have per-DB extent hooks installed. The
// escape detector in the global free path reads this on every free, so it is a
// fixed-size atomic array (per-DB pools own only 1-2 arenas at a time).
constexpr size_t kMaxLiveArenas = 64;
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<unsigned> g_db_arenas[kMaxLiveArenas];
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<size_t> g_db_arena_n{0};
std::mutex g_db_arenas_mutex;

void RegisterLiveArena(unsigned idx) {
  if (idx == 0) return;
  const std::lock_guard lock(g_db_arenas_mutex);
  const size_t n = g_db_arena_n.load(std::memory_order_relaxed);
  for (size_t i = 0; i < n; ++i) {
    if (g_db_arenas[i].load(std::memory_order_relaxed) == idx) return;  // already present
  }
  if (n >= kMaxLiveArenas) return;
  g_db_arenas[n].store(idx, std::memory_order_relaxed);
  g_db_arena_n.store(n + 1, std::memory_order_release);
}

void UnregisterLiveArena(unsigned idx) {
  if (idx == 0) return;
  const std::lock_guard lock(g_db_arenas_mutex);
  size_t n = g_db_arena_n.load(std::memory_order_relaxed);
  for (size_t i = 0; i < n; ++i) {
    if (g_db_arenas[i].load(std::memory_order_relaxed) == idx) {
      g_db_arenas[i].store(g_db_arenas[n - 1].load(std::memory_order_relaxed), std::memory_order_relaxed);
      g_db_arena_n.store(n - 1, std::memory_order_release);
      return;
    }
  }
}

bool IsLiveDbArena(unsigned idx) {
  if (idx == 0) return false;
  const size_t n = g_db_arena_n.load(std::memory_order_acquire);
  for (size_t i = 0; i < n; ++i) {
    if (g_db_arenas[i].load(std::memory_order_relaxed) == idx) return true;
  }
  return false;
}

// --- Allocation-origin map -----------------------------------------------------
// addr -> backtrace captured at the per-DB allocation site. Populated by
// DbAllocateBytes, erased by DbDeallocateBytes (the legitimate per-DB free). A
// pointer freed via the GLOBAL free path (the escape) is NOT erased, so its
// allocating backtrace is still here when the escape detector finds it -> names
// the exact C++ object that leaked onto a foreign thread.
struct AllocRec {
  void *frames[24];
  int depth;
  size_t size;
  unsigned arena_idx;
};

std::mutex g_alloc_map_mutex;
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::unordered_map<void *, AllocRec> g_alloc_map;

void RecordAllocOrigin(void *ptr, size_t size, unsigned arena_idx) {
  if (ptr == nullptr || !AllocOriginEnabled()) return;
  AllocRec rec{};
  rec.depth = backtrace(rec.frames, 24);
  rec.size = size;
  rec.arena_idx = arena_idx;
  const std::lock_guard lock(g_alloc_map_mutex);
  g_alloc_map[ptr] = rec;
}

void ForgetAllocOrigin(void *ptr) {
  if (ptr == nullptr || !AllocOriginEnabled()) return;
  const std::lock_guard lock(g_alloc_map_mutex);
  g_alloc_map.erase(ptr);
}

// Dump (and consume) the stored allocation backtrace for ptr, if any.
void DumpAllocOrigin(void *ptr) {
  if (!AllocOriginEnabled()) return;
  AllocRec rec{};
  bool found = false;
  {
    const std::lock_guard lock(g_alloc_map_mutex);
    if (auto it = g_alloc_map.find(ptr); it != g_alloc_map.end()) {
      rec = it->second;
      found = true;
    }
  }
  char hdr[96];
  const int hn = found ? snprintf(hdr,
                                  sizeof(hdr),
                                  "[db_arena ESCAPING-FREE] alloc-origin size=%zu arena_ind=%u (%d frames):\n",
                                  rec.size,
                                  rec.arena_idx,
                                  rec.depth)
                       : snprintf(hdr, sizeof(hdr), "[db_arena ESCAPING-FREE] alloc-origin: <not in map>\n");
  if (hn > 0) {
    ssize_t ignored = write(STDERR_FILENO, hdr, static_cast<size_t>(hn));
    (void)ignored;
  }
  if (found) backtrace_symbols_fd(rec.frames, rec.depth, STDERR_FILENO);
}

// A tantivy/rayon worker thread — the decisive (dangerous) caller. Main/mg
// threads are benign here (teardown purge), so we never want to drown the
// worker-thread smoking gun under their backtraces.
bool ThreadIsWorker() {
  char tname[32] = {0};
  pthread_getname_np(pthread_self(), tname, sizeof(tname));
  return (strstr(tname, "merge") != nullptr) || (strstr(tname, "segment") != nullptr) ||
         (strstr(tname, "tantivy") != nullptr) || (strstr(tname, "rayon") != nullptr);
}

void ReportHookIssue(const char *severity, const char *op, const DbArenaHooks *dh, unsigned arena_ind) {
  char tname[32] = {0};
  pthread_getname_np(pthread_self(), tname, sizeof(tname));
  char buf[256];
  const int n = snprintf(buf,
                         sizeof(buf),
                         "[db_arena %s] op=%s hooks=%p arena_ind=%u thread=%s scoped=%d\n",
                         severity,
                         op,
                         static_cast<const void *>(dh),
                         arena_ind,
                         tname,
                         tls_db_arena_state.arena_pool != nullptr);
  if (n > 0) {
    ssize_t ignored = write(STDERR_FILENO, buf, static_cast<size_t>(n));
    (void)ignored;
  }
}

// Validate a hook callback BEFORE it dereferences dh->tracker. IsLiveHooks does
// not dereference dh, so it is safe even when dh is a dangling pointer.
void ValidateHookCall(const DbArenaHooks *dh, unsigned arena_ind, const char *op) {
  if (!IsLiveHooks(dh)) {
    // The hooks struct is not currently installed by any live ArenaPool: a
    // foreign thread is decaying/flushing an extent through freed hooks.
    ReportHookIssue("FATAL-STALE-HOOKS-UAF", op, dh, arena_ind);
    DumpBacktrace("FATAL-STALE-HOOKS-UAF");
    abort();
  }
  if (tls_db_arena_state.arena_pool == nullptr) {
    // Live hooks, but the calling thread holds no DbArenaScope: this is the
    // foreign-thread decay (tantivy merge / jemalloc background_thread) that
    // becomes a UAF once the owning DB's MemoryTracker is destroyed. The
    // backtrace shows the jemalloc internal path: tcache_bin_flush_* frames
    // mean the thread cached & is flushing an arena-N region (mechanism A);
    // arena_decay_* from tsd_cleanup means the thread's tcache is bound to the
    // per-DB arena (mechanism B).
    ReportHookIssue("WARN-UNSCOPED-FOREIGN-DECAY", op, dh, arena_ind);
    // Full backtrace for every worker-thread decay (the smoking gun) plus a
    // small global budget for the rest, so benign main-thread teardown purges
    // do not bury the signal under hundreds of identical stacks.
    static std::atomic<int> warn_dump_budget{32};
    if (ThreadIsWorker() || warn_dump_budget.fetch_sub(1, std::memory_order_relaxed) > 0) {
      DumpBacktrace("WARN-UNSCOPED-FOREIGN-DECAY");
    }
  }
}
}  // namespace
#endif  // NDEBUG

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
#ifndef NDEBUG
  ValidateHookCall(dh, arena_ind, "alloc");
#endif
  const bool requested_commit = *commit;
  // tracker may be null on an abandoned arena whose ArenaPool failed to restore default hooks;
  // in that case we skip tracking but still proxy to the base jemalloc hooks.
  // Pre-track if commit was requested (mandatory — base hook must return committed or fail).
  if (requested_commit) {
    if (dh->tracker && !dh->tracker->Alloc(static_cast<int64_t>(size))) {
      return nullptr;
    }
  }
  void *ptr = dh->base_hooks->alloc(dh->base_hooks, new_addr, size, alignment, zero, commit, arena_ind);
  if (ptr == nullptr) {
    if (requested_commit && dh->tracker) dh->tracker->Free(static_cast<int64_t>(size));
    return nullptr;
  }
  // *commit is an out-parameter: the base hook may have committed pages even if we didn't ask.
  if (*commit && !requested_commit) {
    if (dh->tracker) {
      const utils::MemoryTracker::OutOfMemoryExceptionBlocker blocker;
      dh->tracker->Alloc(static_cast<int64_t>(size));
    }
  }
  return ptr;
}

bool db_arena_dalloc(extent_hooks_t *hooks, void *addr, size_t size, bool committed, unsigned arena_ind) {
  auto *dh = reinterpret_cast<DbArenaHooks *>(hooks);
#ifndef NDEBUG
  ValidateHookCall(dh, arena_ind, "dalloc");
#endif
  const bool err = dh->base_hooks->dalloc(dh->base_hooks, addr, size, committed, arena_ind);
  if (!err && committed) {
    if (dh->tracker) dh->tracker->Free(static_cast<int64_t>(size));
  }
  return err;
}

void db_arena_destroy(extent_hooks_t *hooks, void *addr, size_t size, bool committed, unsigned arena_ind) {
  auto *dh = reinterpret_cast<DbArenaHooks *>(hooks);
#ifndef NDEBUG
  ValidateHookCall(dh, arena_ind, "destroy");
#endif
  if (committed) {
    if (dh->tracker) dh->tracker->Free(static_cast<int64_t>(size));
  }
  dh->base_hooks->destroy(dh->base_hooks, addr, size, committed, arena_ind);
}

bool db_arena_commit(extent_hooks_t *hooks, void *addr, size_t size, size_t offset, size_t length, unsigned arena_ind) {
  auto *dh = reinterpret_cast<DbArenaHooks *>(hooks);
#ifndef NDEBUG
  ValidateHookCall(dh, arena_ind, "commit");
#endif
  const bool err = dh->base_hooks->commit(dh->base_hooks, addr, size, offset, length, arena_ind);
  if (!err) {
    if (dh->tracker) {
      // Pages are already committed by the OS — we cannot undo the commit here.
      // OutOfMemoryExceptionBlocker makes MemoryTrackerCanThrow() return false on this thread,
      // which means Alloc() will never enter its rollback path: the fetch_add is permanent and
      // the entire tracker chain returns true unconditionally. Tracking is therefore guaranteed.
      const utils::MemoryTracker::OutOfMemoryExceptionBlocker blocker;
      dh->tracker->Alloc(static_cast<int64_t>(length));
    }
  }
  return err;
}

bool db_arena_decommit(extent_hooks_t *hooks, void *addr, size_t size, size_t offset, size_t length,
                       unsigned arena_ind) {
  auto *dh = reinterpret_cast<DbArenaHooks *>(hooks);
#ifndef NDEBUG
  ValidateHookCall(dh, arena_ind, "decommit");
#endif
  const bool err = dh->base_hooks->decommit(dh->base_hooks, addr, size, offset, length, arena_ind);
  if (!err) {
    if (dh->tracker) dh->tracker->Free(static_cast<int64_t>(length));
  }
  return err;
}

bool db_arena_purge_forced(extent_hooks_t *hooks, void *addr, size_t size, size_t offset, size_t length,
                           unsigned arena_ind) {
  auto *dh = reinterpret_cast<DbArenaHooks *>(hooks);
#ifndef NDEBUG
  ValidateHookCall(dh, arena_ind, "purge_forced");
#endif
  if (dh->base_hooks->purge_forced == nullptr) return true;
  const bool err = dh->base_hooks->purge_forced(dh->base_hooks, addr, size, offset, length, arena_ind);
  if (!err) {
    if (dh->tracker) dh->tracker->Free(static_cast<int64_t>(length));
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

  hooks_ = std::make_unique<DbArenaHooks>();
  InitDbArenaHooks(*hooks_, tracker, base_hooks);
  const extent_hooks_t *new_hooks = &hooks_->hooks;
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
#ifndef NDEBUG
  // Construction succeeded: the hooks are installed and reachable by jemalloc.
  // Acquire() installs this same struct on any additional arenas, so a single
  // registration covers the whole pool.
  RegisterLiveHooks(hooks_.get());
  RegisterLiveArena(first_arena_idx_);
  {
    char buf[160];
    const int n = snprintf(buf,
                           sizeof(buf),
                           "[db_arena ARENA-LIVE] arena_ind=%u hooks=%p tracker=%p\n",
                           first_arena_idx_,
                           static_cast<void *>(hooks_.get()),
                           static_cast<void *>(tracker));
    if (n > 0) {
      ssize_t ignored = write(STDERR_FILENO, buf, static_cast<size_t>(n));
      (void)ignored;
    }
  }
#endif
}

ArenaPool::~ArenaPool() noexcept {
  try {
    DMG_ASSERT(free_count_ == arenas_.size(), "Destroying DB ArenaPool while some arenas are still in use");
    DMG_ASSERT(first_arena_use_count_ == 0, "Destroying DB ArenaPool while the first arena is still in use");

    // First release all the tcached memory back to the arenas, then purge them
    DestroyAllTcaches();

    for (const auto arena_idx : arenas_) {
      const std::string arena_key = "arena." + std::to_string(arena_idx);

      // Purge all dirty/muzzy pages back to the OS FIRST, while our custom hooks are
      // still installed.
      if (int perr = je_mallctl((arena_key + ".purge").c_str(), nullptr, nullptr, nullptr, 0); perr != 0) {
        spdlog::error(
            "ArenaPool {}: purge failed (err={}); MemoryTracker may drift before hook restore", arena_idx, perr);
      }

      // Restore the default hooks AFTER purging
      const extent_hooks_t *base = hooks_->base_hooks;
      int err = je_mallctl((arena_key + ".extent_hooks").c_str(),
                           nullptr,
                           nullptr,
                           static_cast<void *>(const_cast<extent_hooks_t **>(&base)),
                           sizeof(extent_hooks_t *));
      if (err != 0) {
        spdlog::error(
            "ArenaPool {}: failed to restore default hooks (err={}); hooks_ may outlive arena", arena_idx, err);
        spdlog::error("ArenaPool {}: leaking arena from GlobalArenaPool reuse after failed hook restore", arena_idx);
        hooks_->tracker = nullptr;
        // Intentional leak: keep hooks alive so the orphaned arena (which we failed to restore to
        // default hooks) does not call back into freed memory on subsequent allocations.
        (void)hooks_.release();  // NOLINT(bugprone-unused-return-value)
        continue;
      }

#ifndef NDEBUG
      // Hooks restored to default for this arena: it is no longer a per-DB
      // arena. After this point any foreign-thread free of an arena_idx pointer
      // is a stale/recycled-arena access. (On the unhook-failure path above we
      // `continue` without reaching here, so the arena stays registered — its
      // hooks are intentionally leaked and still installed.)
      UnregisterLiveArena(arena_idx);
      {
        char buf[128];
        const int n = snprintf(buf, sizeof(buf), "[db_arena ARENA-GONE] arena_ind=%u (hooks restored)\n", arena_idx);
        if (n > 0) {
          ssize_t ignored = write(STDERR_FILENO, buf, static_cast<size_t>(n));
          (void)ignored;
        }
      }
#endif
      try {
        GlobalArenaPool::Instance().Release(arena_idx);
      } catch (const std::exception &e) {
        LogDestructorCleanupFailure("ArenaPool", arena_idx, "release", e.what());
      } catch (...) {
        LogDestructorCleanupFailure("ArenaPool", arena_idx, "release", "unknown exception");
      }
    }
#ifndef NDEBUG
    // All arenas restored to default hooks above, so the hooks struct is no
    // longer reachable by jemalloc on the happy path; drop it from the live
    // registry. On the unhook-failure path hooks_ was release()'d (now null)
    // and intentionally leaked with tracker=nullptr, so we deliberately leave
    // it registered — a late call there hits the tracker==nullptr guard in the
    // hook body and is harmless, and must not trip the stale-hooks abort.
    if (hooks_) UnregisterLiveHooks(hooks_.get());
#endif
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
  if (!InstallDbArenaHooks(new_idx, *hooks_, "per-thread")) {
    spdlog::trace("Failed to install hooks on arena. Fallback to first arena...");
    ++first_arena_use_count_;
    return first_arena_idx_;  // pending dtor releases new_idx
  }

  pending.MarkHooksInstalled(hooks_->base_hooks);
  arenas_.push_back(new_idx);
  pending.Commit();
#ifndef NDEBUG
  RegisterLiveArena(new_idx);
  {
    char buf[160];
    const int n = snprintf(buf,
                           sizeof(buf),
                           "[db_arena ARENA-LIVE] arena_ind=%u hooks=%p (Acquire)\n",
                           new_idx,
                           static_cast<void *>(hooks_.get()));
    if (n > 0) {
      ssize_t ignored = write(STDERR_FILENO, buf, static_cast<size_t>(n));
      (void)ignored;
    }
  }
#endif
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
    arena_indices.reserve(arenas_.size());
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
    void *p = ::JeNew(bytes, flags);
#ifndef NDEBUG
    // Record the call site so that, if this per-DB pointer is later freed via
    // the GLOBAL free path on a foreign thread, the escape detector can name
    // the object that leaked.
    RecordAllocOrigin(p, bytes, idx);
#endif
    return p;
  }
#endif
  return ::operator new(bytes, std::align_val_t{alignment});
}

void DbDeallocateBytes(void *p, std::size_t bytes, std::size_t alignment) noexcept {
#if USE_JEMALLOC
#ifndef NDEBUG
  // This is the legitimate per-DB free path: drop the alloc-origin record so a
  // later GLOBAL-path free of the same address is not mistaken for an escape.
  ForgetAllocOrigin(p);
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

#ifndef NDEBUG
namespace dbg {
// Called from the GLOBAL free shims (free / dallocx / sdallocx / JeDealloc) on
// every free. If a foreign (unscoped) thread frees a pointer that belongs to a
// per-DB arena, this is the ownership escape that ultimately crashes: the region
// enters the foreign thread's automatic tcache and is flushed to the per-DB
// arena at thread exit, possibly after ~ArenaPool. We log (never abort) so a
// single run captures every escape, the freeing thread, and — via the
// alloc-origin map — the C++ object and call site that leaked.
void CheckEscapingFree(void *ptr, const char *op, int flags) noexcept {
#if USE_JEMALLOC
  if (ptr == nullptr) return;
  // Only AUTOMATIC-tcache frees are dangerous: they cache the per-DB extent in
  // the freeing thread and defer its return to the arena until tcache flush /
  // thread exit (the UAF window). The legitimate per-DB free path
  // (DbDeallocateBytes -> JeFree -> JeDealloc) passes MALLOCX_TCACHE_NONE or an
  // explicit tcache; jemalloc encodes the tcache selector in bits [8..19], so a
  // non-zero tcache field means this is NOT an automatic-tcache free -> skip.
  constexpr int kTcacheFieldMask = 0xFFF << 8;
  if ((flags & kTcacheFieldMask) != 0) return;
  if (g_db_arena_n.load(std::memory_order_acquire) == 0) return;  // no per-DB arena currently live
  // A thread under a DbArenaScope frees per-DB memory through DbDeallocateBytes,
  // not here; skip it. Unscoped frees of per-DB pointers are either the escape
  // (e.g. a tantivy merge thread) or benign main-thread teardown — the logged
  // thread name distinguishes them.
  if (tls_db_arena_state.arena_pool != nullptr) return;
  unsigned ptr_arena = 0;
  size_t sz = sizeof(ptr_arena);
  if (je_mallctl("arenas.lookup", &ptr_arena, &sz, static_cast<void *>(&ptr), sizeof(ptr)) != 0) return;
  if (!IsLiveDbArena(ptr_arena)) return;

  // Bound the output. The DANGEROUS escapes (tantivy merge/segment threads) free
  // during operation, BEFORE teardown, so they consume the dump budget first;
  // the BENIGN flood (main thread freeing the whole graph during ~Storage)
  // arrives last and only gets rate-limited one-liners. A merge thread is also
  // always granted a full dump regardless of budget.
  char tname[32] = {0};
  pthread_getname_np(pthread_self(), tname, sizeof(tname));
  const bool worker_thread = (strstr(tname, "merge") != nullptr) || (strstr(tname, "segment") != nullptr) ||
                             (strstr(tname, "tantivy") != nullptr) || (strstr(tname, "rayon") != nullptr);

  // Per-thread one-line cap: first 16, then every 4096th.
  thread_local uint64_t tl_seen = 0;
  const uint64_t seen = tl_seen++;
  const bool log_line = worker_thread || seen < 16 || (seen % 4096 == 0);
  if (log_line) {
    char buf[256];
    const int n = snprintf(buf,
                           sizeof(buf),
                           "[db_arena ESCAPING-FREE] op=%s addr=%p arena_ind=%u thread=%s scoped=0 seen=%llu\n",
                           op,
                           ptr,
                           ptr_arena,
                           tname,
                           static_cast<unsigned long long>(seen));
    if (n > 0) {
      ssize_t ignored = write(STDERR_FILENO, buf, static_cast<size_t>(n));
      (void)ignored;
    }
  }

  // Full backtrace dump: every worker-thread escape, plus a global budget for
  // the rest (so the earliest escapes are always captured in full).
  static std::atomic<int> dump_budget{256};
  if (worker_thread || dump_budget.fetch_sub(1, std::memory_order_relaxed) > 0) {
    DumpBacktrace("ESCAPING-FREE free-site");
    DumpAllocOrigin(ptr);
  }
#else
  (void)ptr;
  (void)op;
#endif
}
}  // namespace dbg
#endif  // NDEBUG

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
