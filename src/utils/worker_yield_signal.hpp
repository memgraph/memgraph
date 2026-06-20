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

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>

#include "utils/logging.hpp"

namespace memgraph::utils {

/**
 * Per-worker yield signal for scheduler-driven query yield.
 *
 * Each pool worker owns one `std::atomic<bool>` yield flag. An external thread
 * (e.g. a high-priority task arriving at the scheduler) can set a worker's flag
 * to request that the worker's currently-running query cooperatively yield. The
 * running worker publishes a thread-local pointer to its own flag so the query
 * layer (StoppingContext) can poll it without knowing worker ids.
 *
 * DESIGN (differs from a naive shared_ptr-per-signal registry):
 *   - The flags live in ONE contiguous, owned block of cache-line-padded atomics
 *     (`PaddedSignal`, `alignas(kCacheLine)`). This avoids both per-signal
 *     `shared_ptr` refcount traffic on the hot publish path AND false sharing
 *     between workers polling/clearing adjacent flags.
 *   - The thread-local "current signal" is a raw pointer INTO that block. It is
 *     stable for the registry's lifetime (the block is sized once at construction
 *     and never reallocated), so the raw pointer never dangles while the registry
 *     is alive.
 *   - Not a singleton: an instance is injected, which keeps it unit-testable and
 *     makes lifetime explicit.
 *
 * LIFETIME CONTRACT (load-bearing): the registry must OUTLIVE every thread that
 * may call SetCurrentWorker / GetCurrentYieldSignal. In production this means the
 * registry is declared BEFORE (and so destroyed AFTER) the thread pool. Behavior
 * preservation hinges on `GetCurrentYieldSignal()` returning nullptr when the
 * caller is not running on a registered worker (or after ClearCurrentWorker) —
 * the query layer treats a null signal as "yield never requested", i.e. legacy
 * behavior.
 *
 * Usage:
 *   1. Construct once: WorkerYieldRegistry registry(num_workers);
 *   2. Worker thread, before running a task: registry.SetCurrentWorker(worker_id);
 *   3. Worker thread, after the task: WorkerYieldRegistry::ClearCurrentWorker();
 *   4. Query setup: stopping_context.yield_requested = WorkerYieldRegistry::GetCurrentYieldSignal();
 *   5. External thread: registry.RequestYieldForWorker(worker_id);
 *
 * Wiring into the pool / interpreter is intentionally NOT part of this type.
 */
class WorkerYieldRegistry {
 public:
  using WorkerId = uint16_t;
  using YieldSignal = std::atomic<bool>;

  explicit WorkerYieldRegistry(WorkerId max_workers)
      : max_workers_(max_workers), signals_(std::make_unique<PaddedSignal[]>(max_workers)) {
    MG_ASSERT(max_workers > 0, "WorkerYieldRegistry requires at least one worker");
    // PaddedSignal value-initializes each atomic to false.
  }

  WorkerYieldRegistry(const WorkerYieldRegistry &) = delete;
  WorkerYieldRegistry &operator=(const WorkerYieldRegistry &) = delete;
  WorkerYieldRegistry(WorkerYieldRegistry &&) = delete;
  WorkerYieldRegistry &operator=(WorkerYieldRegistry &&) = delete;
  // Destroying the registry frees the signal block. Any thread that still holds a
  // published thread-local pointer (i.e. has not called ClearCurrentWorker) would
  // then dangle -- so the registry MUST be destroyed only after the thread pool is
  // stopped. This is the lifetime contract above; it is documented, not RAII-
  // enforced (there is no live-worker counter to assert on here).
  ~WorkerYieldRegistry() = default;

  /**
   * Returns the raw yield-signal pointer for the given worker. Stable for the
   * registry's lifetime. Safe to call from any thread. worker_id must be in
   * [0, max_workers).
   */
  YieldSignal *GetSignalForWorker(WorkerId worker_id) {
    DMG_ASSERT(worker_id < max_workers_, "worker_id {} out of range (max {})", worker_id, max_workers_);
    return &signals_[worker_id].value;
  }

  /**
   * Request yield for a specific worker. Safe to call from an external thread.
   * Equivalent to GetSignalForWorker(worker_id)->store(true).
   */
  void RequestYieldForWorker(WorkerId worker_id) {
    GetSignalForWorker(worker_id)->store(true, std::memory_order_release);
  }

  /**
   * Clear yield for a specific worker. Safe to call from an external thread.
   */
  void ClearYieldForWorker(WorkerId worker_id) {
    GetSignalForWorker(worker_id)->store(false, std::memory_order_release);
  }

  /**
   * Clear yield for the current worker. Must be called on a registered worker
   * thread (after SetCurrentWorker, before ClearCurrentWorker).
   */
  static void ClearYieldForCurrentWorker() {
    DMG_ASSERT(GetTlsCurrentSignal() != nullptr, "no current yield signal");
    GetTlsCurrentSignal()->store(false, std::memory_order_release);
  }

  /**
   * Called by the worker thread before running a task. Publishes this worker's
   * flag as the thread-local current signal and clears any stale yield request so
   * the new task starts with a clean flag.
   */
  void SetCurrentWorker(WorkerId worker_id) {
    YieldSignal *signal = GetSignalForWorker(worker_id);
    signal->store(false, std::memory_order_release);
    GetTlsCurrentSignal() = signal;
    GetTlsCurrentWorkerId() = worker_id;
  }

  /**
   * Called by the worker thread after running a task. Clears the thread-local so
   * the raw pointer is not observed after the task completes. Static so it can be
   * called without an instance (e.g. at thread exit).
   */
  static void ClearCurrentWorker() {
    GetTlsCurrentSignal() = nullptr;
    GetTlsCurrentWorkerId() = std::nullopt;
  }

  /**
   * Returns the current thread's yield signal, or nullptr if SetCurrentWorker
   * has not been called on this thread (or was cleared). A null result means
   * "yield never requested" -> legacy behavior.
   */
  static YieldSignal *GetCurrentYieldSignal() { return GetTlsCurrentSignal(); }

  /**
   * Returns the current thread's worker id if SetCurrentWorker was called (and
   * not cleared). Use to schedule a continuation that must resume on the same
   * worker.
   */
  static std::optional<WorkerId> GetCurrentWorkerId() { return GetTlsCurrentWorkerId(); }

  WorkerId MaxWorkers() const { return max_workers_; }

 private:
  // One cache line per signal: the contiguous block has no false sharing between
  // workers that concurrently poll/clear their own flags.
  static constexpr std::size_t kCacheLine = 64;

  struct alignas(kCacheLine) PaddedSignal {
    YieldSignal value{false};
  };

  static_assert(sizeof(PaddedSignal) == kCacheLine, "PaddedSignal must occupy exactly one cache line");

  static YieldSignal *&GetTlsCurrentSignal() {
    thread_local YieldSignal *tls_current = nullptr;
    return tls_current;
  }

  static std::optional<WorkerId> &GetTlsCurrentWorkerId() {
    thread_local std::optional<WorkerId> tls_worker_id;
    return tls_worker_id;
  }

  const WorkerId max_workers_;
  std::unique_ptr<PaddedSignal[]> signals_;
};

}  // namespace memgraph::utils
