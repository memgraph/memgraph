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
#include <vector>

#include "utils/logging.hpp"

namespace memgraph::utils {

/**
 * Per-worker yield signal for scheduler-driven query yield.
 *
 * Each registered worker has a shared_ptr to an atomic<bool> that can be set
 * by an external thread to request yield. The running worker uses a thread-local
 * pointer to the current worker's signal so the query layer (e.g. StoppingContext)
 * can check it without knowing worker ids.
 *
 * Usage:
 * 1. Create a registry: WorkerYieldRegistry registry(kMaxWorkers);
 * 2. Before a worker runs a task, call registry.SetCurrentWorker(worker_id);
 *    (e.g. from PriorityThreadPool::Worker::operator() before invoking the task).
 * 3. After the task, call registry.ClearCurrentWorker();
 * 4. When building ExecutionContext/StoppingContext for the query, set
 *    stopping_context.yield_requested = registry.GetCurrentYieldSignal();
 * 5. External thread: registry.RequestYieldForWorker(worker_id) or
 *    registry.GetSignalForWorker(worker_id)->store(true);
 *
 * The registry must outlive all threads that call SetCurrentWorker / GetCurrentYieldSignal.
 */
class WorkerYieldRegistry {
 public:
  using WorkerId = uint16_t;
  using YieldSignal = std::atomic<bool>;
  using YieldSignalPtr = std::shared_ptr<YieldSignal>;

  explicit WorkerYieldRegistry(WorkerId max_workers) : max_workers_(max_workers), signals_(max_workers) {
    MG_ASSERT(max_workers > 0, "WorkerYieldRegistry requires at least one worker");
    for (WorkerId i = 0; i < max_workers; ++i) {
      signals_[i] = std::make_shared<YieldSignal>(false);
    }
  }

  WorkerYieldRegistry(const WorkerYieldRegistry &) = delete;
  WorkerYieldRegistry &operator=(const WorkerYieldRegistry &) = delete;
  WorkerYieldRegistry(WorkerYieldRegistry &&) = delete;
  WorkerYieldRegistry &operator=(WorkerYieldRegistry &&) = delete;

  /**
   * Returns a shared_ptr to the yield signal for the given worker.
   * Safe to call from any thread. worker_id must be in [0, max_workers).
   */
  YieldSignalPtr GetSignalForWorker(WorkerId worker_id) {
    DMG_ASSERT(worker_id < max_workers_, "worker_id {} out of range (max {})", worker_id, max_workers_);
    return signals_[worker_id];
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
   * Equivalent to GetSignalForWorker(worker_id)->store(false).
   */
  void ClearYieldForWorker(WorkerId worker_id) {
    DMG_ASSERT(worker_id < max_workers_, "worker_id {} out of range (max {})", worker_id, max_workers_);
    GetSignalForWorker(worker_id)->store(false, std::memory_order_release);
  }

  /**
   * Clear yield for the current worker. Safe to call from an external thread.
   * Equivalent to ClearYieldForWorker(*GetCurrentWorkerId()).
   */
  static void ClearYieldForCurrentWorker() {
    DMG_ASSERT(GetTlsCurrentSignal(), "no current yield signal");
    GetTlsCurrentSignal()->store(false, std::memory_order_release);
  }

  /**
   * Called by the worker thread before running a task. Sets the thread-local
   * current yield signal to this worker's signal and clears any previous
   * yield request so the new task starts with a clean flag.
   */
  void SetCurrentWorker(WorkerId worker_id) {
    YieldSignalPtr ptr = GetSignalForWorker(worker_id);
    ptr->store(false, std::memory_order_release);
    GetTlsCurrentSignal() = ptr.get();
    GetTlsCurrentWorkerId() = worker_id;
  }

  /**
   * Called by the worker thread after running a task. Clears the thread-local
   * so the pointer is not used after the registry or signals are destroyed.
   * Static so it can be called without an instance (e.g. at thread exit).
   */
  static void ClearCurrentWorker() {
    GetTlsCurrentSignal() = nullptr;
    GetTlsCurrentWorkerId() = std::nullopt;
  }

  /**
   * Returns the current thread's yield signal pointer, or nullptr if
   * SetCurrentWorker has not been called (or was cleared).
   * Use this when building StoppingContext: yield_requested = GetCurrentYieldSignal().
   */
  static YieldSignal *GetCurrentYieldSignal() { return GetTlsCurrentSignal(); }

  /**
   * Returns the current thread's worker id if SetCurrentWorker was called
   * (and not cleared). Use this when scheduling a continuation that must
   * run on the same worker (e.g. after a yield).
   */
  static std::optional<WorkerId> GetCurrentWorkerId() { return GetTlsCurrentWorkerId(); }

  WorkerId MaxWorkers() const { return max_workers_; }

 private:
  static YieldSignal *&GetTlsCurrentSignal() {
    thread_local YieldSignal *tls_current = nullptr;
    return tls_current;
  }

  static std::optional<WorkerId> &GetTlsCurrentWorkerId() {
    thread_local std::optional<WorkerId> tls_worker_id;
    return tls_worker_id;
  }

  const WorkerId max_workers_;
  std::vector<YieldSignalPtr> signals_;
};

}  // namespace memgraph::utils
