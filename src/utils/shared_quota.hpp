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
#include <cassert>
#include <cstdint>
#include <memory>
#include <optional>

namespace memgraph::utils {

class QuotaCoordinator {
  std::atomic<uint64_t> remaining_quota_{0};
  std::atomic<uint64_t> active_handlers_{0};
  // Incremented whenever the state changes in a way waiters care about (quota returned or holders finished).
  std::atomic<uint32_t> epoch_{0};
  std::atomic<uint8_t> initialized_{false};

 public:
  explicit QuotaCoordinator(uint64_t total_limit);
  QuotaCoordinator() = default;

  class QuotaHandle {
    QuotaCoordinator *coord_;
    uint64_t amount_;

   public:
    QuotaHandle(QuotaCoordinator *coord, uint64_t amount);

    ~QuotaHandle() { ReturnUnused(); }

    QuotaHandle(const QuotaHandle &) = delete;
    QuotaHandle &operator=(const QuotaHandle &) = delete;
    QuotaHandle(QuotaHandle &&other) noexcept;
    QuotaHandle &operator=(QuotaHandle &&other) noexcept;

    uint64_t Count() const { return amount_; }

    // Returns the amount that was actually consumed.
    uint64_t Consume(uint64_t amount);
    void Increment(uint64_t amount);

   private:
    void ReturnUnused();
  };

  void Initialize(uint64_t limit);
  std::optional<QuotaHandle> Acquire(uint64_t desired_batch_size);

 private:
  friend class QuotaHandle;
  void NotifyWaiters();
};

/**
 * SharedQuota provides thread-safe quota management with adaptive batching.
 *
 * SINGLE-THREADED: Equivalent to a simple counter with amortized O(1) operations.
 *   - First Decrement() acquires entire quota in one batch
 *   - Subsequent operations consume from local batch (no atomic overhead)
 *
 * PARALLEL: Lock-free quota distribution with epoch-based waiting.
 *   - Multiple threads share a QuotaCoordinator via shared_ptr
 *   - Each thread acquires batches adaptively to prevent starvation
 *   - Automatic batch reacquisition when local quota is exhausted
 *
 * Example (single-threaded):
 *   SharedQuota quota(100);  // 100 total quota
 *   while (quota.Decrement() > 0) { process(); }
 *
 * Example (parallel - preloaded):
 *   SharedQuota quota(SharedQuota::preload);  // Create uninitialized
 *   // Later, when limit is known:
 *   quota.Initialize(limit, num_threads);
 *   while (quota.Decrement() > 0) { process(); }
 */
class SharedQuota {
  std::shared_ptr<QuotaCoordinator> coord_{nullptr};
  uint64_t desired_batch_size_{0};
  std::optional<QuotaCoordinator::QuotaHandle> handle_{std::nullopt};

 public:
  constexpr static struct Preload {
  } preload;

  explicit SharedQuota(uint64_t limit, uint64_t n_batches = 1);
  // Used to setup the objects, but not initialize the quota.
  explicit SharedQuota(Preload /*unused*/);
  ~SharedQuota() = default;

  SharedQuota(const SharedQuota &other) noexcept;
  SharedQuota &operator=(const SharedQuota &other) noexcept;
  SharedQuota(SharedQuota &&other) noexcept;
  SharedQuota &operator=(SharedQuota &&other) noexcept;

  // Primary entry point for workers to consume quota.
  uint64_t Decrement(uint64_t amount = 1);
  // Returns quota to the LOCAL handle (no atomic overhead).
  void Increment();
  // Manually refresh the local batch from the coordinator.
  void Reacquire();
  // Drops the current handle and returns unused quota to the coordinator.
  void Free();
  // Initialize the coordinator if it was created via Preload.
  void Initialize(uint64_t limit, uint64_t n_batches = 1);

  static uint64_t WorkersToBatch(uint64_t n_workers) {
    // Used in multi-threaded execution to determine the number of batches for each worker.
    // Generating 4x more batches to lower the possibility of one worker exhausting the quota.
    return n_workers * 4;
  }
};

}  // namespace memgraph::utils
