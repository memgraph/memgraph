// Copyright 2025 Memgraph Ltd.
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
#include <memory>
#include <optional>

namespace memgraph::utils {

class QuotaCoordinator {
  std::atomic<int64_t> remaining_quota_{0};
  std::atomic<int> active_holders_{0};
  std::atomic<uint32_t> epoch_{0};  // Incremented whenever the state changes in a way waiters care about.
  std::atomic<uint8_t> initialized_{false};

 public:
  explicit QuotaCoordinator(int64_t total_limit);
  QuotaCoordinator() = default;

  class QuotaHandle {
    QuotaCoordinator *coord_;
    int64_t amount_;

   public:
    QuotaHandle(QuotaCoordinator *coord, int64_t amount);
    ~QuotaHandle() { ReturnUnused(); }

    QuotaHandle(const QuotaHandle &) = delete;
    QuotaHandle &operator=(const QuotaHandle &) = delete;
    QuotaHandle(QuotaHandle &&other) noexcept;
    QuotaHandle &operator=(QuotaHandle &&other) noexcept;

    int64_t Count() const { return amount_; }
    // Returns the amount that was actually consumed.
    int64_t Consume(int64_t amount);
    void ReturnUnused();
  };

  void Initialize(int64_t limit);
  std::optional<QuotaHandle> Acquire(int64_t desired_batch_size);

 private:
  friend class QuotaHandle;
  void ReturnQuota(int64_t amount);
  void NotifyWaiters();
};

class SharedQuota {
  std::shared_ptr<QuotaCoordinator> coord_{nullptr};
  int64_t desired_batch_size_{0};
  std::optional<QuotaCoordinator::QuotaHandle> handle_{std::nullopt};

 public:
  constexpr static struct Preload {
  } preload;

  explicit SharedQuota(int64_t limit, int64_t n_batches = 1);
  // Used to setup the objects, but not initialize the quota.
  explicit SharedQuota(Preload /*unused*/);
  ~SharedQuota() = default;

  SharedQuota(const SharedQuota &other) noexcept;
  SharedQuota &operator=(const SharedQuota &other) noexcept;
  SharedQuota(SharedQuota &&other) noexcept;
  SharedQuota &operator=(SharedQuota &&other) noexcept;

  // Returns true if the quota was successfully incremented.
  // Generally, it returns true if the quota was incremented by the requested amount.
  // However, if the quota is exhausted, it returns false (and the quota was not incremented or partially incremented).
  // The caller should handle the case where the quota is exhausted.
  // BUT the previous implementation returned bool, so to keep it compatible with the previous implementation
  // we will change it to return the amount incremented.
  int64_t Increment(int64_t amount = 1);
  void Reacquire();
  // Useful for multi-threaded exeucitons where each thread needs to free its left quota.
  void Free();
  // Initialize the quota coordinator when preloaded and used by multiple threads.
  void Initialize(int64_t limit, int64_t n_batches = 1);
};

}  // namespace memgraph::utils
