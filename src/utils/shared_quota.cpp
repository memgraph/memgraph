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

#include "utils/shared_quota.hpp"

#include <algorithm>
#include <utility>
#include "utils/logging.hpp"

namespace memgraph::utils {

// --- QuotaCoordinator Implementation ---

QuotaCoordinator::QuotaCoordinator(uint64_t total_limit)
    : remaining_quota_(total_limit), active_handlers_(0), initialized_(true) {}

void QuotaCoordinator::Initialize(uint64_t limit) {
  // Use acq_rel to establish a barrier so the store is not visible before the flag.
  const auto prev = initialized_.fetch_or(true, std::memory_order_acq_rel);
  if (prev) return;
  // Release store ensures Acquire() sees this value after reading the flag.
  remaining_quota_.store(limit, std::memory_order_release);
}

std::optional<QuotaCoordinator::QuotaHandle> QuotaCoordinator::Acquire(uint64_t desired_batch_size) {
  while (true) {
    uint64_t current = remaining_quota_.load(std::memory_order_acquire);

    if (current > 0) {
      // Adaptive batch sizing logic
      uint64_t actual_batch = desired_batch_size;
      if (current < desired_batch_size) {
        actual_batch = std::max<uint64_t>(1, current / 2);
      }
      // Take from quota
      if (remaining_quota_.compare_exchange_weak(
              current, current - actual_batch, std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return QuotaHandle(this, actual_batch);
      }
      continue;  // Retry if CAS failed
    }

    // Quota is empty. Snapshot epoch with Acquire before checking termination.
    const uint32_t epoch = epoch_.load(std::memory_order_acquire);

    // Termination logic: No quota left AND we are the only one processing a batch.
    if (active_handlers_.load(std::memory_order_acquire) == 0) {
      // Final Acquire check to ensure we didn't miss a return.
      if (remaining_quota_.load(std::memory_order_acquire) <= 0) {
        return std::nullopt;
      }
      // If quota > 0 now, loop again to try and grab it
      continue;
    }

    // Wait for a NotifyWaiters call.
    epoch_.wait(epoch, std::memory_order_relaxed);
  }
}

void QuotaCoordinator::NotifyWaiters() {
  epoch_.fetch_add(1, std::memory_order_release);
  epoch_.notify_all();
}

// --- QuotaHandle Implementation ---

QuotaCoordinator::QuotaHandle::QuotaHandle(QuotaCoordinator *coord, uint64_t amount) : coord_(coord), amount_(amount) {
  MG_ASSERT(coord_, "QuotaCoordinator cannot be nullptr");
  coord_->active_handlers_.fetch_add(1, std::memory_order_acq_rel);
}

QuotaCoordinator::QuotaHandle::QuotaHandle(QuotaHandle &&other) noexcept
    : coord_(std::exchange(other.coord_, nullptr)), amount_(std::exchange(other.amount_, 0)) {}

QuotaCoordinator::QuotaHandle &QuotaCoordinator::QuotaHandle::operator=(QuotaHandle &&other) noexcept {
  if (this != &other) {
    ReturnUnused();  // Ensure existing local quota is returned before overwriting.
    coord_ = std::exchange(other.coord_, nullptr);
    amount_ = std::exchange(other.amount_, 0);
  }
  return *this;
}

uint64_t QuotaCoordinator::QuotaHandle::Consume(uint64_t amount) {
  // Consume from local batch
  auto consumed = std::min(amount, amount_);
  amount_ -= consumed;
  return consumed;
}

void QuotaCoordinator::QuotaHandle::Increment(uint64_t amount) {
  // NOTE: Does not protect against over incrementing.
  amount_ += amount;
}

void QuotaCoordinator::QuotaHandle::ReturnUnused() {
  if (coord_) {
    // Return quota to coordinator
    if (amount_ > 0) coord_->remaining_quota_.fetch_add(amount_, std::memory_order_release);
    // Decrement active handlers
    auto active_handlers = coord_->active_handlers_.fetch_sub(1, std::memory_order_acq_rel) - 1;
    // State changed!
    // 1. Quota increased (Waiters care about this)
    // 2. Holders decreased (Waiters might care if it hit 0)
    if (amount_ > 0 || active_handlers == 0) coord_->NotifyWaiters();

    // Handle is now invalid
    coord_ = nullptr;
    amount_ = 0;
  }
}

// --- SharedQuota Implementation ---

SharedQuota::SharedQuota(uint64_t limit, uint64_t n_batches)
    : coord_(std::make_shared<QuotaCoordinator>(limit)),
      desired_batch_size_(std::max<uint64_t>(1, limit / n_batches)),
      handle_(coord_->Acquire(desired_batch_size_)) {
  MG_ASSERT(n_batches > 0, "Number of batches has to be greater than 0");
}

SharedQuota::SharedQuota(Preload)
    : coord_(std::make_shared<QuotaCoordinator>()), desired_batch_size_(0), handle_(std::nullopt) {}

SharedQuota::SharedQuota(const SharedQuota &other) noexcept
    : coord_(other.coord_),
      desired_batch_size_(other.desired_batch_size_),
      handle_(std::nullopt) {}  // Handle is acquired lazily on first Decrement.

SharedQuota &SharedQuota::operator=(const SharedQuota &other) noexcept {
  if (this != &other) {
    handle_.reset();
    coord_ = other.coord_;
    desired_batch_size_ = other.desired_batch_size_;
    // Do not acquire a new handle here, it will lazily acquire on first Decrement.
  }
  return *this;
}

SharedQuota::SharedQuota(SharedQuota &&other) noexcept
    : coord_(std::exchange(other.coord_, nullptr)),
      desired_batch_size_(std::exchange(other.desired_batch_size_, 0)),
      handle_(std::exchange(other.handle_, std::nullopt)) {}

SharedQuota &SharedQuota::operator=(SharedQuota &&other) noexcept {
  if (this != &other) {
    handle_.reset();
    coord_ = std::exchange(other.coord_, nullptr);
    desired_batch_size_ = std::exchange(other.desired_batch_size_, 0);
    handle_ = std::exchange(other.handle_, std::nullopt);
  }
  return *this;
}

uint64_t SharedQuota::Decrement(uint64_t amount) {
  if (!coord_) return 0;

  if (!handle_) {
    Reacquire();
    if (!handle_) return 0;
  }

  uint64_t total_consumed = 0;
  while (amount > 0) {
    auto consumed = handle_->Consume(amount);
    total_consumed += consumed;
    amount -= consumed;

    if (amount > 0) {
      Reacquire();
      if (!handle_) break;  // No more quota available globally.
    }
  }
  return total_consumed;
}

void SharedQuota::Increment() {
  if (handle_) handle_->Increment(1U);
}

void SharedQuota::Reacquire() {
  handle_.reset();  // Returns existing unused quota before grabbing a new batch.
  if (coord_) handle_ = coord_->Acquire(desired_batch_size_);
}

void SharedQuota::Free() { handle_.reset(); }

void SharedQuota::Initialize(uint64_t limit, uint64_t n_batches) {
  if (desired_batch_size_ != 0) return;
  desired_batch_size_ = std::max<uint64_t>(1, limit / n_batches);
  if (coord_) coord_->Initialize(limit);
}

}  // namespace memgraph::utils
