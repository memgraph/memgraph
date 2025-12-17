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

#include "utils/shared_quota.hpp"

#include <utility>
#include "utils/logging.hpp"

namespace memgraph::utils {
QuotaCoordinator::QuotaHandle::QuotaHandle(QuotaCoordinator *coord, int64_t amount) : coord_(coord), amount_(amount) {
  MG_ASSERT(coord_, "QuotaCoordinator cannot be nullptr");
  coord_->active_holders_.fetch_add(1, std::memory_order_acquire);
}

QuotaCoordinator::QuotaHandle::QuotaHandle(QuotaHandle &&other) noexcept
    : coord_(std::exchange(other.coord_, nullptr)), amount_(std::exchange(other.amount_, 0)) {}

QuotaCoordinator::QuotaHandle &QuotaCoordinator::QuotaHandle::operator=(QuotaHandle &&other) noexcept {
  if (this != &other) {
    ReturnUnused();
    coord_ = std::exchange(other.coord_, nullptr);
    amount_ = std::exchange(other.amount_, 0);
  }
  return *this;
}

int64_t QuotaCoordinator::QuotaHandle::Consume(int64_t amount) {
  // If local quota is available, consume it
  auto consumed = std::min(amount, amount_);
  amount_ -= consumed;
  return consumed;
}

void QuotaCoordinator::QuotaHandle::Increment(int64_t amount) { amount_ += amount; }

void QuotaCoordinator::QuotaHandle::ReturnUnused() {
  if (coord_) coord_->ReturnQuota(amount_);
}

QuotaCoordinator::QuotaCoordinator(int64_t total_limit)
    : remaining_quota_(total_limit), active_holders_(0), initialized_(true) {}

std::optional<QuotaCoordinator::QuotaHandle> QuotaCoordinator::Acquire(int64_t desired_batch_size) {
  while (true) {
    int64_t current = remaining_quota_.load(std::memory_order_relaxed);
    // If quota is available, consume it
    if (current > 0) {
      // Adaptive batch sizing logic
      int64_t actual_batch = desired_batch_size;
      if (current < desired_batch_size) {
        actual_batch = std::max<int64_t>(1, current / 2);
      }
      // Take from quota
      if (remaining_quota_.compare_exchange_weak(current, current - actual_batch, std::memory_order_acquire,
                                                 std::memory_order_relaxed)) {
        return QuotaHandle(this, actual_batch);
      }
      continue;  // CAS failed, retry
    }
    // Quota is 0. Check completion or Wait.
    // 1. Snapshot the epoch BEFORE checking conditions.
    uint32_t epoch = epoch_.load(std::memory_order_acquire);
    // 2. Check termination condition
    if (active_holders_.load(std::memory_order_acquire) == 0) {
      // Re-read quota to ensure we didn't miss a last-second return
      if (remaining_quota_.load(std::memory_order_relaxed) <= 0) {
        return std::nullopt;  // Global Done
      }
      // If quota > 0 now, loop again to try and grab it
      continue;
    }
    // 3. Wait on the epoch, not the quota.
    // If the generation has changed since our snapshot (step 1), wait returns immediately.
    // If the generation changes while we sleep, we wake up.
    epoch_.wait(epoch, std::memory_order_relaxed);
  }
}

void QuotaCoordinator::ReturnQuota(int64_t amount) {
  // Increase quota
  if (amount > 0) remaining_quota_.fetch_add(amount, std::memory_order_release);
  // Decrement holders
  auto prev = active_holders_.fetch_sub(1, std::memory_order_release);
  // State changed!
  // 1. Quota increased (Waiters care about this)
  // 2. Holders decreased (Waiters might care if it hit 0)
  if (amount > 0 || prev == 1) NotifyWaiters();
}

void QuotaCoordinator::NotifyWaiters() {
  // Increment generation so wait() sees a value change
  epoch_.fetch_add(1, std::memory_order_release);
  epoch_.notify_all();
}

SharedQuota::SharedQuota(int64_t limit, int64_t n_batches)
    : coord_(std::make_shared<QuotaCoordinator>(limit)),
      desired_batch_size_(std::max<int64_t>(1, limit / n_batches)),
      handle_(coord_->Acquire(desired_batch_size_)) {
  MG_ASSERT(n_batches > 0, "Number of batches has to be greater than 0");
}

SharedQuota::SharedQuota(const SharedQuota &other) noexcept
    : coord_(other.coord_),
      desired_batch_size_(other.desired_batch_size_),
      // Do not acquire a new handle here, it will be acquired when the operator++ is called.
      handle_(std::nullopt) {}

SharedQuota &SharedQuota::operator=(const SharedQuota &other) noexcept {
  if (this != &other) {
    handle_.reset();
    coord_ = other.coord_;
    desired_batch_size_ = other.desired_batch_size_;
    // Do not acquire a new handle here, it will be acquired when the operator++ is called.
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

int64_t SharedQuota::Decrement(int64_t amount) {
  // coord_ is not set after a move
  if (!coord_) {
    return 0;
  }

  // handle_ is not set after a copy or Free
  if (!handle_) {
    Reacquire();
    // failed to acquire a quota
    if (!handle_) return 0;
  }

  int64_t total_consumed = 0;
  while (amount > 0) {
    // try to consume local quota
    auto consumed = handle_->Consume(amount);
    total_consumed += consumed;
    amount -= consumed;

    if (amount > 0) {
      // try to acquire more quota
      Reacquire();
      // If we couldn't get a new handle, we are done
      if (!handle_) break;
    }
  }
  return total_consumed;
}

void SharedQuota::Increment(int64_t amount) {
  if (!handle_) {
    Reacquire();
    // Silently fail if we failed to acquire a quota
    if (!handle_) return;
  }
  handle_->Increment(amount);
}

void SharedQuota::Reacquire() {
  // handler_ has to be reset before acquiring a new one
  handle_.reset();
  if (coord_) handle_ = coord_->Acquire(desired_batch_size_);
}

void SharedQuota::Free() { handle_.reset(); }

void SharedQuota::Initialize(int64_t limit, int64_t n_batches) {
  // Initialize only if not created via Preload
  if (desired_batch_size_ != -1) return;
  desired_batch_size_ = std::max<int64_t>(1, limit / n_batches);
  if (coord_) coord_->Initialize(limit);
}
void QuotaCoordinator::Initialize(int64_t limit) {
  const auto prev = initialized_.fetch_or(true);
  if (prev) return;  // Already initialized
  remaining_quota_ = limit;
}
SharedQuota::SharedQuota(Preload /*unused*/)
    : coord_(std::make_shared<QuotaCoordinator>()), desired_batch_size_(-1), handle_(std::nullopt) {}
}  // namespace memgraph::utils
