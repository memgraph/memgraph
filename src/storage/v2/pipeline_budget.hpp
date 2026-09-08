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
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <vector>

namespace memgraph::storage {

/// Per-database byte budget for the memory a pipelined commit retains outside the commit serializer (its
/// materialized commands and its private WAL buffer). Charging never blocks: a charge that would exceed the
/// maximum is refused, and the committer converts itself into the ordered legacy path instead of waiting.
/// Charges are taken per allocation, so a container's growth holds the old and the new allocation at once: the
/// accounted peak of one buffer is up to three times its final size, and the maximum bounds that peak, not the
/// steady-state size.
class PipelineBudget {
 public:
  explicit PipelineBudget(uint64_t max_bytes) noexcept : max_bytes_{max_bytes} {}

  /// Charges `bytes` if the total stays within the maximum; false (and no change) otherwise. Never blocks.
  [[nodiscard]] bool TryCharge(uint64_t bytes) noexcept {
    auto current = in_flight_.load(std::memory_order_relaxed);
    while (true) {
      if (bytes > max_bytes_ || current > max_bytes_ - bytes) return false;
      if (in_flight_.compare_exchange_weak(
              current, current + bytes, std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return true;
      }
    }
  }

  void Release(uint64_t bytes) noexcept { in_flight_.fetch_sub(bytes, std::memory_order_acq_rel); }

  auto InFlightBytes() const noexcept -> uint64_t { return in_flight_.load(std::memory_order_acquire); }

  auto MaxBytes() const noexcept -> uint64_t { return max_bytes_; }

 private:
  uint64_t max_bytes_;
  std::atomic<uint64_t> in_flight_{0};
};

/// Thrown by a budget-charging allocation when the pipeline budget refuses the charge.
struct PipelineBudgetExceeded : std::exception {
  const char *what() const noexcept override { return "pipelined commit budget exceeded"; }
};

/// RAII charge: the constructor calls TryCharge and throws PipelineBudgetExceeded on refusal; the destructor
/// releases the charge unless ownership was detached. Move-only.
class BudgetCharge {
 public:
  BudgetCharge(PipelineBudget &budget, uint64_t bytes) : budget_{&budget}, bytes_{bytes} {
    if (!budget_->TryCharge(bytes_)) throw PipelineBudgetExceeded{};
  }

  ~BudgetCharge() {
    if (budget_ != nullptr) budget_->Release(bytes_);
  }

  BudgetCharge(BudgetCharge const &) = delete;
  BudgetCharge &operator=(BudgetCharge const &) = delete;

  BudgetCharge(BudgetCharge &&other) noexcept : budget_{other.budget_}, bytes_{other.bytes_} {
    other.budget_ = nullptr;
  }

  BudgetCharge &operator=(BudgetCharge &&other) noexcept {
    if (this != &other) {
      if (budget_ != nullptr) budget_->Release(bytes_);
      budget_ = other.budget_;
      bytes_ = other.bytes_;
      other.budget_ = nullptr;
    }
    return *this;
  }

  /// Gives up ownership of the charge without releasing it; the caller now owes the matching Release.
  void Detach() noexcept { budget_ = nullptr; }

 private:
  PipelineBudget *budget_;
  uint64_t bytes_;
};

/// Test-only refusal injection, consulted by BudgetAllocator::allocate on the first allocation at `site` by the
/// committer whose ticket equals `ticket`. Installed through TxnAllocPolicy::refuse (a pointer the storage fills
/// from the commit probe when one is set; nullptr in production).
struct BudgetRefuse {
  enum Site : uint8_t { kNone, kMaterializer, kEncoder };

  // kThrowRuntimeError is the generic S2 fault for the lifecycle tests.
  enum Mode : uint8_t { kRefuse, kThrowRuntimeError };

  std::atomic<uint64_t> ticket{0};
  std::atomic<Site> site{kNone};
  std::atomic<Mode> mode{kRefuse};
  std::atomic<bool> fired{false};
};

/// Allocation policy handed to everything the encode stage allocates. A null `budget` selects the plain
/// std::allocator with no charging and no refusal (the flag-off path and the ordered legacy continuation).
struct TxnAllocPolicy {
  PipelineBudget *budget{nullptr};
  uint64_t ticket{0};
  BudgetRefuse *refuse{nullptr};
  BudgetRefuse::Site site{BudgetRefuse::kNone};
};

/// Allocator adapter over std::allocator<T>: allocate(n) charges n*sizeof(T) before delegating (throwing
/// PipelineBudgetExceeded on refusal and releasing the charge if the underlying allocation throws); deallocate
/// frees first and then releases exactly once. Rebinding covers unordered-container nodes and buckets. The
/// allocator holds only the policy; a charge belongs to the allocation it covers, so copies and rebinds carry
/// no state of their own.
template <class T>
class BudgetAllocator {
 public:
  using value_type = T;
  using propagate_on_container_move_assignment = std::true_type;
  using propagate_on_container_swap = std::true_type;
  using is_always_equal = std::false_type;

  template <class U>
  struct rebind {
    using other = BudgetAllocator<U>;
  };

  explicit BudgetAllocator(TxnAllocPolicy policy) noexcept : policy_{policy} {}

  template <class U>
  // NOLINTNEXTLINE(google-explicit-constructor)
  BudgetAllocator(BudgetAllocator<U> const &other) noexcept : policy_{other.policy()} {}

  [[nodiscard]] T *allocate(std::size_t n) {
    if (n > std::numeric_limits<std::size_t>::max() / sizeof(T)) throw std::bad_array_new_length{};
    if (policy_.budget == nullptr) return std::allocator<T>{}.allocate(n);
    ConsultRefusal();
    BudgetCharge charge{*policy_.budget, n * sizeof(T)};
    T *memory = std::allocator<T>{}.allocate(n);
    // The charge now belongs to the allocation and is released by deallocate.
    charge.Detach();
    return memory;
  }

  void deallocate(T *memory, std::size_t n) noexcept {
    std::allocator<T>{}.deallocate(memory, n);
    if (policy_.budget != nullptr) policy_.budget->Release(n * sizeof(T));
  }

  auto policy() const noexcept -> TxnAllocPolicy const & { return policy_; }

  template <class U>
  friend bool operator==(BudgetAllocator const &lhs, BudgetAllocator<U> const &rhs) noexcept {
    return lhs.policy_.budget == rhs.policy().budget;
  }

 private:
  void ConsultRefusal() const {
    auto *refuse = policy_.refuse;
    if (refuse == nullptr) return;
    if (refuse->site.load(std::memory_order_acquire) != policy_.site ||
        refuse->ticket.load(std::memory_order_acquire) != policy_.ticket) {
      return;
    }
    if (refuse->fired.exchange(true, std::memory_order_acq_rel)) return;
    if (refuse->mode.load(std::memory_order_acquire) == BudgetRefuse::kThrowRuntimeError) {
      throw std::runtime_error("injected encode-stage failure");
    }
    throw PipelineBudgetExceeded{};
  }

  TxnAllocPolicy policy_;
};

template <class T>
using BudgetVector = std::vector<T, BudgetAllocator<T>>;

}  // namespace memgraph::storage
