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

#include "utils/memory_tracker.hpp"
#include "utils/rw_spin_lock.hpp"

#include <atomic>
#include <cstddef>
#include <limits>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>

namespace memgraph::utils {

template <typename T>
class Resource {
 public:
  using value_type = T;

  Resource() = default;
  explicit Resource(T limit) : limit_(limit) {}
  ~Resource() = default;

  Resource(const Resource &) = delete;
  Resource &operator=(const Resource &) = delete;
  Resource(Resource &&) = delete;
  Resource &operator=(Resource &&) = delete;

  static constexpr auto kUnlimited = std::numeric_limits<T>::max();

  // Should this fail?
  void UpdateLimit(T limit) { limit_.store(limit, std::memory_order_release); }

  bool Increment(T size) {
    // Failure is forbidden, so increment, return true and don't throw
    if (!MemoryTrackerCanThrow()) {
      // NOTE: This is needed because we have paths that block exceptions
      allocated_.fetch_add(size, std::memory_order_acq_rel);
      return true;
    }

    // Always track even if unlimited
    const auto limit =
        limit_.load(std::memory_order_relaxed);  // Could miss updates to limit, but allowing stale values for now
    auto current = allocated_.load(std::memory_order_acquire);
    while (current + size <= limit) {
      if (allocated_.compare_exchange_weak(current, current + size, std::memory_order_acq_rel)) {
        return true;  // successfully incremented
      }
    }
    // register our error data, we will pick this up on the other side of jemalloc
    utils::MemoryErrorStatus().set({static_cast<int64_t>(size), static_cast<int64_t>(current + size),
                                    static_cast<int64_t>(limit), utils::MemoryTrackerStatus::kUser});
    return false;  // increment failed
  }

  // Decrementing cannot fail
  void Decrement(T size) {
    auto current = allocated_.load(std::memory_order_acquire);
    // Underflow protection
    auto gen_next = [&current, size] { return current < size ? T(0) : current - size; };
    while (!allocated_.compare_exchange_weak(current, gen_next(), std::memory_order_acq_rel))
      ;
  }

  T GetCurrent() const { return allocated_.load(std::memory_order_acquire); }
  T GetLimit() const { return limit_.load(std::memory_order_acquire); }

 private:
  std::atomic<T> allocated_{0};
  std::atomic<T> limit_{kUnlimited};
};

using SessionsResource = Resource<size_t>;                    // Number of sessions
class TransactionsMemoryResource : public Resource<size_t> {  // Bytes allowed to be allocated
 public:
  TransactionsMemoryResource() = default;
  explicit TransactionsMemoryResource(size_t limit) : Resource(limit) {}

  bool Allocate(size_t size);

  void Deallocate(size_t size);
};

struct UserResources {
  UserResources() = default;
  UserResources(SessionsResource::value_type sessions_limit,
                TransactionsMemoryResource::value_type transactions_memory_limit)
      : sessions(sessions_limit), transactions_memory(transactions_memory_limit) {}

  void Reset() {
    sessions.UpdateLimit(SessionsResource::kUnlimited);
    transactions_memory.UpdateLimit(TransactionsMemoryResource::kUnlimited);
  }

  // Session limits
  void SetSessionLimit(SessionsResource::value_type limit) { sessions.UpdateLimit(limit); }
  bool IncrementSessions() { return sessions.Increment(1); }
  void DecrementSessions() { sessions.Decrement(1); }
  std::pair<SessionsResource::value_type, SessionsResource::value_type> GetSessions() const {
    return {sessions.GetCurrent(), sessions.GetLimit()};
  }

  // Transactional memory limits
  void SetTransactionsMemoryLimit(TransactionsMemoryResource::value_type limit) {
    transactions_memory.UpdateLimit(limit);
  }
  bool IncrementTransactionsMemory(TransactionsMemoryResource::value_type size) {
    return transactions_memory.Allocate(size);
  }
  void DecrementTransactionsMemory(TransactionsMemoryResource::value_type size) {
    transactions_memory.Deallocate(size);
  }
  std::pair<TransactionsMemoryResource::value_type, TransactionsMemoryResource::value_type> GetTransactionsMemory()
      const {
    return {transactions_memory.GetCurrent(), transactions_memory.GetLimit()};
  }

 private:
  SessionsResource sessions{};
  TransactionsMemoryResource transactions_memory{};
};

class ResourceMonitoring {
 public:
  std::shared_ptr<UserResources> GetUser(const std::string &name) {
    // Phase 1: try to find with shared access
    {
      auto lock = std::shared_lock(mtx_);
      auto it = per_user_resources.find(name);
      if (it != per_user_resources.end()) {
        return it->second;
      }
    }
    // Phase 2: get unique access and create if not found
    {
      auto lock = std::unique_lock(mtx_);
      auto [it, _] = per_user_resources.try_emplace(name, std::make_shared<UserResources>());
      return it->second;
    }
  }

  void RemoveUser(const std::string &name) {
    // Resource is passed as a shared_ptr; anything using it should be safe
    auto lock = std::unique_lock(mtx_);
    per_user_resources.erase(name);
  }

  void UpdateUserLimits(const std::string &name, SessionsResource::value_type sessions_limit,
                        TransactionsMemoryResource::value_type transactions_memory_limit) {
    auto resource = GetUser(name);
    resource->SetSessionLimit(sessions_limit);
    resource->SetTransactionsMemoryLimit(transactions_memory_limit);
  }

 private:
  // Per user resources
  std::unordered_map<std::string, std::shared_ptr<UserResources>> per_user_resources;

  mutable utils::RWSpinLock mtx_;
};

}  // namespace memgraph::utils
