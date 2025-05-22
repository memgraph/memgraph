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

#include "utils/rw_spin_lock.hpp"

#include <atomic>
#include <cstddef>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

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
    // Always track even if unlimited
    const auto limit =
        limit_.load(std::memory_order_relaxed);  // Could miss updates to limit, but allowing stale values for now
    auto current = allocated_.load(std::memory_order_acquire);
    while (current + size <= limit) {
      if (allocated_.compare_exchange_weak(current, current + size, std::memory_order_acq_rel)) {
        return true;  // successfully incremented
      }
    }
    return false;  // increment failed
  }

  // Decrementing cannot fail
  void Decrement(T size) {
    auto current = allocated_.load(std::memory_order_acquire);
    while (allocated_.compare_exchange_weak(current, current - size, std::memory_order_acq_rel)) {
    }
  }

  T GetAllocated() const { return allocated_.load(std::memory_order_acquire); }
  T GetLimit() const { return limit_.load(std::memory_order_acquire); }

 private:
  std::atomic<T> allocated_{0};
  std::atomic<T> limit_{kUnlimited};
};

using SessionsResource = Resource<std::size_t>;            // Number of sessions
using TransactionsMemoryResource = Resource<std::size_t>;  // Bytes allowed to be allocated

struct UserResources {
  UserResources() = default;
  UserResources(SessionsResource::value_type sessions_limit,
                TransactionsMemoryResource::value_type transactions_memory_limit)
      : sessions(sessions_limit), transactions_memory(transactions_memory_limit) {}

  void Reset() {
    sessions.UpdateLimit(SessionsResource::kUnlimited);
    transactions_memory.UpdateLimit(TransactionsMemoryResource::kUnlimited);
  }

  void SetSessionLimit(SessionsResource::value_type limit) { sessions.UpdateLimit(limit); }
  bool IncrementSessions() { return sessions.Increment(1); }
  void DecrementSessions() { sessions.Decrement(1); }
  std::pair<SessionsResource::value_type, SessionsResource::value_type> GetSessions() const {
    return {sessions.GetAllocated(), sessions.GetLimit()};
  }

  void SetTransactionsMemoryLimit(TransactionsMemoryResource::value_type limit) {
    transactions_memory.UpdateLimit(limit);
  }
  bool IncrementTransactionsMemory(TransactionsMemoryResource::value_type size) {
    return transactions_memory.Increment(size);
  }
  void DecrementTransactionsMemory(TransactionsMemoryResource::value_type size) { transactions_memory.Decrement(size); }
  std::pair<TransactionsMemoryResource::value_type, TransactionsMemoryResource::value_type> GetTransactionsMemory()
      const {
    return {transactions_memory.GetAllocated(), transactions_memory.GetLimit()};
  }

 private:
  SessionsResource sessions{};
  TransactionsMemoryResource transactions_memory{};
};

class ResourceMonitoring {
 public:
  UserResources &GetUser(const std::string &name) {
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
      auto [it, _] = per_user_resources.try_emplace(name);
      return it->second;
    }
  }

  // TODO Make sure this is fine, if not, we could switch to a shared pointer
  void RemoveUser(const std::string &name) {
    auto lock = std::unique_lock(mtx_);
    per_user_resources.erase(name);
  }

  void UpdateUserLimits(const std::string &name, SessionsResource::value_type sessions_limit,
                        TransactionsMemoryResource::value_type transactions_memory_limit) {
    auto &resource = GetUser(name);
    resource.SetSessionLimit(sessions_limit);
    resource.SetTransactionsMemoryLimit(transactions_memory_limit);
  }

 private:
  // Per user resources
  std::unordered_map<std::string, UserResources> per_user_resources;

  mutable utils::RWSpinLock mtx_;
};

}  // namespace memgraph::utils
