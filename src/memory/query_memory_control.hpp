// Copyright 2023 Memgraph Ltd.
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

#include <cstddef>
#include <cstdint>
#include <thread>
#include <unordered_map>

#include "utils/memory_tracker.hpp"
#include "utils/rw_spin_lock.hpp"

namespace memgraph::memory {

#if USE_JEMALLOC

// Track memory allocations per query.
// Multiple threads can allocate inside one transaction.
// If user forgets to unregister tracking for that thread before it dies, it will continue to
// track allocations for that arena indefinitely.
// As multiple queries can be executed inside one transaction, one by one (multi-transaction)
// it is necessary to restart tracking at the beginning of new query for that transaction.
class QueriesMemoryControl {
 public:
  static unsigned GetArenaForThread();

  // Add counter on threads allocating inside arena
  void AddTrackingOnArena(unsigned);

  // Remove counter on threads allocating in arena
  void RemoveTrackingOnArena(unsigned);

  // Are any threads using current arena for allocations
  // Multiple threads can allocate inside one arena
  bool IsArenaTracked(unsigned);

  // Get arena counter
  std::atomic<int> &GetArenaCounter(unsigned);

  // Map thread to transaction with given id
  // This way we can know which thread belongs to which transaction
  // and get correct tracker for given transaction
  void UpdateThreadToTransactionId(const std::thread::id &, uint64_t);

  // Remove tracking of thread from transaction.
  // Important to reset if one thread gets reused for different transaction
  void ResetThreadToTransactionId(const std::thread::id &);

  // C-API functionality for thread to transaction mapping
  void UpdateThreadToTransactionId(const char *, uint64_t);

  // C-API functionality for thread to transaction unmapping
  void ResetThreadToTransactionId(const char *);

  // Create new tracker for transaction_id
  utils::MemoryTracker &CreateTransactionIdTracker(uint64_t);

  // Remove current tracker for transaction_id
  bool EraseTransactionIdTracker(uint64_t);

  // Add tracker for current thread on given transaction id
  void AddTrackingsOnCurrentThread(uint64_t);

  // Remove tracking for current thread
  void RemoveTrackingsOnCurrentThread();

  utils::MemoryTracker &GetTrackerCurrentThread();

 private:
  std::unordered_map<unsigned, std::atomic<int>> arena_tracking;

  mutable utils::RWSpinLock lock;
  std::unordered_map<std::string, uint64_t> thread_id_to_transaction_id;
  std::unordered_map<uint64_t, utils::MemoryTracker> transaction_id_tracker;
};

inline QueriesMemoryControl &GetQueriesMemoryControl() {
  static QueriesMemoryControl queries_memory_control_;
  return queries_memory_control_;
}

namespace query {}  // namespace query

#endif

// API function call for to start tracking current thread for given transaction.
// Does nothing if jemalloc is not enabled
void StartTrackingCurrentThreadTransaction(uint64_t transaction_id);

// API function call for to stop tracking current thread for given transaction.
// Does nothing if jemalloc is not enabled
void StopTrackingCurrentThreadTransaction(uint64_t transaction_id);

// API function call for to start tracking given thread for given transaction.
// Does nothing if jemalloc is not enabled.
void StartTrackingThreadTransaction(const char *thread_id, uint64_t transaction_id);

// API function call for to stop tracking given thread for given transaction.
// Does nothing if jemalloc is not enabled
void StopTrackingThreadTransaction(const char *thread_id, uint64_t transaction_id);

// API function call for to create tracker for transaction and set it to given limit.
// Does nothing if jemalloc is not enabled
void StartTrackingOnTransaction(uint64_t transaction_id, size_t limit);

// API function call to stop tracking for given transaction.
// Does nothing if jemalloc is not enabled
void StopTrackingOnTransaction(uint64_t transaction_id);

}  // namespace memgraph::memory
