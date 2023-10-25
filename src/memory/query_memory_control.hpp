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
#include "utils/skip_list.hpp"

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
  /*
  Arena stats
  */

  static unsigned GetArenaForThread();

  // Add counter on threads allocating inside arena
  void AddTrackingOnArena(unsigned);

  // Remove counter on threads allocating in arena
  void RemoveTrackingOnArena(unsigned);

  // Are any threads using current arena for allocations
  // Multiple threads can allocate inside one arena
  bool IsArenaTracked(unsigned);

  // Initialize arena counter
  void InitializeArenaCounter(unsigned);

  /*
    Transaction id <-> tracker
  */

  // Create new tracker for transaction_id with initial limit
  void CreateTransactionIdTracker(uint64_t, size_t);

  // Check if tracker for given transaction id exists
  bool CheckTransactionIdTrackerExists(uint64_t);

  // Remove current tracker for transaction_id
  bool EraseTransactionIdTracker(uint64_t);

  /*
  Thread handlings
  */

  // Map thread to transaction with given id
  // This way we can know which thread belongs to which transaction
  // and get correct tracker for given transaction
  void UpdateThreadToTransactionId(const std::thread::id &, uint64_t);

  // Remove tracking of thread from transaction.
  // Important to reset if one thread gets reused for different transaction
  void EraseThreadToTransactionId(const std::thread::id &, uint64_t);

  // C-API functionality for thread to transaction mapping
  void UpdateThreadToTransactionId(const char *, uint64_t);

  // C-API functionality for thread to transaction unmapping
  void EraseThreadToTransactionId(const char *, uint64_t);

  // Get tracker to current thread if exists, otherwise return
  // nullptr. This can happen only if tracker is still
  // being constructed.
  utils::MemoryTracker *GetTrackerCurrentThread();

 private:
  std::unordered_map<unsigned, std::atomic<int>> arena_tracking;

  struct ThreadIdToTransactionId {
    std::thread::id thread_id;
    uint64_t transaction_id;

    bool operator<(const ThreadIdToTransactionId &other) const { return thread_id < other.thread_id; }
    bool operator==(const ThreadIdToTransactionId &other) const { return thread_id == other.thread_id; }

    bool operator<(const std::thread::id other) const { return thread_id < other; }
    bool operator==(const std::thread::id other) const { return thread_id == other; }
  };

  struct TransactionIdToTracker {
    uint64_t transaction_id;
    utils::MemoryTracker tracker;

    bool operator<(const TransactionIdToTracker &other) const { return transaction_id < other.transaction_id; }
    bool operator==(const TransactionIdToTracker &other) const { return transaction_id == other.transaction_id; }

    bool operator<(uint64_t other) const { return transaction_id < other; }
    bool operator==(uint64_t other) const { return transaction_id == other; }
  };

  utils::SkipList<ThreadIdToTransactionId> thread_id_to_transaction_id;
  utils::SkipList<TransactionIdToTracker> transaction_id_to_tracker;
};

inline QueriesMemoryControl &GetQueriesMemoryControl() {
  static QueriesMemoryControl queries_memory_control_;
  return queries_memory_control_;
}

#endif

// API function call for to start tracking current thread for given transaction.
// Does nothing if jemalloc is not enabled
void StartTrackingCurrentThreadTransaction(uint64_t transaction_id);

// API function call for to stop tracking current thread for given transaction.
// Does nothing if jemalloc is not enabled
void StopTrackingCurrentThreadTransaction(uint64_t transaction_id);

// API function call for try to create tracker for transaction and set it to given limit.
// Does nothing if jemalloc is not enabled. Does nothing if tracker already exists
void TryStartTrackingOnTransaction(uint64_t transaction_id, size_t limit);

// API function call to stop tracking for given transaction.
// Does nothing if jemalloc is not enabled. Does nothing if tracker doesn't exist
void TryStopTrackingOnTransaction(uint64_t transaction_id);

}  // namespace memgraph::memory
