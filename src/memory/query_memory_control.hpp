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
#include "utils/query_memory_tracker.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::memory {

static constexpr int64_t UNLIMITED_MEMORY{0};

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

  // Find tracker for current thread if exists, track
  // query allocation and procedure allocation if
  // necessary
  void TrackAllocOnCurrentThread(size_t size);

  // Find tracker for current thread if exists, track
  // query allocation and procedure allocation if
  // necessary
  void TrackFreeOnCurrentThread(size_t size);

  void TryCreateTransactionProcTracker(uint64_t, int64_t, size_t);

  void SetActiveProcIdTracker(uint64_t, int64_t);

  void PauseProcedureTracking(uint64_t);

  bool IsThreadTracked();

 private:
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
    utils::QueryMemoryTracker tracker;

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

// Is transaction with given id tracked in memory tracker
bool IsTransactionTracked(uint64_t transaction_id);

// Creates tracker on procedure if doesn't exist. Sets query tracker
// to track procedure with id.
void CreateOrContinueProcedureTracking(uint64_t transaction_id, int64_t procedure_id, size_t limit);

// Pauses procedure tracking. This enables to continue
// tracking on procedure once procedure execution resumes.
void PauseProcedureTracking(uint64_t transaction_id);

}  // namespace memgraph::memory
