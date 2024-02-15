// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <atomic>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <optional>
#include <shared_mutex>
#include <thread>
#include <tuple>
#include <utility>

#include "query_memory_control.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/rw_spin_lock.hpp"

#if USE_JEMALLOC
#include "jemalloc/jemalloc.h"
#endif

namespace memgraph::memory {

#if USE_JEMALLOC

void QueriesMemoryControl::UpdateThreadToTransactionId(const std::thread::id &thread_id, uint64_t transaction_id) {
  auto accessor = thread_id_to_transaction_id.access();
  auto elem = accessor.find(thread_id);
  if (elem == accessor.end()) {
    accessor.insert({thread_id, {transaction_id, 1}});
  } else {
    elem->transaction_id.cnt++;
  }
}

void QueriesMemoryControl::EraseThreadToTransactionId(const std::thread::id &thread_id, uint64_t transaction_id) {
  auto accessor = thread_id_to_transaction_id.access();
  auto elem = accessor.find(thread_id);
  MG_ASSERT(elem != accessor.end() && elem->transaction_id == transaction_id);
  elem->transaction_id.cnt--;
  if (elem->transaction_id.cnt == 0) {
    accessor.remove(thread_id);
  }
}

void QueriesMemoryControl::TrackAllocOnCurrentThread(size_t size) {
  auto thread_id_to_transaction_id_accessor = thread_id_to_transaction_id.access();

  // we might be just constructing mapping between thread id and transaction id
  // so we miss this allocation
  auto thread_id_to_transaction_id_elem = thread_id_to_transaction_id_accessor.find(std::this_thread::get_id());
  if (thread_id_to_transaction_id_elem == thread_id_to_transaction_id_accessor.end()) {
    return;
  }

  auto transaction_id_to_tracker_accessor = transaction_id_to_tracker.access();
  auto transaction_id_to_tracker =
      transaction_id_to_tracker_accessor.find(thread_id_to_transaction_id_elem->transaction_id);

  // It can happen that some allocation happens between mapping thread to
  // transaction id, so we miss this allocation
  if (transaction_id_to_tracker == transaction_id_to_tracker_accessor.end()) [[unlikely]] {
    return;
  }
  auto &query_tracker = transaction_id_to_tracker->tracker;
  query_tracker.TrackAlloc(size);
}

void QueriesMemoryControl::TrackFreeOnCurrentThread(size_t size) {
  auto thread_id_to_transaction_id_accessor = thread_id_to_transaction_id.access();

  // we might be just constructing mapping between thread id and transaction id
  // so we miss this allocation
  auto thread_id_to_transaction_id_elem = thread_id_to_transaction_id_accessor.find(std::this_thread::get_id());
  if (thread_id_to_transaction_id_elem == thread_id_to_transaction_id_accessor.end()) {
    return;
  }

  auto transaction_id_to_tracker_accessor = transaction_id_to_tracker.access();
  auto transaction_id_to_tracker =
      transaction_id_to_tracker_accessor.find(thread_id_to_transaction_id_elem->transaction_id);

  // It can happen that some allocation happens between mapping thread to
  // transaction id, so we miss this allocation
  if (transaction_id_to_tracker == transaction_id_to_tracker_accessor.end()) [[unlikely]] {
    return;
  }
  auto &query_tracker = transaction_id_to_tracker->tracker;
  query_tracker.TrackFree(size);
}

void QueriesMemoryControl::CreateTransactionIdTracker(uint64_t transaction_id, size_t inital_limit) {
  auto transaction_id_to_tracker_accessor = transaction_id_to_tracker.access();

  auto [elem, result] = transaction_id_to_tracker_accessor.insert({transaction_id, utils::QueryMemoryTracker{}});

  elem->tracker.SetQueryLimit(inital_limit);
}

bool QueriesMemoryControl::EraseTransactionIdTracker(uint64_t transaction_id) {
  auto transaction_id_to_tracker_accessor = transaction_id_to_tracker.access();
  auto removed = transaction_id_to_tracker_accessor.remove(transaction_id);
  return removed;
}

bool QueriesMemoryControl::CheckTransactionIdTrackerExists(uint64_t transaction_id) {
  auto transaction_id_to_tracker_accessor = transaction_id_to_tracker.access();
  return transaction_id_to_tracker_accessor.contains(transaction_id);
}

void QueriesMemoryControl::TryCreateTransactionProcTracker(uint64_t transaction_id, int64_t procedure_id,
                                                           size_t limit) {
  auto transaction_id_to_tracker_accessor = transaction_id_to_tracker.access();
  auto query_tracker = transaction_id_to_tracker_accessor.find(transaction_id);

  if (query_tracker == transaction_id_to_tracker_accessor.end()) {
    return;
  }

  query_tracker->tracker.TryCreateProcTracker(procedure_id, limit);
}

void QueriesMemoryControl::SetActiveProcIdTracker(uint64_t transaction_id, int64_t procedure_id) {
  auto transaction_id_to_tracker_accessor = transaction_id_to_tracker.access();
  auto query_tracker = transaction_id_to_tracker_accessor.find(transaction_id);

  if (query_tracker == transaction_id_to_tracker_accessor.end()) {
    return;
  }

  query_tracker->tracker.SetActiveProc(procedure_id);
}

void QueriesMemoryControl::PauseProcedureTracking(uint64_t transaction_id) {
  auto transaction_id_to_tracker_accessor = transaction_id_to_tracker.access();
  auto query_tracker = transaction_id_to_tracker_accessor.find(transaction_id);

  if (query_tracker == transaction_id_to_tracker_accessor.end()) {
    return;
  }

  query_tracker->tracker.StopProcTracking();
}

inline int &Get_Thread_Tracker() {
  // store variable in bss segment for each thread
  // https://cs-fundamentals.com/c-programming/memory-layout-of-c-program-code-data-segments#size-of-code-data-bss-segments
  static thread_local int is_thread_tracked{0};
  return is_thread_tracked;
}

bool QueriesMemoryControl::IsThreadTracked() { return Get_Thread_Tracker() == 1; }

#endif

void StartTrackingCurrentThreadTransaction(uint64_t transaction_id) {
#if USE_JEMALLOC
  Get_Thread_Tracker() = 0;
  GetQueriesMemoryControl().UpdateThreadToTransactionId(std::this_thread::get_id(), transaction_id);
  Get_Thread_Tracker() = 1;
#endif
}

void StopTrackingCurrentThreadTransaction(uint64_t transaction_id) {
#if USE_JEMALLOC
  Get_Thread_Tracker() = 0;
  GetQueriesMemoryControl().EraseThreadToTransactionId(std::this_thread::get_id(), transaction_id);
#endif
}

void TryStartTrackingOnTransaction(uint64_t transaction_id, size_t limit) {
#if USE_JEMALLOC
  if (GetQueriesMemoryControl().CheckTransactionIdTrackerExists(transaction_id)) {
    return;
  }
  GetQueriesMemoryControl().CreateTransactionIdTracker(transaction_id, limit);

#endif
}

void TryStopTrackingOnTransaction(uint64_t transaction_id) {
#if USE_JEMALLOC
  if (!GetQueriesMemoryControl().CheckTransactionIdTrackerExists(transaction_id)) {
    return;
  }
  GetQueriesMemoryControl().EraseTransactionIdTracker(transaction_id);
#endif
}

#if USE_JEMALLOC
bool IsTransactionTracked(uint64_t transaction_id) {
  return GetQueriesMemoryControl().CheckTransactionIdTrackerExists(transaction_id);
}
#else
bool IsTransactionTracked(uint64_t /*transaction_id*/) { return false; }
#endif

void CreateOrContinueProcedureTracking(uint64_t transaction_id, int64_t procedure_id, size_t limit) {
#if USE_JEMALLOC
  if (!GetQueriesMemoryControl().CheckTransactionIdTrackerExists(transaction_id)) {
    LOG_FATAL("Memory tracker for transaction was not set");
  }

  GetQueriesMemoryControl().TryCreateTransactionProcTracker(transaction_id, procedure_id, limit);
  GetQueriesMemoryControl().SetActiveProcIdTracker(transaction_id, procedure_id);
#endif
}

void PauseProcedureTracking(uint64_t transaction_id) {
#if USE_JEMALLOC
  GetQueriesMemoryControl().PauseProcedureTracking(transaction_id);
#endif
}

}  // namespace memgraph::memory
