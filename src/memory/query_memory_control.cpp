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

#include <atomic>
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
#include "utils/memory_tracker.hpp"
#include "utils/rw_spin_lock.hpp"

#if USE_JEMALLOC
#include "jemalloc/jemalloc.h"
#endif

namespace memgraph::memory {

#if USE_JEMALLOC

const std::thread::id &get_thread_id() {
  static thread_local std::thread::id this_thread_id{std::this_thread::get_id()};
  return this_thread_id;
}

unsigned QueriesMemoryControl::GetArenaForThread() {
  unsigned thread_arena{0};
  size_t size_thread_arena = sizeof(thread_arena);
  int err = mallctl("thread.arena", &thread_arena, &size_thread_arena, nullptr, 0);
  if (err) {
    LOG_FATAL("Can't get arena for thread.");
  }
  return thread_arena;
}

void QueriesMemoryControl::AddTrackingOnArena(unsigned arena_id) { arena_tracking[arena_id].fetch_add(1); }

void QueriesMemoryControl::RemoveTrackingOnArena(unsigned arena_id) { arena_tracking[arena_id].fetch_sub(1); }

void QueriesMemoryControl::UpdateThreadToTransactionId(const std::thread::id &thread_id, uint64_t transaction_id) {
  auto accessor = thread_id_to_transaction_id.access();
  accessor.insert(ThreadIdToTransactionId{thread_id, transaction_id});
}

void QueriesMemoryControl::EraseThreadToTransactionId(const std::thread::id &thread_id, uint64_t transaction_id) {
  auto accessor = thread_id_to_transaction_id.access();
  auto elem = accessor.find(thread_id);
  MG_ASSERT(elem != accessor.end() && elem->transaction_id == transaction_id);
  accessor.remove(thread_id);
}

utils::MemoryTracker *QueriesMemoryControl::GetTrackerCurrentThread() {
  auto thread_id_to_transaction_id_accessor = thread_id_to_transaction_id.access();

  // we might be just constructing mapping between thread id and transaction id
  // so we miss this allocation
  auto thread_id_to_transaction_id_elem = thread_id_to_transaction_id_accessor.find(std::this_thread::get_id());
  if (thread_id_to_transaction_id_elem == thread_id_to_transaction_id_accessor.end()) {
    return nullptr;
  }

  auto transaction_id_to_tracker_accessor = transaction_id_to_tracker.access();
  auto transaction_id_to_tracker =
      transaction_id_to_tracker_accessor.find(thread_id_to_transaction_id_elem->transaction_id);
  return &*transaction_id_to_tracker->tracker;
}

void QueriesMemoryControl::CreateTransactionIdTracker(uint64_t transaction_id, size_t inital_limit) {
  auto transaction_id_to_tracker_accessor = transaction_id_to_tracker.access();

  TransactionIdToTracker transaction_id_to_tracker_obj{transaction_id, std::make_unique<utils::MemoryTracker>()};
  auto [elem, result] = transaction_id_to_tracker_accessor.insert(std::move(transaction_id_to_tracker_obj));

  elem->tracker->SetMaximumHardLimit(inital_limit);
  elem->tracker->SetHardLimit(inital_limit);
}

bool QueriesMemoryControl::EraseTransactionIdTracker(uint64_t transaction_id) {
  auto transaction_id_to_tracker_accessor = transaction_id_to_tracker.access();
  auto removed = transaction_id_to_tracker.access().remove(transaction_id);
  MG_ASSERT(removed);
  return removed;
}

bool QueriesMemoryControl::IsArenaTracked(unsigned arena_ind) {
  return arena_tracking[arena_ind].load(std::memory_order_relaxed) != 0;
}

void QueriesMemoryControl::InitializeArenaCounter(unsigned arena_ind) {
  arena_tracking[arena_ind].store(0, std::memory_order_relaxed);
}

#endif

void StartTrackingCurrentThreadTransaction(uint64_t transaction_id) {
#if USE_JEMALLOC
  GetQueriesMemoryControl().UpdateThreadToTransactionId(std::this_thread::get_id(), transaction_id);
  GetQueriesMemoryControl().AddTrackingOnArena(QueriesMemoryControl::GetArenaForThread());
#endif
}

void StopTrackingCurrentThreadTransaction(uint64_t transaction_id) {
#if USE_JEMALLOC
  GetQueriesMemoryControl().EraseThreadToTransactionId(std::this_thread::get_id(), transaction_id);
  GetQueriesMemoryControl().RemoveTrackingOnArena(QueriesMemoryControl::GetArenaForThread());
#endif
}

void StartTrackingOnTransaction(uint64_t transaction_id, size_t limit) {
#if USE_JEMALLOC
  GetQueriesMemoryControl().CreateTransactionIdTracker(transaction_id, limit);
  GetQueriesMemoryControl().UpdateThreadToTransactionId(std::this_thread::get_id(), transaction_id);
  GetQueriesMemoryControl().AddTrackingOnArena(QueriesMemoryControl::GetArenaForThread());
#endif
}

void StopTrackingOnTransaction(uint64_t transaction_id) {
#if USE_JEMALLOC
  GetQueriesMemoryControl().RemoveTrackingOnArena(QueriesMemoryControl::GetArenaForThread());
  GetQueriesMemoryControl().EraseThreadToTransactionId(std::this_thread::get_id(), transaction_id);
  GetQueriesMemoryControl().EraseTransactionIdTracker(transaction_id);
#endif
}

}  // namespace memgraph::memory
