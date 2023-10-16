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
#include <shared_mutex>
#include <thread>

#include "query_memory_control.hpp"
#include "utils.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/rw_spin_lock.hpp"

#if USE_JEMALLOC
#include "jemalloc/jemalloc.h"
#endif

namespace memgraph::memory {

#if USE_JEMALLOC

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
  std::unique_lock<utils::RWSpinLock> guard{lock};
  thread_id_to_transaction_id[get_string_thread_id(thread_id)] = transaction_id;
}

void QueriesMemoryControl::UpdateThreadToTransactionId(const char *thread_id, uint64_t transaction_id) {
  std::unique_lock<utils::RWSpinLock> guard{lock};
  thread_id_to_transaction_id[std::string(thread_id)] = transaction_id;
}

void QueriesMemoryControl::ResetThreadToTransactionId(const std::thread::id &thread_id) {
  std::unique_lock<utils::RWSpinLock> guard{lock};
  thread_id_to_transaction_id.erase(get_string_thread_id(thread_id));
}

void QueriesMemoryControl::ResetThreadToTransactionId(const char *thread_id) {
  std::unique_lock<utils::RWSpinLock> guard{lock};
  thread_id_to_transaction_id.erase(std::string(thread_id));
}

void QueriesMemoryControl::AddTrackingsOnCurrentThread(uint64_t transaction_id) {
  UpdateThreadToTransactionId(std::this_thread::get_id(), transaction_id);
  AddTrackingOnArena(GetArenaForThread());
}

void QueriesMemoryControl::RemoveTrackingsOnCurrentThread() {
  ResetThreadToTransactionId(std::this_thread::get_id());
  RemoveTrackingOnArena(GetArenaForThread());
}

utils::MemoryTracker &QueriesMemoryControl::GetTrackerCurrentThread() {
  std::shared_lock<utils::RWSpinLock> guard{lock};
  return transaction_id_tracker[thread_id_to_transaction_id[get_thread_id()]];
}

utils::MemoryTracker &QueriesMemoryControl::CreateTransactionIdTracker(uint64_t transaction_id) {
  std::unique_lock<utils::RWSpinLock> guard{lock};
  transaction_id_tracker.emplace(std::piecewise_construct, std::forward_as_tuple(transaction_id),
                                 std::forward_as_tuple());
  return transaction_id_tracker[transaction_id];
}

bool QueriesMemoryControl::EraseTransactionIdTracker(uint64_t transaction_id) {
  std::unique_lock<utils::RWSpinLock> guard{lock};
  transaction_id_tracker.erase(transaction_id);
  return true;
}

bool QueriesMemoryControl::IsArenaTracked(unsigned arena_ind) {
  return arena_tracking[arena_ind].load(std::memory_order_relaxed) != 0;
}

std::atomic<int> &QueriesMemoryControl::GetArenaCounter(unsigned arena_ind) { return arena_tracking[arena_ind]; }

#endif

void StartTrackingCurrentThreadTransaction(uint64_t transaction_id) {
#if USE_JEMALLOC
  GetQueriesMemoryControl().UpdateThreadToTransactionId(std::this_thread::get_id(), transaction_id);
  GetQueriesMemoryControl().AddTrackingOnArena(QueriesMemoryControl::GetArenaForThread());
#endif
}

void StopTrackingCurrentThreadTransaction(uint64_t transaction_id) {
#if USE_JEMALLOC
  GetQueriesMemoryControl().ResetThreadToTransactionId(std::this_thread::get_id());
  GetQueriesMemoryControl().RemoveTrackingOnArena(QueriesMemoryControl::GetArenaForThread());
#endif
}

void StartTrackingThreadTransaction(const char *thread_id, uint64_t transaction_id) {
#if USE_JEMALLOC
  GetQueriesMemoryControl().UpdateThreadToTransactionId(thread_id, transaction_id);
  GetQueriesMemoryControl().AddTrackingOnArena(GetQueriesMemoryControl().GetArenaForThread());
#endif
}

void StopTrackingThreadTransaction(const char *thread_id, uint64_t transaction_id) {
#if USE_JEMALLOC
  GetQueriesMemoryControl().ResetThreadToTransactionId(thread_id);
  GetQueriesMemoryControl().RemoveTrackingOnArena(QueriesMemoryControl::GetArenaForThread());
#endif
}

void StartTrackingOnTransaction(uint64_t transaction_id, size_t limit) {
#if USE_JEMALLOC
  auto &memory_tracker = GetQueriesMemoryControl().CreateTransactionIdTracker(transaction_id);
  memory_tracker.SetMaximumHardLimit(static_cast<int64_t>(limit));
  memory_tracker.SetHardLimit(static_cast<int64_t>(limit));
  GetQueriesMemoryControl().AddTrackingOnArena(QueriesMemoryControl::GetArenaForThread());
  GetQueriesMemoryControl().UpdateThreadToTransactionId(std::this_thread::get_id(), transaction_id);
#endif
}

void StopTrackingOnTransaction(uint64_t transaction_id) {
#if USE_JEMALLOC
  GetQueriesMemoryControl().EraseTransactionIdTracker(transaction_id);

  GetQueriesMemoryControl().RemoveTrackingOnArena(QueriesMemoryControl::GetArenaForThread());
  GetQueriesMemoryControl().ResetThreadToTransactionId(std::this_thread::get_id());
#endif
}

}  // namespace memgraph::memory
