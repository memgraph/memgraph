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
#include <unordered_map>
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"

#include <thread>
namespace memgraph::memory {

void PurgeUnusedMemory();
void SetHooks();

// TODO(af) This should all be part of memgraph::memory::thread namespace, and moved to different file
// This should be part of class
unsigned GetArenaForThread();
bool AddTrackingOnArena(unsigned);
bool RemoveTrackingOnArena(unsigned);
void UpdateThreadToTransactionId(const std::thread::id &, uint64_t);
void ResetThreadToTransactionId(const std::thread::id &);
void UpdateThreadToTransactionId(const char *, uint64_t);
void ResetThreadToTransactionId(const char *);
void AddTrackingsOnCurrentThread(uint64_t);
void RemoveTrackingsOnCurrentThread();

inline std::unordered_map<std::string, uint64_t> thread_id_to_transaction_id;
// TODO(af): think if we need to solve issue of tracking allocations for arena
// if user forgets to unregister tracking for that thread before it dies.
inline std::unordered_map<unsigned, std::atomic<int>> arena_tracking;
inline std::unordered_map<uint64_t, utils::MemoryTracker> transaction_id_tracker;

}  // namespace memgraph::memory
