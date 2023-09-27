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
#include "utils/logging.hpp"
namespace memgraph::memory {

void PurgeUnusedMemory();
void SetHooks();
void UnSetHooks();
int GetArenaForThread();
void TrackMemoryForThread(int arena_ind, size_t size);
void SetGlobalLimit(size_t size);

inline std::atomic<int64_t> allocated_memory{0};
inline std::atomic<int64_t> virtual_allocated_memory{0};
inline size_t global_limit{0};

}  // namespace memgraph::memory
