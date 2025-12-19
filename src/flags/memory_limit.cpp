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
#include "flags/memory_limit.hpp"

#include "utils/logging.hpp"
#include "utils/sysinfo/memory.hpp"

#include "gflags/gflags.h"

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_uint64(
    memory_limit, 0,
    "Total memory limit in MiB. Set to 0 to use the default values which are 100\% of the phyisical memory if the swap "
    "is enabled and 90\% of the physical memory otherwise.");

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_uint64(memory_decay_interval_ms, 10000,
              "Interval in milliseconds for scheduler-controlled jemalloc memory decay. "
              "When set to a positive value, jemalloc's internal decay timing is disabled "
              "and memory is returned to the OS on this interval instead. "
              "This reduces non-deterministic clock_gettime calls during allocation. "
              "Set to 0 to disable scheduler-controlled decay and use jemalloc's internal timing.");

int64_t memgraph::flags::GetMemoryLimit() {
  if (FLAGS_memory_limit == 0) {
    auto maybe_total_memory = memgraph::utils::sysinfo::TotalMemory();
    MG_ASSERT(maybe_total_memory, "Failed to fetch the total physical memory");
    const auto maybe_swap_memory = memgraph::utils::sysinfo::SwapTotalMemory();
    MG_ASSERT(maybe_swap_memory, "Failed to fetch the total swap memory");

    if (*maybe_swap_memory == 0) {
      // take only 90% of the total memory
      *maybe_total_memory *= 9;
      *maybe_total_memory /= 10;
    }
    return *maybe_total_memory * 1024;
  }

  // We parse the memory as MiB every time
  return FLAGS_memory_limit * 1024 * 1024;
}

std::chrono::milliseconds memgraph::flags::GetMemoryDecayInterval() {
  return std::chrono::milliseconds(FLAGS_memory_decay_interval_ms);
}
