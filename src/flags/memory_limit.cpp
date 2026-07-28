// Copyright 2026 Memgraph Ltd.
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

#include <optional>

#include "gflags/gflags.h"
#include "utils/logging.hpp"
#include "utils/sysinfo/memory.hpp"

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_uint64(
    memory_limit, 0,
    "Total memory limit in MiB. Set to 0 to use the default values which are 100\% of the phyisical memory if the swap "
    "is enabled and 90\% of the physical memory otherwise.");

int64_t memgraph::flags::GetMemoryLimit() {
  if (FLAGS_memory_limit == 0) {
    const auto maybe_capacity = memgraph::utils::sysinfo::InstalledMemory();
    MG_ASSERT(maybe_capacity, "Failed to fetch the total physical and swap memory capacity");

    auto memory_kib = maybe_capacity->ram_kib;
    if (maybe_capacity->swap_kib == 0) {
      // take only 90% of the total memory
      memory_kib *= 9;
      memory_kib /= 10;
    }
    return memory_kib * 1024;
  }

  // We parse the memory as MiB every time
  return FLAGS_memory_limit * 1024 * 1024;
}
