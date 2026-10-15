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

#pragma once

#include <cstdint>
#include <optional>

namespace memgraph::memory {

// Process-wide allocator counters, in bytes. allocated is what the application holds, active adds the
// unused parts of slabs, resident adds dirty and muzzy pages not yet returned to the OS, mapped is
// everything the allocator has mapped, retained is unmapped-but-reserved address space, metadata is the
// allocator's own bookkeeping. Nullopt when the allocator is not jemalloc or the counters cannot be read.
//
// Kept in its own translation unit so that linking it does not also pull in the arena machinery, which
// needs the allocation hooks only the memgraph binary defines.
struct AllocatorStats {
  uint64_t allocated{0};
  uint64_t active{0};
  uint64_t metadata{0};
  uint64_t resident{0};
  uint64_t mapped{0};
  uint64_t retained{0};
};

std::optional<AllocatorStats> GetAllocatorStats();

}  // namespace memgraph::memory
