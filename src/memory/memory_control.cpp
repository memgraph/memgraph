// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "memory_control.hpp"

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

namespace memory {

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define STRINGIFY_HELPER(x) #x
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define STRINGIFY(x) STRINGIFY_HELPER(x)

void PurgeUnusedMemory() {
#if USE_JEMALLOC
  mallctl("arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge", nullptr, nullptr, nullptr, 0);
#endif
}

#undef STRINGIFY
#undef STRINGIFY_HELPER
}  // namespace memory
