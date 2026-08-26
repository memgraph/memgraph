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

#include <algorithm>
#include <atomic>
#include <cstddef>

#include "utils/system_info.hpp"

namespace memgraph::utils {

/// Expected number of concurrent worker threads.
/// Set once during startup (e.g. from FLAGS_bolt_num_workers);
/// read by allocators to pre-size thread-local structures.
inline std::atomic<std::size_t> global_num_workers{0};

inline void SetNumWorkers(std::size_t n) { global_num_workers.store(n, std::memory_order_release); }

inline auto GetNumWorkers() -> std::size_t {
  auto n = global_num_workers.load(std::memory_order_acquire);
  if (n > 0) return n;
  // No explicit count set yet: fall back to the cgroup-aware usable core count
  // (never 0), so container CPU limits are respected instead of host cores.
  return static_cast<std::size_t>(UsableCoreCount());
}

}  // namespace memgraph::utils
