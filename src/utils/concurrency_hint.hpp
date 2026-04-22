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

#include <atomic>
#include <cstddef>
#include <thread>

namespace memgraph::utils {

/// Expected number of concurrent worker threads.
/// Set once during startup (e.g. from FLAGS_bolt_num_workers);
/// read by allocators to pre-size thread-local structures.
inline std::atomic<std::size_t> global_num_workers{0};

inline void SetNumWorkers(std::size_t n) { global_num_workers.store(n, std::memory_order_release); }

inline auto GetNumWorkers() -> std::size_t {
  auto n = global_num_workers.load(std::memory_order_acquire);
  return n > 0 ? n : std::thread::hardware_concurrency();
}

}  // namespace memgraph::utils
