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

#include "utils/skip_list.hpp"

namespace memgraph::utils::detail {

bool &SkipListGcRunning() {
  thread_local bool skip_list_gc_running{false};
  return skip_list_gc_running;
}

bool IsSkipListGcRunning() { return SkipListGcRunning(); }

auto thread_local_mt19937() -> std::mt19937 & {
  thread_local std::mt19937 gen{std::random_device{}()};
  return gen;
}

}  // namespace memgraph::utils::detail
