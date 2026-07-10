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
#include <limits>

#include "storage/v2/id_types.hpp"

namespace memgraph::query {

// Synthetic Gids for VirtualNode and VirtualEdge share a single counter counting down from UINT64_MAX,
// so node and edge Gids are drawn from the same space and can never collide with each other
// (nor with real Gids, which count up from 0).
inline storage::Gid NextSyntheticGid() {
  static std::atomic<uint64_t> counter{std::numeric_limits<uint64_t>::max()};
  return storage::Gid::FromUint(counter.fetch_sub(1, std::memory_order_relaxed));
}

}  // namespace memgraph::query
