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
#include <cstdint>
#include <limits>
#include <mutex>
#include <unordered_map>

#include "storage/v2/id_types.hpp"

namespace memgraph::query {

// Synthetic Gids for VirtualNode and VirtualEdge share a single counter counting down from UINT64_MAX,
// so node and edge Gids are drawn from the same space and can never collide with each other
// (nor with real Gids, which count up from 0).
inline storage::Gid NextSyntheticGid() {
  static std::atomic<uint64_t> counter{std::numeric_limits<uint64_t>::max()};
  return storage::Gid::FromUint(counter.fetch_sub(1, std::memory_order_relaxed));
}

// Projects process-global synthetic Gids onto small, query-local external ids. A synthetic Gid is
// unique for the life of the process, which keeps virtual-entity identity collision-free but makes
// the user-facing id() of a virtual node depend on how many were made earlier. This mapper hands out
// dense negative ids (-1, -2, ...) the first time it sees each Gid, so within one query the author
// gets stable, predictable identifiers and repeated references to the same entity share an id, while
// a new query (a new mapper) starts again at -1.
class SyntheticIdMapper {
 public:
  int64_t ExternalId(storage::Gid internal) {
    auto guard = std::lock_guard{mutex_};
    auto [it, inserted] = mapping_.try_emplace(internal.AsUint(), next_);
    if (inserted) --next_;
    return it->second;
  }

 private:
  std::mutex mutex_;
  std::unordered_map<uint64_t, int64_t> mapping_;
  int64_t next_{-1};
};

// The single synthetic-axis id fallback: a mapper maps the synthetic Gid to the query-local external
// id id() reports; with no mapper the raw synthetic Gid is used, read as a signed int so the query and
// wire axes agree. This is the synthetic axis only - a real (overlay) node serializes at its origin's
// unsigned real Gid, a separate axis that does not go through here.
inline int64_t ExternalId(storage::Gid gid, SyntheticIdMapper *mapper) {
  return mapper != nullptr ? mapper->ExternalId(gid) : gid.AsInt();
}

}  // namespace memgraph::query
