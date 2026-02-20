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

#include "storage/v2/edge_ref.hpp"

#include "storage/v2/edge.hpp"
#include "utils/logging.hpp"

#include <type_traits>

namespace memgraph::storage {

static_assert(sizeof(Gid) == sizeof(Edge *), "The Gid should be the same size as an Edge *!");
static_assert(std::is_standard_layout_v<Gid>, "The Gid must have a standard layout!");
static_assert(std::is_standard_layout_v<Edge *>, "The Edge * must have a standard layout!");
static_assert(std::is_trivially_copyable_v<EdgeRef>, "EdgeRef must remain trivially copyable for Delta");

Gid EdgeRef::GetGid() const {
  if (storage_ & kEdgeRefGidBit) {
    return Gid::FromUint(storage_ & ~kEdgeRefGidBit);
  }
  return ptr_->Gid();
}

Edge *EdgeRef::GetEdgePtr() const {
  MG_ASSERT(HasPointer(), "GetEdgePtr() only valid when HasPointer()");
  return ptr_;
}

bool operator==(const EdgeRef &a, const EdgeRef &b) noexcept { return a.GetGid() == b.GetGid(); }

bool operator<(const EdgeRef &first, const EdgeRef &second) { return first.GetGid() < second.GetGid(); }

}  // namespace memgraph::storage
