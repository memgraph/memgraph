// Copyright 2023 Memgraph Ltd.
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

#include <type_traits>

#include "storage/v2/id_types.hpp"

namespace memgraph::storage {

// Forward declaration because we only store a pointer here.
struct Edge;

struct EdgeRef {
  explicit EdgeRef(Gid gid) : gid(gid) {}
  explicit EdgeRef(Edge *ptr) : ptr(ptr) {}

  union {
    Gid gid;
    Edge *ptr;
  };
};

static_assert(sizeof(Gid) == sizeof(Edge *), "The Gid should be the same size as an Edge *!");
static_assert(std::is_standard_layout_v<Gid>, "The Gid must have a standard layout!");
static_assert(std::is_standard_layout_v<Edge *>, "The Edge * must have a standard layout!");
static_assert(std::is_standard_layout_v<EdgeRef>, "The EdgeRef must have a standard layout!");

inline bool operator==(const EdgeRef &a, const EdgeRef &b) noexcept { return a.gid == b.gid; }
inline bool operator<(const EdgeRef &first, const EdgeRef &second) { return first.gid < second.gid; }
inline bool operator!=(const EdgeRef &a, const EdgeRef &b) noexcept { return a.gid != b.gid; }
}  // namespace memgraph::storage
