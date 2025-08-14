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

#pragma once

#include "storage/v2/id_types.hpp"

namespace memgraph::storage {

struct Edge;

struct EdgeRef {
  explicit EdgeRef(Gid gid) : gid(gid) {}
  explicit EdgeRef(Edge *ptr) : ptr(ptr) {}

  friend bool operator==(const EdgeRef &a, const EdgeRef &b) noexcept { return a.gid == b.gid; }
  friend bool operator<(const EdgeRef &first, const EdgeRef &second) { return first.gid < second.gid; }

  union {
    Gid gid;
    Edge *ptr;
  };

  template <typename H>
  friend H AbslHashValue(H h, EdgeRef const &edge_ref) {
    return H::combine(std::move(h), edge_ref.gid);
  }
};

}  // namespace memgraph::storage
