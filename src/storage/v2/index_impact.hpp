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

namespace memgraph::storage {

/// Which of the two index families a set of writes could have left stale entries in.
///
/// The families are disjoint: no vertex property or label appears in an edge index key, and no
/// edge property or type appears in a vertex index key. A single write therefore names at most
/// one of them, and the two sweeps are gated separately so a workload touching only one side
/// does not pay to scan the other.
///
/// This travels from where the writes happen to where the sweeps are scheduled, because a delta
/// records the action but not the object it belonged to.
struct IndexImpact {
  bool vertex_indexes{false};
  bool edge_indexes{false};

  IndexImpact &operator|=(IndexImpact const &other) {
    vertex_indexes |= other.vertex_indexes;
    edge_indexes |= other.edge_indexes;
    return *this;
  }

  bool any() const { return vertex_indexes || edge_indexes; }

  friend bool operator==(IndexImpact const &, IndexImpact const &) = default;
};

}  // namespace memgraph::storage
