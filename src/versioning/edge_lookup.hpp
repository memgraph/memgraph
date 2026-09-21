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

#include <optional>

#include "storage/v2/id_types.hpp"
#include "storage/v2/storage.hpp"  // Storage::Accessor; transitively: EdgeAccessor, View

namespace memgraph::versioning {

// FindEdge(gid, View::OLD) is unreliable for light edges: their adjacency entry is unconditionally
// popped on delete (no per-edge delta chain), so a deleted light edge reads as "never existed"
// regardless of view. Walk OutEdges(View::OLD) via the FROM vertex's own MVCC chain instead —
// the same primitive BranchContext::ResolveEdges relies on (branch_engine.cpp).
//
// Used by both merge.cpp (pass-1 fork-state classification) and branch_engine.cpp
// (MarkMainEdgeBranched). Both callers need the same View::OLD endpoint-relative scan.
inline std::optional<storage::EdgeAccessor> FindHistoricalEdgeByEndpoint(storage::Storage::Accessor &historical,
                                                                         storage::Gid from_vertex_gid,
                                                                         storage::Gid edge_gid) {
  auto from_v = historical.FindVertex(from_vertex_gid, storage::View::OLD);
  if (!from_v.has_value()) return std::nullopt;
  auto out_res = from_v->OutEdges(storage::View::OLD);
  if (!out_res.has_value()) return std::nullopt;
  for (auto &e : out_res->edges) {
    if (e.Gid() == edge_gid) return e;
  }
  return std::nullopt;
}

}  // namespace memgraph::versioning
