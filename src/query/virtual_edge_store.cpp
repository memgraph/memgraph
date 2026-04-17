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

#include "query/virtual_edge_store.hpp"

namespace memgraph::query {

void VirtualEdgeStore::IndexEdge(const VirtualEdge *edge) {
  out_index_[edge->FromGid()].push_back(edge);
  in_index_[edge->ToGid()].push_back(edge);
}

void VirtualEdgeStore::RebuildIndexes() {
  for (const auto &edge : edges_) {
    IndexEdge(&edge);
  }
}

bool VirtualEdgeStore::InsertIfNew(VirtualEdge edge) {
  const auto [it, inserted] = edges_.insert(std::move(edge));
  if (!inserted) return false;
  IndexEdge(&*it);
  return true;
}

std::span<const VirtualEdge *const> VirtualEdgeStore::OutEdges(storage::Gid vertex_gid) const {
  if (const auto it = out_index_.find(vertex_gid); it != out_index_.end()) return it->second;
  return {};
}

std::span<const VirtualEdge *const> VirtualEdgeStore::InEdges(storage::Gid vertex_gid) const {
  if (const auto it = in_index_.find(vertex_gid); it != in_index_.end()) return it->second;
  return {};
}

}  // namespace memgraph::query
