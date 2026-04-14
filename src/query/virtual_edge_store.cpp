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

void VirtualEdgeStore::IndexEdge(const VirtualEdge &edge) {
  out_index_[edge.FromGid()].push_back(edge);
  in_index_[edge.ToGid()].push_back(edge);
}

void VirtualEdgeStore::RebuildIndexes() {
  dedup_.clear();
  out_index_.clear();
  in_index_.clear();
  for (const auto &edge : edges_) {
    dedup_.insert(DedupKey{.from = edge.FromGid(), .to = edge.ToGid(), .type = edge.EdgeTypeName()});
    IndexEdge(edge);
  }
}

void VirtualEdgeStore::Insert(const VirtualEdge &edge) {
  edges_.insert(edge);
  IndexEdge(edge);
}

bool VirtualEdgeStore::InsertIfNew(VirtualEdge edge) {
  auto inserted =
      dedup_.insert(DedupKey{.from = edge.FromGid(), .to = edge.ToGid(), .type = edge.EdgeTypeName()}).second;
  if (!inserted) return false;
  IndexEdge(edge);
  edges_.insert(std::move(edge));
  return true;
}

bool VirtualEdgeStore::Contains(const VirtualEdge &edge) const { return edges_.contains(edge); }

std::span<const VirtualEdge> VirtualEdgeStore::OutEdges(storage::Gid vertex_gid) const {
  if (auto it = out_index_.find(vertex_gid); it != out_index_.end()) return it->second;
  return {};
}

std::span<const VirtualEdge> VirtualEdgeStore::InEdges(storage::Gid vertex_gid) const {
  if (auto it = in_index_.find(vertex_gid); it != in_index_.end()) return it->second;
  return {};
}

}  // namespace memgraph::query
