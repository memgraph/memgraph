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

#include "query/virtual_graph.hpp"

namespace memgraph::query {

namespace {
EdgeRefView MakeView(std::span<const VirtualEdge *const> ptrs) {
  return ptrs | std::views::transform(detail::kDerefEdgePtr);
}
}  // namespace

void VirtualGraph::IndexEdge(const VirtualEdge *edge) {
  out_index_[edge->FromGid()].push_back(edge);
  in_index_[edge->ToGid()].push_back(edge);
}

void VirtualGraph::RebuildEdgeIndexes() {
  for (const auto &edge : edges_) {
    IndexEdge(&edge);
  }
}

const VirtualNode &VirtualGraph::InsertOrGetNode(VirtualNode node) {
  const auto original_gid = node.OriginalGid();
  const auto synthetic_gid = node.Gid();
  auto [it, inserted] = nodes_.try_emplace(original_gid, std::move(node));
  if (inserted) {
    synthetic_to_original_[synthetic_gid] = original_gid;
  }
  return it->second;
}

const VirtualNode *VirtualGraph::FindNode(storage::Gid original_gid) const {
  if (const auto it = nodes_.find(original_gid); it != nodes_.end()) return &it->second;
  return nullptr;
}

const VirtualNode *VirtualGraph::FindNodeBySyntheticGid(storage::Gid synthetic_gid) const {
  if (const auto it = synthetic_to_original_.find(synthetic_gid); it != synthetic_to_original_.end()) {
    return FindNode(it->second);
  }
  return nullptr;
}

bool VirtualGraph::InsertEdgeIfNew(VirtualEdge edge) {
  const auto [it, inserted] = edges_.insert(std::move(edge));
  if (!inserted) return false;
  IndexEdge(&*it);
  return true;
}

EdgeRefView VirtualGraph::OutEdges(storage::Gid vertex_gid) const {
  if (const auto it = out_index_.find(vertex_gid); it != out_index_.end()) return MakeView(it->second);
  return MakeView({});
}

EdgeRefView VirtualGraph::InEdges(storage::Gid vertex_gid) const {
  if (const auto it = in_index_.find(vertex_gid); it != in_index_.end()) return MakeView(it->second);
  return MakeView({});
}

void VirtualGraph::Merge(const VirtualGraph &other) {
  // Keep this graph's canonical node for any original_gid it already holds; record every
  // incoming synthetic gid as an alias so edges built in other's synth space remain resolvable.
  for (const auto &[original_gid, node] : other.nodes_) {
    nodes_.try_emplace(original_gid, node);
    synthetic_to_original_[node.Gid()] = original_gid;
  }
  for (const auto &edge : other.edges_) {
    InsertEdgeIfNew(edge);
  }
}

void VirtualGraph::Merge(VirtualGraph &&other) {
  for (const auto &[original_gid, node] : other.nodes_) {
    synthetic_to_original_[node.Gid()] = original_gid;
  }
  if (nodes_.get_allocator() == other.nodes_.get_allocator()) {
    // Matching allocators: unordered_map::merge transfers node handles for keys we don't
    // already hold; conflicting nodes are left in other (discarded when other goes away).
    nodes_.merge(other.nodes_);
  } else {
    for (auto &[original_gid, node] : other.nodes_) {
      nodes_.try_emplace(original_gid, std::move(node));
    }
  }
  // edges_ yields const references (unordered_set stores keys immutably), so the inner
  // copy can't be avoided.
  for (const auto &edge : other.edges_) {
    InsertEdgeIfNew(edge);
  }
}

}  // namespace memgraph::query
