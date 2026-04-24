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

#include "utils/logging.hpp"

namespace memgraph::query {

namespace {
EdgeRefView MakeView(std::span<const VirtualEdge *const> ptrs) {
  return ptrs | std::views::transform(detail::kDerefEdgePtr);
}
}  // namespace

VirtualGraph::VirtualGraph(const VirtualGraph &other, allocator_type alloc)
    : nodes_(other.nodes_, alloc), edges_(alloc), out_index_(alloc), in_index_(alloc) {
  CopyEdgesRebound(other.edges_, alloc);
}

VirtualGraph::VirtualGraph(VirtualGraph &&other, allocator_type alloc)
    : nodes_(alloc), edges_(alloc), out_index_(alloc), in_index_(alloc) {
  if (alloc == other.nodes_.get_allocator()) {
    nodes_ = std::move(other.nodes_);
    edges_ = std::move(other.edges_);
    out_index_ = std::move(other.out_index_);
    in_index_ = std::move(other.in_index_);
  } else {
    nodes_ = node_map(other.nodes_, alloc);
    CopyEdgesRebound(other.edges_, alloc);
  }
}

void VirtualGraph::IndexEdge(const VirtualEdge *edge) {
  out_index_[edge->FromGid()].push_back(edge);
  in_index_[edge->ToGid()].push_back(edge);
}

void VirtualGraph::RebuildEdgeIndexes() {
  for (const auto &edge : edges_) IndexEdge(&edge);
}

void VirtualGraph::CopyEdgesRebound(const edge_set &source, allocator_type alloc) {
  // edges_.emplace() would route through uses-allocator construction and append the
  // allocator as an extra argument, which our rebound-copy ctor can't accept.
  // Build each edge directly and insert.
  const VirtualEdge::allocator_type edge_alloc(alloc.resource());
  for (const auto &edge : source) {
    const auto it_from = nodes_.find(edge.FromGid());
    const auto it_to = nodes_.find(edge.ToGid());
    DMG_ASSERT(it_from != nodes_.end() && it_to != nodes_.end(),
               "VirtualEdge references a synthetic gid absent from the source graph's node map");
    edges_.insert(VirtualEdge(edge, it_from->second, it_to->second, edge_alloc));
  }
  RebuildEdgeIndexes();
}

const VirtualNode &VirtualGraph::InsertNode(VirtualNode node) {
  const auto synthetic_gid = node.Gid();
  auto shared = std::allocate_shared<VirtualNode>(
      std::pmr::polymorphic_allocator<VirtualNode>(nodes_.get_allocator().resource()), std::move(node));
  const auto [it, inserted] = nodes_.try_emplace(synthetic_gid, std::move(shared));
  DMG_ASSERT(inserted, "NextSyntheticGid counter collision: synthetic gid not unique");
  return *it->second;
}

std::shared_ptr<const VirtualNode> VirtualGraph::FindNode(storage::Gid synthetic_gid) const {
  if (const auto it = nodes_.find(synthetic_gid); it != nodes_.end()) return it->second;
  return {};
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

void VirtualGraph::Merge(const VirtualGraph &other, const VirtualGraphAliasMap &aliases) {
  for (const auto &[synth_gid, node] : other.nodes_) {
    if (aliases.contains(synth_gid)) continue;  // aliased to an existing canonical; skip
    nodes_.try_emplace(synth_gid, node);
  }
  const auto resolve = [&aliases](storage::Gid g) {
    if (const auto it = aliases.find(g); it != aliases.end()) return it->second;
    return g;
  };
  const VirtualEdge::allocator_type edge_alloc(get_allocator().resource());
  for (const auto &edge : other.edges_) {
    auto from_shared = FindNode(resolve(edge.FromGid()));
    auto to_shared = FindNode(resolve(edge.ToGid()));
    DMG_ASSERT(from_shared && to_shared, "merge alias resolution failed");
    InsertEdgeIfNew(VirtualEdge(edge, std::move(from_shared), std::move(to_shared), edge_alloc));
  }
}

}  // namespace memgraph::query
