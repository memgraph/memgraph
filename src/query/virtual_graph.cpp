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
    : nodes_ptr_(std::make_shared<node_map>(*other.nodes_ptr_, alloc)),
      synthetic_to_original_(other.synthetic_to_original_, alloc),
      edges_(alloc),
      out_index_(alloc),
      in_index_(alloc) {
  CopyEdgesRebound(other.edges_, alloc);
}

VirtualGraph::VirtualGraph(VirtualGraph &&other, allocator_type alloc)
    : synthetic_to_original_(std::move(other.synthetic_to_original_), alloc),
      edges_(alloc),
      out_index_(alloc),
      in_index_(alloc) {
  if (alloc == other.nodes_ptr_->get_allocator()) {
    // Same allocator: steal the shared_ptr, the edge set, and the indexes outright.
    nodes_ptr_ = std::move(other.nodes_ptr_);
    edges_ = std::move(other.edges_);
    out_index_ = std::move(other.out_index_);
    in_index_ = std::move(other.in_index_);
  } else {
    // Different allocator: have to duplicate the node map under the new allocator
    // and rebind every edge's from_/to_ pointers to addresses in the new map.
    nodes_ptr_ = std::make_shared<node_map>(*other.nodes_ptr_, alloc);
    CopyEdgesRebound(other.edges_, alloc);
  }
}

void VirtualGraph::IndexEdge(const VirtualEdge *edge) {
  out_index_[edge->FromGid()].push_back(edge);
  in_index_[edge->ToGid()].push_back(edge);
}

void VirtualGraph::RebuildEdgeIndexes() {
  for (const auto &edge : edges_) {
    IndexEdge(&edge);
  }
}

void VirtualGraph::CopyEdgesRebound(const edge_set &source, allocator_type alloc) {
  // VirtualEdge's rebound-copy ctor is a custom 5-arg form; uses-allocator construction
  // via edges_.emplace() would try to append the allocator as an extra argument. Construct
  // the edge directly and insert instead.
  VirtualEdge::allocator_type edge_alloc(alloc.resource());
  for (const auto &edge : source) {
    const auto it_from = nodes_ptr_->find(edge.From().OriginalGid());
    const auto it_to = nodes_ptr_->find(edge.To().OriginalGid());
    DMG_ASSERT(it_from != nodes_ptr_->end() && it_to != nodes_ptr_->end(),
               "VirtualEdge references a node absent from the source graph's node map");
    edges_.insert(VirtualEdge(edge, it_from->second, it_to->second, nodes_ptr_, edge_alloc));
  }
  RebuildEdgeIndexes();
}

const VirtualNode &VirtualGraph::InsertOrGetNode(VirtualNode node) {
  const auto original_gid = node.OriginalGid();
  const auto synthetic_gid = node.Gid();
  auto [it, inserted] = nodes_ptr_->try_emplace(original_gid, std::move(node));
  if (inserted) {
    synthetic_to_original_[synthetic_gid] = original_gid;
  }
  return it->second;
}

const VirtualNode *VirtualGraph::FindNode(storage::Gid original_gid) const {
  if (const auto it = nodes_ptr_->find(original_gid); it != nodes_ptr_->end()) return &it->second;
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
  for (const auto &[original_gid, node] : *other.nodes_ptr_) {
    nodes_ptr_->try_emplace(original_gid, node);
    synthetic_to_original_[node.Gid()] = original_gid;
  }
  // Edges from other still point into other.nodes_ptr_; their anchor keeps that map alive.
  // We insert copies here (anchor propagates via the copy ctor).
  for (const auto &edge : other.edges_) {
    InsertEdgeIfNew(edge);
  }
}

void VirtualGraph::Merge(VirtualGraph &&other) {
  for (const auto &[original_gid, node] : *other.nodes_ptr_) {
    synthetic_to_original_[node.Gid()] = original_gid;
  }
  if (nodes_ptr_->get_allocator() == other.nodes_ptr_->get_allocator()) {
    // Matching allocators: unordered_map::merge transfers node handles for keys we don't
    // already hold; conflicting nodes are left in other (discarded when other goes away).
    nodes_ptr_->merge(*other.nodes_ptr_);
  } else {
    for (auto &[original_gid, node] : *other.nodes_ptr_) {
      nodes_ptr_->try_emplace(original_gid, std::move(node));
    }
  }
  // edges_ yields const references (unordered_set stores keys immutably), so the inner
  // copy can't be avoided.
  for (const auto &edge : other.edges_) {
    InsertEdgeIfNew(edge);
  }
}

}  // namespace memgraph::query
