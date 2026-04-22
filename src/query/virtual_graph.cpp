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
      real_to_virtual_(other.real_to_virtual_, alloc),
      edges_(alloc),
      out_index_(alloc),
      in_index_(alloc) {
  CopyEdgesRebound(other.edges_, alloc);
}

VirtualGraph::VirtualGraph(VirtualGraph &&other, allocator_type alloc)
    : real_to_virtual_(std::move(other.real_to_virtual_), alloc), edges_(alloc), out_index_(alloc), in_index_(alloc) {
  if (alloc == other.nodes_ptr_->get_allocator()) {
    nodes_ptr_ = std::move(other.nodes_ptr_);
    edges_ = std::move(other.edges_);
    out_index_ = std::move(other.out_index_);
    in_index_ = std::move(other.in_index_);
  } else {
    nodes_ptr_ = std::make_shared<node_map>(*other.nodes_ptr_, alloc);
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
  // allocator as an extra argument, which our 5-arg rebound-copy ctor can't accept.
  // Build each edge directly and insert.
  const VirtualEdge::allocator_type edge_alloc(alloc.resource());
  for (const auto &edge : source) {
    const auto it_from = nodes_ptr_->find(edge.FromGid());
    const auto it_to = nodes_ptr_->find(edge.ToGid());
    DMG_ASSERT(it_from != nodes_ptr_->end() && it_to != nodes_ptr_->end(),
               "VirtualEdge references a synthetic gid absent from the source graph's node map");
    edges_.insert(VirtualEdge(edge, it_from->second, it_to->second, nodes_ptr_, edge_alloc));
  }
  RebuildEdgeIndexes();
}

const VirtualNode &VirtualGraph::InsertOrGetNode(storage::Gid real_gid, VirtualNode node) {
  if (const auto it = real_to_virtual_.find(real_gid); it != real_to_virtual_.end()) {
    return nodes_ptr_->at(it->second);
  }
  const auto synthetic_gid = node.Gid();
  const auto [it, inserted] = nodes_ptr_->try_emplace(synthetic_gid, std::move(node));
  DMG_ASSERT(inserted, "NextSyntheticGid counter collision: synthetic gid not unique");
  real_to_virtual_[real_gid] = synthetic_gid;
  return it->second;
}

const VirtualNode *VirtualGraph::FindNode(storage::Gid synthetic_gid) const {
  if (const auto it = nodes_ptr_->find(synthetic_gid); it != nodes_ptr_->end()) return &it->second;
  return nullptr;
}

const VirtualNode *VirtualGraph::FindNodeByRealGid(storage::Gid real_gid) const {
  if (const auto it = real_to_virtual_.find(real_gid); it != real_to_virtual_.end()) return FindNode(it->second);
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

namespace {
// Fold other's real→virtual mapping into main's and return other_synth → main_canonical_synth.
// For each real vertex, the first-seen synth wins; new nodes are copied into main under their
// own synth gid, aliased nodes reuse the canonical entry.
utils::pmr::unordered_map<storage::Gid, storage::Gid> MergeNodesAndBuildAliasMap(
    VirtualGraph::node_map &main_nodes, utils::pmr::unordered_map<storage::Gid, storage::Gid> &main_real_to_virtual,
    const VirtualGraph::node_map &other_nodes,
    const utils::pmr::unordered_map<storage::Gid, storage::Gid> &other_real_to_virtual, utils::MemoryResource *memory) {
  utils::pmr::unordered_map<storage::Gid, storage::Gid> alias(memory);
  for (const auto &[real_gid, other_synth] : other_real_to_virtual) {
    auto [it, inserted] = main_real_to_virtual.try_emplace(real_gid, other_synth);
    if (inserted) {
      if (const auto node_it = other_nodes.find(other_synth); node_it != other_nodes.end()) {
        main_nodes.try_emplace(other_synth, node_it->second);
      }
    }
    alias[other_synth] = it->second;
  }
  return alias;
}
}  // namespace

void VirtualGraph::Merge(const VirtualGraph &other) {
  const auto alias_map = MergeNodesAndBuildAliasMap(
      *nodes_ptr_, real_to_virtual_, *other.nodes_ptr_, other.real_to_virtual_, get_allocator().resource());
  const VirtualEdge::allocator_type edge_alloc(get_allocator().resource());
  for (const auto &edge : other.edges_) {
    const auto *from_node = FindNode(alias_map.at(edge.FromGid()));
    const auto *to_node = FindNode(alias_map.at(edge.ToGid()));
    DMG_ASSERT(from_node && to_node, "merge alias resolution failed");
    // Rebound-copy ctor preserves gid/type/props; only the endpoints change.
    InsertEdgeIfNew(VirtualEdge(edge, *from_node, *to_node, nodes_ptr_, edge_alloc));
  }
}

}  // namespace memgraph::query
