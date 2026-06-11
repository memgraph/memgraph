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

#include <optional>

#include "query/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/pmr/unordered_map.hpp"

namespace memgraph::query {

VirtualGraph::VirtualGraph(const VirtualGraph &other, allocator_type alloc)
    : nodes_(other.nodes_, alloc), edges_(other.edges_, alloc), out_index_(alloc), in_index_(alloc) {
  RebuildEdgeIndexes();
}

VirtualGraph &VirtualGraph::operator=(const VirtualGraph &other) {
  if (this == &other) return *this;
  nodes_ = other.nodes_;
  edges_ = other.edges_;
  out_index_.clear();
  in_index_.clear();
  RebuildEdgeIndexes();
  return *this;
}

VirtualGraph &VirtualGraph::operator=(VirtualGraph &&other) noexcept {
  if (this == &other) return *this;
  DMG_ASSERT(get_allocator() == other.get_allocator(), "VirtualGraph move-assign across allocators is not supported");
  nodes_ = std::move(other.nodes_);
  edges_ = std::move(other.edges_);
  out_index_ = std::move(other.out_index_);
  in_index_ = std::move(other.in_index_);
  return *this;
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
    edges_ = edge_set(other.edges_, alloc);
    RebuildEdgeIndexes();
  }
}

void VirtualGraph::IndexEdge(const VirtualEdge *edge) {
  out_index_[edge->FromGid()].push_back(edge);
  in_index_[edge->ToGid()].push_back(edge);
}

void VirtualGraph::RebuildEdgeIndexes() {
  for (const auto &edge : edges_) IndexEdge(&edge);
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

std::span<const VirtualEdge *const> VirtualGraph::OutEdges(storage::Gid vertex_gid) const {
  if (const auto it = out_index_.find(vertex_gid); it != out_index_.end()) return it->second;
  return {};
}

std::span<const VirtualEdge *const> VirtualGraph::InEdges(storage::Gid vertex_gid) const {
  if (const auto it = in_index_.find(vertex_gid); it != in_index_.end()) return it->second;
  return {};
}

void VirtualGraph::Merge(const VirtualGraph &other, const VirtualGraphAliasMap &aliases) {
  for (const auto &[synth_gid, node] : other.nodes_) {
    if (aliases.contains(synth_gid)) continue;
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

VirtualGraph AssembleVirtualGraph(std::span<const VirtualNode> nodes, std::span<const VirtualEdge> edges,
                                  DanglingEdgePolicy policy, VirtualGraph::allocator_type alloc) {
  VirtualGraph graph(alloc);

  // The synthetic gid of the node carrying each import handle, so a handle endpoint binds to a node.
  utils::pmr::unordered_map<int64_t, storage::Gid> handle_to_gid(alloc.resource());
  for (const auto &node : nodes) {
    const auto &inserted = graph.InsertNode(node);
    if (const auto handle = inserted.Handle()) {
      const auto [it, added] = handle_to_gid.try_emplace(*handle, inserted.Gid());
      if (!added && it->second != inserted.Gid()) {
        throw QueryRuntimeException("Duplicate import handle {} in the virtualGraph() node list.", *handle);
      }
    }
  }

  // Bind an endpoint to a listed node: a handle endpoint by matching handle, a resolved endpoint by
  // its synthetic gid. Returns nullptr when no listed node matches (the endpoint is dangling).
  const auto bind = [&](std::optional<int64_t> handle, storage::Gid gid) -> std::shared_ptr<const VirtualNode> {
    if (handle) {
      const auto it = handle_to_gid.find(*handle);
      if (it == handle_to_gid.end()) return nullptr;
      return graph.FindNode(it->second);
    }
    return graph.FindNode(gid);
  };

  const VirtualEdge::allocator_type edge_alloc(alloc.resource());
  for (const auto &edge : edges) {
    auto from = bind(edge.FromHandle(), edge.FromGid());
    auto to = bind(edge.ToHandle(), edge.ToGid());
    if (!from || !to) {
      if (policy == DanglingEdgePolicy::kDrop) continue;
      throw QueryRuntimeException("virtualGraph() edge references a node absent from the node list (dangling edge).");
    }
    graph.InsertEdgeIfNew(VirtualEdge(edge, std::move(from), std::move(to), edge_alloc));
  }
  return graph;
}

}  // namespace memgraph::query
