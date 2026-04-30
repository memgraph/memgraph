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

#include <memory>
#include <span>

#include "query/virtual_edge.hpp"
#include "query/virtual_node.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/unordered_map.hpp"
#include "utils/pmr/unordered_set.hpp"
#include "utils/pmr/vector.hpp"

namespace memgraph::query {

// Maps synthetic gids in one VirtualGraph (the "aliased" one) to the synthetic
// gid of the canonical VirtualNode in another VirtualGraph.
using VirtualGraphAliasMap = utils::pmr::unordered_map<storage::Gid, storage::Gid>;

class VirtualGraph final {
 public:
  using allocator_type = utils::Allocator<VirtualGraph>;
  using node_map = utils::pmr::unordered_map<storage::Gid, std::shared_ptr<const VirtualNode>>;
  using edge_set = utils::pmr::unordered_set<VirtualEdge>;
  using adjacency_map = utils::pmr::unordered_map<storage::Gid, utils::pmr::vector<const VirtualEdge *>>;

  explicit VirtualGraph(allocator_type alloc) : nodes_(alloc), edges_(alloc), out_index_(alloc), in_index_(alloc) {}

  VirtualGraph(const VirtualGraph &other, allocator_type alloc);

  VirtualGraph(const VirtualGraph &other) : VirtualGraph(other, other.get_allocator()) {}

  VirtualGraph(VirtualGraph &&other) noexcept = default;

  VirtualGraph(VirtualGraph &&other, allocator_type alloc);

  VirtualGraph &operator=(const VirtualGraph &other);
  VirtualGraph &operator=(VirtualGraph &&other);
  ~VirtualGraph() = default;

  const VirtualNode &InsertNode(VirtualNode node);

  [[nodiscard]] std::shared_ptr<const VirtualNode> FindNode(storage::Gid synthetic_gid) const;

  bool InsertEdgeIfNew(VirtualEdge edge);

  [[nodiscard]] bool ContainsEdge(const VirtualEdge &edge) const { return edges_.contains(edge); }

  [[nodiscard]] std::span<const VirtualEdge *const> OutEdges(storage::Gid vertex_gid) const;
  [[nodiscard]] std::span<const VirtualEdge *const> InEdges(storage::Gid vertex_gid) const;

  [[nodiscard]] const node_map &nodes() const noexcept { return nodes_; }

  [[nodiscard]] const edge_set &edges() const noexcept { return edges_; }

  void Merge(const VirtualGraph &other, const VirtualGraphAliasMap &aliases);

  [[nodiscard]] auto get_allocator() const noexcept -> allocator_type { return nodes_.get_allocator(); }

 private:
  void IndexEdge(const VirtualEdge *edge);
  void RebuildEdgeIndexes();

  node_map nodes_;
  edge_set edges_;
  adjacency_map out_index_;
  adjacency_map in_index_;
};

}  // namespace memgraph::query
