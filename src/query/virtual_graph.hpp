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

class TypedValue;
class DbAccessor;

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
  VirtualGraph &operator=(VirtualGraph &&other) noexcept;
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

// What to do with a dangling edge - one whose endpoint resolves to no node in the node list - when
// assembling a projection from lists.
enum class DanglingEdgePolicy { kError, kDrop };

// Assemble a projection from lists of synthetic nodes and edges. Nodes are inserted (deduplicated by
// synthetic gid) and indexed by their import handle; a duplicate handle is an error. Each edge's
// endpoints are bound to a listed node - an unresolved (handle) endpoint by matching handle, a
// resolved endpoint by synthetic-gid membership - then the edge is inserted (deduplicated by
// from/to/type). A dangling edge aborts the assembly under kError or is omitted under kDrop.
VirtualGraph AssembleVirtualGraph(std::span<const VirtualNode> nodes, std::span<const VirtualEdge> edges,
                                  DanglingEdgePolicy policy, VirtualGraph::allocator_type alloc);

// The real-vertex-gid -> canonical-synthetic-gid map derive()/project() assembly uses to deduplicate
// a path endpoint that recurs across rows: the first occurrence builds an overlay node, later ones
// reuse it. Owned by the aggregation slot and threaded into AddPathToProjection.
using DerivedNodeDedup = utils::pmr::unordered_map<storage::Gid, storage::Gid>;

// Collapse a derive() path to a single synthetic overlay edge between its endpoints (intermediate
// vertices are ignored by design), adding the endpoint overlay nodes and the edge to
// projected_graph. Each endpoint reads through to its real origin lazily; options carries the
// label / property / edge-type / binding configuration. dedup canonicalizes endpoints that recur
// across rows; projection_ref is the schema reference stamped on the overlay nodes (or
// VirtualNode::kNoProjectionRef). This is the aggregation counterpart to AssembleVirtualGraph's list
// assembly - the Aggregate operator drives it per row - so all VirtualGraph construction lives here.
void AddPathToProjection(const TypedValue &path_value, const TypedValue &options_value, VirtualGraph &projected_graph,
                         DerivedNodeDedup &dedup, int64_t projection_ref, DbAccessor *db_accessor);

}  // namespace memgraph::query
