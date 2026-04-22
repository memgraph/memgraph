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
#include <ranges>
#include <span>

#include "query/virtual_edge.hpp"
#include "query/virtual_node.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/unordered_map.hpp"
#include "utils/pmr/unordered_set.hpp"
#include "utils/pmr/vector.hpp"

namespace memgraph::query {

namespace detail {
// Named lambda so the resulting transform_view type is nameable in aliases and struct fields.
inline constexpr auto kDerefEdgePtr = [](const VirtualEdge *p) noexcept -> const VirtualEdge & { return *p; };
}  // namespace detail

using EdgeRefView = decltype(std::span<const VirtualEdge *const>{} | std::views::transform(detail::kDerefEdgePtr));

// Holds nodes and edges of a virtual graph plus the adjacency indexes.
//
// Invariants:
//  - nodes_ptr_ is a shared_ptr<node_map>. Every VirtualEdge carries a
//    shared_ptr<const void> anchor that shares this control block, so edges
//    that escape the graph (collect(e), g.edges returned from a subquery)
//    keep the node map alive via their refcount.
//  - The node map is keyed by VirtualNode::Gid() (synthetic). VirtualNodes
//    themselves carry no provenance back to any real vertex; they're identified
//    solely by their synthetic gid.
//  - real_to_virtual_ (optional; empty for graphs not built by derive())
//    maps an external real-vertex gid → the synthetic gid of its canonical
//    VirtualNode. derive() populates this via InsertOrGetNode; callers that
//    need to look up "the VirtualNode derived from real vertex X" go through
//    FindNodeByRealGid.
//  - edges_ is a unordered_set keyed by (from_synthetic_gid, to_synthetic_gid,
//    type) via VirtualEdge's semantic operator== / hash. pmr::unordered_set
//    stores keys by const-value with pointer stability — out_index_ and
//    in_index_ hold const VirtualEdge* into edges_'s buckets.
class VirtualGraph final {
 public:
  using allocator_type = utils::Allocator<VirtualGraph>;
  using node_map = utils::pmr::unordered_map<storage::Gid, VirtualNode>;
  using edge_set = utils::pmr::unordered_set<VirtualEdge>;
  using adjacency_map = utils::pmr::unordered_map<storage::Gid, utils::pmr::vector<const VirtualEdge *>>;

  explicit VirtualGraph(allocator_type alloc)
      : nodes_ptr_(std::make_shared<node_map>(alloc)),
        real_to_virtual_(alloc),
        edges_(alloc),
        out_index_(alloc),
        in_index_(alloc) {}

  VirtualGraph(const VirtualGraph &other, allocator_type alloc);

  VirtualGraph(const VirtualGraph &other) : VirtualGraph(other, other.get_allocator()) {}

  VirtualGraph(VirtualGraph &&other) noexcept = default;

  VirtualGraph(VirtualGraph &&other, allocator_type alloc);

  VirtualGraph &operator=(const VirtualGraph &) = default;
  VirtualGraph &operator=(VirtualGraph &&) = default;
  ~VirtualGraph() = default;

  // Dedup-by-real-vertex insert. If real_gid already maps to a VirtualNode in this
  // graph, returns the existing one (and the passed `node` is discarded). Otherwise
  // inserts `node` under its synthetic gid and records real_gid → node.Gid().
  const VirtualNode &InsertOrGetNode(storage::Gid real_gid, VirtualNode node);

  // Primary lookup by synthetic gid.
  [[nodiscard]] const VirtualNode *FindNode(storage::Gid synthetic_gid) const;

  // Find the canonical VirtualNode derived from a given real vertex. Returns nullptr
  // if no such mapping exists (either the real vertex wasn't derived, or this graph
  // wasn't produced by derive()).
  [[nodiscard]] const VirtualNode *FindNodeByRealGid(storage::Gid real_gid) const;

  // Edge operations. Returns true iff the (from, to, type) triple was not already present.
  bool InsertEdgeIfNew(VirtualEdge edge);

  [[nodiscard]] bool ContainsEdge(const VirtualEdge &edge) const { return edges_.contains(edge); }

  [[nodiscard]] EdgeRefView OutEdges(storage::Gid vertex_gid) const;
  [[nodiscard]] EdgeRefView InEdges(storage::Gid vertex_gid) const;

  // Views and sizes.
  [[nodiscard]] auto nodes() const noexcept { return std::views::all(*nodes_ptr_); }

  [[nodiscard]] auto edges() const noexcept { return std::views::all(edges_); }

  [[nodiscard]] auto node_count() const noexcept { return nodes_ptr_->size(); }

  [[nodiscard]] auto edge_count() const noexcept { return edges_.size(); }

  [[nodiscard]] auto nodes_empty() const noexcept { return nodes_ptr_->empty(); }

  [[nodiscard]] auto edges_empty() const noexcept { return edges_.empty(); }

  // Anchor for edges to carry: keeping any copy of this shared_ptr alive keeps
  // the node map alive. Callers constructing VirtualEdges that may escape the
  // graph's lifetime should pass the anchor to VirtualEdge's ctor.
  [[nodiscard]] std::shared_ptr<const void> NodesAnchor() const noexcept { return nodes_ptr_; }

  // Cross-graph union (used by parallel-aggregate reduce). Resolves real-gid
  // aliases between branches: each real vertex ends up with a single canonical
  // VirtualNode in the merged graph, and edges from `other` that pointed at
  // aliased nodes are rewritten to point at the canonical ones.
  void Merge(const VirtualGraph &other);
  void Merge(VirtualGraph &&other);

  [[nodiscard]] auto get_allocator() const noexcept -> allocator_type { return nodes_ptr_->get_allocator(); }

 private:
  void IndexEdge(const VirtualEdge *edge);
  void RebuildEdgeIndexes();

  // Rebind every edge in `source` to point at the corresponding nodes in this graph's
  // (already-populated) nodes_ptr_, and insert each as a new entry in edges_. Used by
  // the copy/move-with-allocator ctors when we had to duplicate the node map under a
  // new allocator. Edges' from_/to_ are re-resolved by synthetic gid lookup.
  void CopyEdgesRebound(const edge_set &source, allocator_type alloc);

  std::shared_ptr<node_map> nodes_ptr_;
  utils::pmr::unordered_map<storage::Gid, storage::Gid> real_to_virtual_;
  edge_set edges_;
  adjacency_map out_index_;
  adjacency_map in_index_;
};

}  // namespace memgraph::query
