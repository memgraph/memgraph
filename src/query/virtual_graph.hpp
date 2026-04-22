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
//    shared_ptr<const void> anchor that is this same control block, so edges
//    that escape the graph (collect(e), g.edges returned from a subquery)
//    keep the node map alive through their refcount.
//  - nodes_ptr_'s map is keyed by original_gid (the real vertex gid the node
//    was derived from). Dedup is by original_gid: two derive() calls touching
//    the same real vertex resolve to a single canonical VirtualNode.
//  - synthetic_to_original_ maps each VirtualNode's synthetic gid to its
//    original_gid, so callers holding a synthetic gid can resolve back to
//    the canonical node.
//  - edges_ is a unordered_set keyed by (from_gid, to_gid, type) via
//    VirtualEdge's semantic operator== / hash. pmr::unordered_set stores
//    keys by const-value with pointer stability across insertions — out_index_
//    and in_index_ hold const VirtualEdge* into edges_'s buckets.
class VirtualGraph final {
 public:
  using allocator_type = utils::Allocator<VirtualGraph>;
  using node_map = utils::pmr::unordered_map<storage::Gid, VirtualNode>;
  using edge_set = utils::pmr::unordered_set<VirtualEdge>;
  using adjacency_map = utils::pmr::unordered_map<storage::Gid, utils::pmr::vector<const VirtualEdge *>>;

  explicit VirtualGraph(allocator_type alloc)
      : nodes_ptr_(std::make_shared<node_map>(alloc)),
        synthetic_to_original_(alloc),
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

  // Node operations.
  const VirtualNode &InsertOrGetNode(VirtualNode node);

  [[nodiscard]] const VirtualNode *FindNode(storage::Gid original_gid) const;
  [[nodiscard]] const VirtualNode *FindNodeBySyntheticGid(storage::Gid synthetic_gid) const;

  [[nodiscard]] bool ContainsNode(storage::Gid original_gid) const { return nodes_ptr_->contains(original_gid); }

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

  // Cross-graph union (used by parallel-aggregate reduce).
  void Merge(const VirtualGraph &other);
  void Merge(VirtualGraph &&other);

  [[nodiscard]] auto get_allocator() const noexcept -> allocator_type { return nodes_ptr_->get_allocator(); }

 private:
  void IndexEdge(const VirtualEdge *edge);
  void RebuildEdgeIndexes();

  // Rebind every edge in `source` to point at the corresponding nodes in this graph's
  // (already-populated) nodes_ptr_, and insert each as a new entry in edges_. Used by
  // the copy/move-with-allocator ctors when we had to duplicate the node map under a
  // new allocator.
  void CopyEdgesRebound(const edge_set &source, allocator_type alloc);

  std::shared_ptr<node_map> nodes_ptr_;
  utils::pmr::unordered_map<storage::Gid, storage::Gid> synthetic_to_original_;
  edge_set edges_;
  adjacency_map out_index_;
  adjacency_map in_index_;
};

}  // namespace memgraph::query
