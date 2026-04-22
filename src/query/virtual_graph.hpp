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
//  - nodes_ is keyed by original_gid (the real vertex gid the node was derived from).
//    dedup is by original_gid: two derive() calls touching the same real vertex resolve
//    to a single canonical VirtualNode.
//  - synthetic_to_original_ maps each VirtualNode's synthetic gid to its original_gid,
//    so callers holding a synthetic gid (from iterating or from another branch's edges)
//    can resolve back to the canonical node.
//  - edges_ is a unordered_set keyed by (from_gid, to_gid, type) via VirtualEdge's
//    semantic operator== / hash. pmr::unordered_set stores keys by const-value with
//    pointer stability across insertions — out_index_ and in_index_ hold const VirtualEdge*
//    into edges_'s buckets.
class VirtualGraph final {
 public:
  using allocator_type = utils::Allocator<VirtualGraph>;
  using node_map = utils::pmr::unordered_map<storage::Gid, VirtualNode>;
  using edge_set = utils::pmr::unordered_set<VirtualEdge>;
  using adjacency_map = utils::pmr::unordered_map<storage::Gid, utils::pmr::vector<const VirtualEdge *>>;

  explicit VirtualGraph(allocator_type alloc)
      : nodes_(alloc), synthetic_to_original_(alloc), edges_(alloc), out_index_(alloc), in_index_(alloc) {}

  VirtualGraph(const VirtualGraph &other, allocator_type alloc)
      : nodes_(other.nodes_, alloc),
        synthetic_to_original_(other.synthetic_to_original_, alloc),
        edges_(other.edges_, alloc),
        out_index_(alloc),
        in_index_(alloc) {
    RebuildEdgeIndexes();
  }

  VirtualGraph(const VirtualGraph &other) : VirtualGraph(other, other.nodes_.get_allocator()) {}

  VirtualGraph(VirtualGraph &&other) noexcept = default;

  VirtualGraph(VirtualGraph &&other, allocator_type alloc)
      : nodes_(std::move(other.nodes_), alloc),
        synthetic_to_original_(std::move(other.synthetic_to_original_), alloc),
        edges_(std::move(other.edges_), alloc),
        out_index_(alloc),
        in_index_(alloc) {
    // Same-allocator move: edges_ stole buckets, element addresses are stable, so the pointers
    // in other's indexes are still valid. Move the indexes instead of rebuilding.
    if (alloc == other.edges_.get_allocator()) {
      out_index_ = std::move(other.out_index_);
      in_index_ = std::move(other.in_index_);
    } else {
      RebuildEdgeIndexes();
    }
  }

  VirtualGraph &operator=(const VirtualGraph &) = default;
  VirtualGraph &operator=(VirtualGraph &&) = default;
  ~VirtualGraph() = default;

  // Node operations.
  const VirtualNode &InsertOrGetNode(VirtualNode node);

  [[nodiscard]] const VirtualNode *FindNode(storage::Gid original_gid) const;
  [[nodiscard]] const VirtualNode *FindNodeBySyntheticGid(storage::Gid synthetic_gid) const;

  [[nodiscard]] bool ContainsNode(storage::Gid original_gid) const { return nodes_.contains(original_gid); }

  // Edge operations. Returns true iff the (from, to, type) triple was not already present.
  bool InsertEdgeIfNew(VirtualEdge edge);

  [[nodiscard]] bool ContainsEdge(const VirtualEdge &edge) const { return edges_.contains(edge); }

  [[nodiscard]] EdgeRefView OutEdges(storage::Gid vertex_gid) const;
  [[nodiscard]] EdgeRefView InEdges(storage::Gid vertex_gid) const;

  // Views and sizes.
  [[nodiscard]] auto nodes() const noexcept { return std::views::all(nodes_); }

  [[nodiscard]] auto edges() const noexcept { return std::views::all(edges_); }

  [[nodiscard]] auto node_count() const noexcept { return nodes_.size(); }

  [[nodiscard]] auto edge_count() const noexcept { return edges_.size(); }

  [[nodiscard]] auto nodes_empty() const noexcept { return nodes_.empty(); }

  [[nodiscard]] auto edges_empty() const noexcept { return edges_.empty(); }

  // Cross-graph union (used by parallel-aggregate reduce).
  void Merge(const VirtualGraph &other);
  void Merge(VirtualGraph &&other);

  [[nodiscard]] auto get_allocator() const noexcept -> allocator_type { return nodes_.get_allocator(); }

 private:
  void IndexEdge(const VirtualEdge *edge);
  void RebuildEdgeIndexes();

  node_map nodes_;
  utils::pmr::unordered_map<storage::Gid, storage::Gid> synthetic_to_original_;
  edge_set edges_;
  adjacency_map out_index_;
  adjacency_map in_index_;
};

}  // namespace memgraph::query
