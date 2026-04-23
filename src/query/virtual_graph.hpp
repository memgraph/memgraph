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

// Maps synthetic gids in one VirtualGraph (the "aliased" one) to the synthetic
// gid of the canonical VirtualNode in another VirtualGraph. Used by Merge when
// the caller knows that two VirtualNodes in the two graphs represent the same
// external entity (e.g. same real vertex under derive aggregation) and wants
// the merged graph to collapse them.
using VirtualGraphAliasMap = utils::pmr::unordered_map<storage::Gid, storage::Gid>;

// Invariants:
//  - nodes_ptr_ is a shared_ptr<node_map> so VirtualEdges can carry a
//    type-erased copy as an anchor; edges that escape the graph (collect(e),
//    g.edges returned from a subquery) keep the node map alive via the refcount.
//  - The node map is keyed by VirtualNode::Gid() (synthetic).
//  - edges_ is dedup'd on VirtualEdge's semantic (from_synth, to_synth, type)
//    hash/eq. pmr::unordered_set guarantees element-address stability, so
//    out_index_/in_index_ can hold raw const VirtualEdge* into its buckets.
class VirtualGraph final {
 public:
  using allocator_type = utils::Allocator<VirtualGraph>;
  using node_map = utils::pmr::unordered_map<storage::Gid, VirtualNode>;
  using edge_set = utils::pmr::unordered_set<VirtualEdge>;
  using adjacency_map = utils::pmr::unordered_map<storage::Gid, utils::pmr::vector<const VirtualEdge *>>;

  explicit VirtualGraph(allocator_type alloc)
      : nodes_ptr_(std::make_shared<node_map>(alloc)), edges_(alloc), out_index_(alloc), in_index_(alloc) {}

  VirtualGraph(const VirtualGraph &other, allocator_type alloc);

  VirtualGraph(const VirtualGraph &other) : VirtualGraph(other, other.get_allocator()) {}

  VirtualGraph(VirtualGraph &&other) noexcept = default;

  VirtualGraph(VirtualGraph &&other, allocator_type alloc);

  VirtualGraph &operator=(const VirtualGraph &) = default;
  VirtualGraph &operator=(VirtualGraph &&) = default;
  ~VirtualGraph() = default;

  // Insert a VirtualNode keyed by its own synthetic gid. Assumes the synth gid
  // is unique within this graph (holds by construction — NextSyntheticGid is a
  // process-wide monotonic counter).
  const VirtualNode &InsertNode(VirtualNode node);

  [[nodiscard]] const VirtualNode *FindNode(storage::Gid synthetic_gid) const;

  // Returns true iff the (from, to, type) triple was not already present.
  bool InsertEdgeIfNew(VirtualEdge edge);

  [[nodiscard]] bool ContainsEdge(const VirtualEdge &edge) const { return edges_.contains(edge); }

  [[nodiscard]] EdgeRefView OutEdges(storage::Gid vertex_gid) const;
  [[nodiscard]] EdgeRefView InEdges(storage::Gid vertex_gid) const;

  [[nodiscard]] auto nodes() const noexcept { return std::views::all(*nodes_ptr_); }

  [[nodiscard]] auto edges() const noexcept { return std::views::all(edges_); }

  // Callers that construct VirtualEdges which may escape the graph's lifetime must
  // pass this anchor to the edge's ctor so the node map outlives them.
  [[nodiscard]] std::shared_ptr<const void> NodesAnchor() const noexcept { return nodes_ptr_; }

  // Merge other's nodes and edges into this graph. `aliases` maps a synthetic gid
  // in `other` to the synthetic gid of an already-canonical VirtualNode in this
  // graph; entries cause other's aliased node to be skipped (we already have a
  // canonical) and edges referencing that synth gid to be rewritten to point at
  // the canonical. Synth gids not in `aliases` are assumed unique across the two
  // graphs (true by construction of NextSyntheticGid) and copied through.
  void Merge(const VirtualGraph &other, const VirtualGraphAliasMap &aliases);

  [[nodiscard]] auto get_allocator() const noexcept -> allocator_type { return nodes_ptr_->get_allocator(); }

 private:
  void IndexEdge(const VirtualEdge *edge);
  void RebuildEdgeIndexes();

  // Re-resolve each edge in `source` to endpoints in this graph's nodes_ptr_ and
  // insert as a new entry. Used by the copy/move-with-allocator ctors when the
  // node map was duplicated under a new allocator.
  void CopyEdgesRebound(const edge_set &source, allocator_type alloc);

  std::shared_ptr<node_map> nodes_ptr_;
  edge_set edges_;
  adjacency_map out_index_;
  adjacency_map in_index_;
};

}  // namespace memgraph::query
