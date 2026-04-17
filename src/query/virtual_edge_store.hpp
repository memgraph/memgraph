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

class VirtualEdgeStore {
 public:
  using allocator_type = utils::Allocator<VirtualEdgeStore>;

  explicit VirtualEdgeStore(allocator_type alloc) : edges_(alloc), out_index_(alloc), in_index_(alloc) {}

  // Copy/move ctors rebuild the per-vertex indexes because they hold pointers into edges_;
  // copying or moving across PMR allocators reallocates set nodes and invalidates those pointers.
  VirtualEdgeStore(const VirtualEdgeStore &other, allocator_type alloc)
      : edges_(other.edges_, alloc), out_index_(alloc), in_index_(alloc) {
    RebuildIndexes();
  }

  VirtualEdgeStore(VirtualEdgeStore &&other) noexcept = default;

  VirtualEdgeStore(VirtualEdgeStore &&other, allocator_type alloc)
      : edges_(std::move(other.edges_), alloc), out_index_(alloc), in_index_(alloc) {
    RebuildIndexes();
  }

  VirtualEdgeStore(const VirtualEdgeStore &other) : VirtualEdgeStore(other, other.edges_.get_allocator()) {}

  VirtualEdgeStore &operator=(const VirtualEdgeStore &) = default;
  VirtualEdgeStore &operator=(VirtualEdgeStore &&) = default;
  ~VirtualEdgeStore() = default;

  // Returns true iff the (from, to, type) triple was not already present.
  bool InsertIfNew(VirtualEdge edge);

  [[nodiscard]] bool Contains(const VirtualEdge &edge) const { return edges_.contains(edge); }

  [[nodiscard]] EdgeRefView OutEdges(storage::Gid vertex_gid) const;
  [[nodiscard]] EdgeRefView InEdges(storage::Gid vertex_gid) const;

  [[nodiscard]] auto edges() const { return std::views::all(edges_); }

  [[nodiscard]] auto size() const { return edges_.size(); }

  [[nodiscard]] auto empty() const { return edges_.empty(); }

  [[nodiscard]] auto get_allocator() const -> allocator_type { return edges_.get_allocator(); }

 private:
  // Single source of truth; VirtualEdge's operator==/hash are semantic (from, to, type),
  // so unordered_set membership is the dedup. References/pointers into unordered_set are
  // stable across insertions, which is why out_index_/in_index_ can hold raw pointers.
  utils::pmr::unordered_set<VirtualEdge> edges_;
  utils::pmr::unordered_map<storage::Gid, utils::pmr::vector<const VirtualEdge *>> out_index_;
  utils::pmr::unordered_map<storage::Gid, utils::pmr::vector<const VirtualEdge *>> in_index_;

  void IndexEdge(const VirtualEdge *edge);
  void RebuildIndexes();
};

}  // namespace memgraph::query
