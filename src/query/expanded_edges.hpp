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

#include <concepts>
#include <cstddef>
#include <iterator>
#include <utility>
#include <vector>

#include "query/edge_accessor.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/expanded_edges.hpp"

namespace memgraph::query {

/// The edges one expansion found, wrapped as the query layer sees them only when looked at.
///
/// Without a branch a `query::EdgeAccessor` is just a `storage::EdgeAccessor`, so a vector of the first
/// built from a vector of the second would copy the whole list to change only its type.
class ExpandedEdges {
 public:
  ExpandedEdges() = default;

  explicit ExpandedEdges(storage::ExpandedEdges edges) : storage_edges_(std::move(edges)) {}

#ifdef MG_ENTERPRISE
  /// A branch's edges come from its overlay and each carries the branch context, so they are held built.
  explicit ExpandedEdges(std::vector<EdgeAccessor> edges) : branch_edges_(std::move(edges)), from_storage_(false) {}
#endif

  class Iterator {
   public:
    using iterator_concept = std::forward_iterator_tag;
    using iterator_category = std::input_iterator_tag;
    using value_type = EdgeAccessor;
    using difference_type = std::ptrdiff_t;

    Iterator() = default;

    Iterator(ExpandedEdges const *owner, std::size_t index) : owner_(owner), index_(index) {}

    auto operator*() const -> EdgeAccessor { return owner_->At(index_); }

    Iterator &operator++() {
      ++index_;
      return *this;
    }

    Iterator operator++(int) {
      auto copy = *this;
      ++index_;
      return copy;
    }

    friend bool operator==(Iterator const &lhs, Iterator const &rhs) { return lhs.index_ == rhs.index_; }

   private:
    ExpandedEdges const *owner_{nullptr};
    std::size_t index_{0};
  };

  auto begin() const -> Iterator { return Iterator{this, 0}; }

  auto end() const -> Iterator { return Iterator{this, size()}; }

  auto size() const -> std::size_t {
#ifdef MG_ENTERPRISE
    if (!from_storage_) [[unlikely]]
      return branch_edges_.size();
#endif
    return storage_edges_.size();
  }

  auto empty() const -> bool { return size() == 0; }

  auto operator[](std::size_t index) const -> EdgeAccessor { return At(index); }

  template <typename Predicate>
  auto Filtered(Predicate keep) const -> ExpandedEdges {
#ifdef MG_ENTERPRISE
    if (!from_storage_) [[unlikely]] {
      auto kept = std::vector<EdgeAccessor>{};
      for (auto const &edge : branch_edges_) {
        if (keep(edge)) kept.push_back(edge);
      }
      return ExpandedEdges{std::move(kept)};
    }
#endif
    return ExpandedEdges{storage_edges_.Filtered(
        [&keep](storage::EdgeAccessor const &edge) -> bool { return keep(EdgeAccessor{edge}); })};
  }

  /// Worth calling, over walking the expansion, only where visiting an edge waits on memory: an
  /// edge is built as it is looked at, so collecting first lets several of those waits be
  /// outstanding at once. `out` keeps its capacity.
  template <typename Collection>
    requires std::same_as<typename Collection::value_type, EdgeAccessor>
  void MaterialiseInto(Collection &out) const {
    auto const count = size();
    out.clear();
    out.reserve(count);
    for (auto index = std::size_t{0}; index != count; ++index) out.push_back(At(index));
  }

 private:
  auto At(std::size_t index) const -> EdgeAccessor {
#ifdef MG_ENTERPRISE
    if (!from_storage_) [[unlikely]]
      return branch_edges_[index];
#endif
    return EdgeAccessor{storage_edges_.At(index)};
  }

  storage::ExpandedEdges storage_edges_;
#ifdef MG_ENTERPRISE
  std::vector<EdgeAccessor> branch_edges_;
  bool from_storage_{true};
#endif
};

struct ExpandedEdgesResult {
  ExpandedEdges edges;
  int64_t expanded_count;
};

}  // namespace memgraph::query
