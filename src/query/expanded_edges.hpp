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

#include "query/edge_accessor.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/expanded_edges.hpp"

namespace memgraph::query {

/// The edges one expansion found, wrapped as the query layer sees them only when looked at.
///
/// A `query::EdgeAccessor` is a `storage::EdgeAccessor` and nothing else, so a vector of the first
/// built from a vector of the second copies the whole list to change only its type.
class ExpandedEdges {
 public:
  ExpandedEdges() = default;

  explicit ExpandedEdges(storage::ExpandedEdges edges) : storage_edges_(std::move(edges)) {}

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

  auto size() const -> std::size_t { return storage_edges_.size(); }

  auto operator[](std::size_t index) const -> EdgeAccessor { return At(index); }

  /// Builds every edge into `out`, replacing what it held.
  ///
  /// An expansion builds each edge only as it is looked at, so a caller doing work per edge
  /// interleaves the two. Collecting first pays where visiting an edge waits on memory, since
  /// several of those waits can then be outstanding at once. `out` keeps its capacity, so
  /// expanding many vertices into one collection stops allocating once it has taken the widest
  /// degree met.
  template <typename Collection>
    requires std::same_as<typename Collection::value_type, EdgeAccessor>
  void MaterialiseInto(Collection &out) const {
    auto const count = size();
    out.clear();
    out.reserve(count);
    for (auto index = std::size_t{0}; index != count; ++index) out.push_back(At(index));
  }

 private:
  auto At(std::size_t index) const -> EdgeAccessor { return EdgeAccessor{storage_edges_.At(index)}; }

  storage::ExpandedEdges storage_edges_;
};

struct ExpandedEdgesResult {
  ExpandedEdges edges;
  int64_t expanded_count;
};

}  // namespace memgraph::query
