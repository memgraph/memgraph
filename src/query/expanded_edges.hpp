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

#include <cstddef>
#include <iterator>
#include <utility>
#include <vector>

#include "query/edge_accessor.hpp"
#include "storage/v2/edge_accessor.hpp"

namespace memgraph::query {

/// The edges one expansion found, wrapped as the query layer sees them only when looked at.
///
/// A `query::EdgeAccessor` is a `storage::EdgeAccessor` and nothing else, so building a vector of
/// the first from a vector of the second copies the whole list to change only its type. For a
/// supernode that is half a gigabyte of pages the kernel has to fault in and zero before anything
/// can be written to them, to hold values that are walked once and then thrown away.
///
/// The wrapping happens on dereference instead. What is held is the list the storage layer
/// already built.
class ExpandedEdges {
 public:
  ExpandedEdges() = default;

  /// The usual case: wrap as we go.
  explicit ExpandedEdges(std::vector<storage::EdgeAccessor> edges) : storage_edges_(std::move(edges)) {}

  /// For a caller that has already built the query-layer list, such as one that filtered it.
  explicit ExpandedEdges(std::vector<EdgeAccessor> edges) : wrapped_edges_(std::move(edges)), wrap_on_demand_(false) {}

  class Iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
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

  auto size() const -> std::size_t { return wrap_on_demand_ ? storage_edges_.size() : wrapped_edges_.size(); }

  auto empty() const -> bool { return size() == 0; }

 private:
  auto At(std::size_t index) const -> EdgeAccessor {
    if (wrap_on_demand_) return EdgeAccessor{storage_edges_[index]};
    return wrapped_edges_[index];
  }

  std::vector<storage::EdgeAccessor> storage_edges_;
  std::vector<EdgeAccessor> wrapped_edges_;
  bool wrap_on_demand_{true};
};

struct ExpandedEdgesResult {
  ExpandedEdges edges;
  int64_t expanded_count;
};

}  // namespace memgraph::query
