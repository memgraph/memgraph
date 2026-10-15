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
#include "storage/v2/expanded_edges.hpp"

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

  explicit ExpandedEdges(storage::ExpandedEdges edges) : storage_edges_(std::move(edges)) {}

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

  auto size() const -> std::size_t { return storage_edges_.size(); }

  auto empty() const -> bool { return size() == 0; }

  /// The edge at `index`, built now. For a caller that holds a position rather than an iterator,
  /// which is how it keeps that position across a call that hands control back to its own caller.
  /// The edge comes back by value, because there is none to refer to until it is asked for.
  auto operator[](std::size_t index) const -> EdgeAccessor { return At(index); }

  /// The array, for a caller that needs one after all, which is every caller that has not moved to
  /// taking this range. Nothing on a hot path should want it: building it is the cost this type
  /// exists to avoid.
  auto Materialise() const -> std::vector<EdgeAccessor> {
    auto built = std::vector<EdgeAccessor>{};
    built.reserve(size());
    for (auto const &edge : *this) built.emplace_back(edge);
    return built;
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
