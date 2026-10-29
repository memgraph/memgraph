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

#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/vertex.hpp"

namespace memgraph::storage {

class Storage;
struct Transaction;

/// The edges one expansion found, kept as the triples the vertex itself stores.
///
/// An array of accessors repeats, once per edge, the context that every edge of one expansion
/// shares. That context is held once here, and an accessor built from it on dereference.
///
/// The `Vertex *` and `EdgeRef` are the ones an accessor would hold, read under the same lock and
/// for the same span, so a concurrent writer can do to this what it could do to those. In
/// analytical mode an edge can leave adjacency at once, so a snapshot can name edges that no longer
/// exist; the memory behind them outlives the read, because analytical GC takes a WRITE hold on the
/// storage's main lock and an accessor in flight holds a shared one.
///
/// The disk engine filters for visibility as it collects, so it has accessors rather than triples
/// by the time it is done, and those are carried as they are.
class ExpandedEdges {
 public:
  ExpandedEdges() = default;

  /// The in-memory form: triples plus the context every edge of this expansion shares.
  ExpandedEdges(Edges edges, Vertex *from, Storage *storage, Transaction *transaction, EdgeDirection direction)
      : triples_(std::move(edges)),
        from_(from),
        storage_(storage),
        transaction_(transaction),
        direction_(direction),
        from_triples_(true) {}

  /// The form the disk engine already has.
  explicit ExpandedEdges(std::vector<EdgeAccessor> edges) : materialised_(std::move(edges)), from_triples_(false) {}

  class Iterator {
   public:
    /// Dereferencing builds the edge, so the older category claims only input: it promises a
    /// reference that outlives the dereference and there is none. Walking twice gives the same
    /// edges, which is all the concept asks of forward.
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
    if (from_triples_) [[likely]]
      return triples_.size();
    return materialised_.size();
  }

  auto empty() const -> bool { return size() == 0; }

  /// The edge at `index`, built now. Public so a layer above can wrap without materialising.
  auto At(std::size_t index) const -> EdgeAccessor {
    if (!from_triples_) [[unlikely]]
      return materialised_[index];
    auto const &[edge_type, other, edge] = triples_[index];
    // The triple names the far end of the edge; which end that is, is what the direction says.
    return direction_ == EdgeDirection::OUT ? EdgeAccessor{edge, edge_type, from_, other, storage_, transaction_}
                                            : EdgeAccessor{edge, edge_type, other, from_, storage_, transaction_};
  }

  /// The edge comes back by value, because there is none to refer to until it is asked for, so
  /// `auto &edge = edges[0]` has nothing to bind to and must be `auto`.
  auto operator[](std::size_t index) const -> EdgeAccessor { return At(index); }

  /// The edges `keep` accepts, held the same way: deciding means looking at every edge, but what
  /// comes back still builds its accessors when they are looked at.
  template <typename Predicate>
  auto Filtered(Predicate keep) const -> ExpandedEdges {
    if (!from_triples_) [[unlikely]] {
      auto kept = std::vector<EdgeAccessor>{};
      for (auto const &edge : materialised_) {
        if (keep(edge)) kept.push_back(edge);
      }
      return ExpandedEdges{std::move(kept)};
    }
    auto kept = Edges{triples_.get_allocator()};
    for (std::size_t index = 0; index != triples_.size(); ++index) {
      if (keep(At(index))) kept.push_back(triples_[index]);
    }
    return ExpandedEdges{std::move(kept), from_, storage_, transaction_, direction_};
  }

 private:
  Edges triples_;
  std::vector<EdgeAccessor> materialised_;
  Vertex *from_{nullptr};
  Storage *storage_{nullptr};
  Transaction *transaction_{nullptr};
  EdgeDirection direction_{EdgeDirection::OUT};
  /// Which of the two above holds the edges. Defaulting to the triples puts an expansion that
  /// found nothing on the same arm as one that found something; holding nothing, it never reaches
  /// the context the triples would need.
  bool from_triples_{true};
};

}  // namespace memgraph::storage
