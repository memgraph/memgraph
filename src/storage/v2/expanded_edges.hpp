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
/// Expanding built a second array as long as the vertex's degree, holding one `EdgeAccessor` per
/// edge. Every field of those accessors except the triple is the same for all of them, so the array
/// was one shared context repeated a degree number of times. For a supernode that is half a
/// gigabyte, and what it costs is mostly the kernel faulting in the pages to write it to: asking
/// `perf` for page faults rather than cycles put every one of them in the function that built it.
///
/// The accessor is made when the iterator is dereferenced instead, from the triple and the context
/// held once here.
///
/// This holds the same `Vertex *` and `EdgeRef` the accessors held, taken under the same lock, for
/// the same span - so what a concurrent writer can do to it is exactly what it could do before. In
/// analytical mode that is: delete an edge and have it vanish from adjacency straight away, so a
/// snapshot can name edges that no longer exist. The memory behind them survives the read, because
/// analytical GC needs a WRITE hold on the storage's main lock and an accessor in flight holds a
/// shared one.
///
/// The disk engine filters its edges for visibility as it collects them, so by the time it is done
/// it has accessors rather than triples. Those are carried as they are.
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
  explicit ExpandedEdges(std::vector<EdgeAccessor> edges) : materialised_(std::move(edges)) {}

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

  auto size() const -> std::size_t { return from_triples_ ? triples_.size() : materialised_.size(); }

  auto empty() const -> bool { return size() == 0; }

  /// The edge at `index`, built now. Public so a layer above can wrap without materialising.
  auto At(std::size_t index) const -> EdgeAccessor {
    if (!from_triples_) return materialised_[index];
    auto const &[edge_type, other, edge] = triples_[index];
    // The triple names the far end of the edge; which end that is, is what the direction says.
    return direction_ == EdgeDirection::OUT ? EdgeAccessor{edge, edge_type, from_, other, storage_, transaction_}
                                            : EdgeAccessor{edge, edge_type, other, from_, storage_, transaction_};
  }

  /// The sequence accessors, so a caller that only wants one edge reads like it is holding a
  /// sequence of them. Each returns the edge by value, because there is no edge to refer to until
  /// it is asked for - which is the point of this type, and the one way it does not substitute for
  /// the vector it replaced: `auto &edge = edges[0]` has nothing to bind to and must be `auto`.
  auto operator[](std::size_t index) const -> EdgeAccessor { return At(index); }

  auto at(std::size_t index) const -> EdgeAccessor { return At(index); }

  auto front() const -> EdgeAccessor { return At(0); }

  auto back() const -> EdgeAccessor { return At(size() - 1); }

  /// For a caller that needs the array after all. Nothing on a hot path should want this.
  auto Materialise() const -> std::vector<EdgeAccessor> {
    if (!from_triples_) return materialised_;
    auto built = std::vector<EdgeAccessor>{};
    built.reserve(triples_.size());
    for (std::size_t index = 0; index != triples_.size(); ++index) built.push_back(At(index));
    return built;
  }

 private:
  Edges triples_;
  std::vector<EdgeAccessor> materialised_;
  Vertex *from_{nullptr};
  Storage *storage_{nullptr};
  Transaction *transaction_{nullptr};
  EdgeDirection direction_{EdgeDirection::OUT};
  bool from_triples_{false};
};

}  // namespace memgraph::storage
