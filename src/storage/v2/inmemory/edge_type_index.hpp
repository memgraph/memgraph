// Copyright 2024 Memgraph Ltd.
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

#include <map>
#include <utility>

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/edge_type_index.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

class InMemoryEdgeTypeIndex : public storage::EdgeTypeIndex {
 private:
  struct Entry {
    Vertex *from_vertex;
    Vertex *to_vertex;

    Edge *edge;

    uint64_t timestamp;

    bool operator<(const Entry &rhs) const {
      return std::tie(edge->gid, from_vertex->gid, to_vertex->gid, timestamp) <
             std::tie(rhs.edge->gid, rhs.from_vertex->gid, rhs.to_vertex->gid, rhs.timestamp);
    }
    bool operator==(const Entry &rhs) const {
      return std::tie(edge, from_vertex, to_vertex, timestamp) ==
             std::tie(rhs.edge, rhs.from_vertex, rhs.to_vertex, rhs.timestamp);
    }
  };

 public:
  InMemoryEdgeTypeIndex() = default;

  /// @throw std::bad_alloc
  bool CreateIndex(EdgeTypeId edge_type, utils::SkipList<Vertex>::Accessor vertices);

  /// Returns false if there was no index to drop
  bool DropIndex(EdgeTypeId edge_type) override;

  bool IndexExists(EdgeTypeId edge_type) const override;

  std::vector<EdgeTypeId> ListIndices() const override;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token);

  void AbortEntries(EdgeTypeId edge_type, std::span<std::tuple<Vertex *const, Vertex *const, Edge *const> const> edges,
                    uint64_t exact_start_timestamp);

  uint64_t ApproximateEdgeCount(EdgeTypeId edge_type) const override;

  void UpdateOnEdgeCreation(Vertex *from, Vertex *to, EdgeRef edge_ref, EdgeTypeId edge_type,
                            const Transaction &tx) override;

  void UpdateOnEdgeModification(Vertex *old_from, Vertex *old_to, Vertex *new_from, Vertex *new_to, EdgeRef edge_ref,
                                EdgeTypeId edge_type, const Transaction &tx) override;

  void DropGraphClearIndices() override;

  std::vector<EdgeTypeId> Analysis() const;

  static constexpr std::size_t kEdgeTypeIdPos = 0U;
  static constexpr std::size_t kVertexPos = 1U;
  static constexpr std::size_t kEdgeRefPos = 2U;

  class Iterable {
   public:
    Iterable(utils::SkipList<Entry>::Accessor index_accessor, EdgeTypeId edge_type, View view, Storage *storage,
             Transaction *transaction);

    class Iterator {
     public:
      Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator);

      EdgeAccessor const &operator*() const { return current_accessor_; }

      bool operator==(const Iterator &other) const { return index_iterator_ == other.index_iterator_; }
      bool operator!=(const Iterator &other) const { return index_iterator_ != other.index_iterator_; }

      Iterator &operator++();

     private:
      void AdvanceUntilValid();

      Iterable *self_;
      utils::SkipList<Entry>::Iterator index_iterator_;
      EdgeRef current_edge_{nullptr};
      EdgeAccessor current_accessor_;
    };

    Iterator begin() { return {this, index_accessor_.begin()}; }
    Iterator end() { return {this, index_accessor_.end()}; }

   private:
    utils::SkipList<Entry>::Accessor index_accessor_;
    [[maybe_unused]] EdgeTypeId edge_type_;
    View view_;
    Storage *storage_;
    Transaction *transaction_;
  };

  void RunGC();

  Iterable Edges(EdgeTypeId edge_type, View view, Storage *storage, Transaction *transaction);

 private:
  std::map<EdgeTypeId, utils::SkipList<Entry>> index_;  // This should be a std::map because we use it with assumption
                                                        // that it's sorted
};

}  // namespace memgraph::storage
