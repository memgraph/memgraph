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

#include <cstdint>
#include <map>
#include <utility>

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/edge_type_property_index.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

class InMemoryEdgeTypePropertyIndex : public storage::EdgeTypePropertyIndex {
 private:
  struct Entry {
    // do we need this?
    PropertyValue value;
    Vertex *from_vertex;
    Vertex *to_vertex;
    Edge *edge;

    uint64_t timestamp;

    bool operator<(const Entry &rhs) const;
    bool operator==(const Entry &rhs) const;
    bool operator<(const PropertyValue &rhs) const;
    bool operator==(const PropertyValue &rhs) const;
  };

 public:
  InMemoryEdgeTypePropertyIndex() = default;

  /// @throw std::bad_alloc
  bool CreateIndex(EdgeTypeId edge_type, PropertyId property, utils::SkipList<Vertex>::Accessor vertices);

  /// Returns false if there was no index to drop
  bool DropIndex(EdgeTypeId edge_type, PropertyId property) override;

  bool IndexExists(EdgeTypeId edge_type, PropertyId property) const override;

  std::vector<std::pair<EdgeTypeId, PropertyId>> ListIndices() const override;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token);

  uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property) const override;

  // Functions that update the index
  void UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge, EdgeTypeId edge_type,
                           PropertyId property, PropertyValue value, uint64_t timestamp) override;

  void UpdateOnEdgeModification(Vertex *old_from, Vertex *old_to, Vertex *new_from, Vertex *new_to, EdgeRef edge_ref,
                                EdgeTypeId edge_type, PropertyId property, PropertyValue value,
                                const Transaction &tx) override;

  void DropGraphClearIndices() override;

  static constexpr std::size_t kEdgeTypeIdPos = 0U;
  static constexpr std::size_t kVertexPos = 1U;
  static constexpr std::size_t kEdgeRefPos = 2U;

  class Iterable {
   public:
    Iterable(utils::SkipList<Entry>::Accessor index_accessor, EdgeTypeId edge_type, PropertyId property, View view,
             Storage *storage, Transaction *transaction);

    class Iterator {
     public:
      Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator);

      EdgeAccessor const &operator*() const { return current_edge_accessor_; }

      bool operator==(const Iterator &other) const { return index_iterator_ == other.index_iterator_; }
      bool operator!=(const Iterator &other) const { return index_iterator_ != other.index_iterator_; }

      Iterator &operator++();

     private:
      void AdvanceUntilValid();
      std::tuple<EdgeRef, EdgeTypeId, Vertex *, Vertex *> GetEdgeInfo();

      Iterable *self_;
      utils::SkipList<Entry>::Iterator index_iterator_;
      EdgeAccessor current_edge_accessor_;
      EdgeRef current_edge_{nullptr};
    };

    Iterator begin() { return {this, index_accessor_.begin()}; }
    Iterator end() { return {this, index_accessor_.end()}; }

   private:
    utils::SkipList<Entry>::Accessor index_accessor_;
    EdgeTypeId edge_type_;
    PropertyId property_;
    View view_;
    Storage *storage_;
    Transaction *transaction_;
  };

  void RunGC();

  Iterable Edges(EdgeTypeId edge_type, PropertyId property, View view, Storage *storage, Transaction *transaction);

 private:
  std::map<std::pair<EdgeTypeId, PropertyId>, utils::SkipList<Entry>> index_;
};

}  // namespace memgraph::storage
