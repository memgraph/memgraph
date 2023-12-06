// Copyright 2023 Memgraph Ltd.
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
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

class InMemoryEdgeTypeIndex : public storage::EdgeTypeIndex {
 private:
  // Here we have two options,
  // 1. store the from- and to- vertices only, and figure out the
  //    rest of the needed information runtime.
  // 2. store the from-, the to- vertices and the EdgeRef itself.
  //    if two is the case we might be wasting memory because if
  //    properties are enabled on edges then we are storing the
  //    same information twice.
  struct Entry {
    Vertex *from_vertex;
    Vertex *to_vertex;

    uint64_t timestamp;

    bool operator<(const Entry &rhs) {
      return std::make_tuple(from_vertex, to_vertex, timestamp) <
             std::make_tuple(rhs.from_vertex, rhs.to_vertex, rhs.timestamp);
    }
    bool operator==(const Entry &rhs) const {
      return from_vertex == rhs.from_vertex && to_vertex == rhs.to_vertex && timestamp == rhs.timestamp;
    }
  };

 public:
  InMemoryEdgeTypeIndex() = default;

  // I don't think these are relevant since the edgetype is assigned at creation and can't be changed.
  // /// @throw std::bad_alloc
  // void UpdateOnAddLabel(EdgeTypeId added_label, Edge *edge_after_update, const Transaction &tx) override;

  // void UpdateOnRemoveLabel(EdgeTypeId removed_label, const Transaction &tx) override {}

  /// @throw std::bad_alloc
  bool CreateIndex(EdgeTypeId edge_type, utils::SkipList<Vertex>::Accessor vertices);

  /// Returns false if there was no index to drop
  bool DropIndex(EdgeTypeId label) override;

  bool IndexExists(EdgeTypeId label) const override;

  std::vector<EdgeTypeId> ListIndices() const override;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp);

  uint64_t ApproximateEdgeCount(EdgeTypeId edge_type) const override;

  class Iterable {
   public:
    Iterable(utils::SkipList<Entry>::Accessor index_accessor, EdgeTypeId label, View view, Storage *storage,
             Transaction *transaction);

    class Iterator {
     public:
      Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator);

      EdgeAccessor const &operator*() const { return current_edge_accessor_; }

      bool operator==(const Iterator &other) const { return index_iterator_ == other.index_iterator_; }
      bool operator!=(const Iterator &other) const { return index_iterator_ != other.index_iterator_; }

      Iterator &operator++();

     private:
      void AdvanceUntilValid();

      Iterable *self_;
      utils::SkipList<Entry>::Iterator index_iterator_;
      EdgeAccessor current_edge_accessor_;
      // current edge might be a bad idea.
      EdgeRef current_edge_{nullptr};
    };

    Iterator begin() { return {this, index_accessor_.begin()}; }
    Iterator end() { return {this, index_accessor_.end()}; }

   private:
    utils::SkipList<Entry>::Accessor index_accessor_;
    EdgeTypeId label_;
    View view_;
    Storage *storage_;
    Transaction *transaction_;
  };

  void RunGC();

  Iterable Edges(EdgeTypeId label, View view, Storage *storage, Transaction *transaction);

  // TODO(gvolfing) - check this.
  // void SetIndexStats(const storage::LabelId &label, const storage::LabelIndexStats &stats);
  // std::optional<storage::LabelIndexStats> GetIndexStats(const storage::LabelId &label) const;
  // std::vector<LabelId> ClearIndexStats();
  // bool DeleteIndexStats(const storage::LabelId &label);

 private:
  // std::map<LabelId, utils::SkipList<Entry>> index_;
  std::map<EdgeTypeId, utils::SkipList<Entry>> index_;
  // utils::Synchronized<std::map<LabelId, storage::LabelIndexStats>, utils::ReadPrioritizedRWLock> stats_;
};

}  // namespace memgraph::storage
