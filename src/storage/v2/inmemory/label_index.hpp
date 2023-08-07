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

#include "storage/v2/indices/label_index.hpp"
#include "storage/v2/vertex.hpp"

namespace memgraph::storage {

struct LabelIndexStats {
  uint64_t count;
  double avg_degree;
};

using ParallelizedIndexCreationInfo =
    std::pair<std::vector<std::pair<Gid, uint64_t>> /*vertex_recovery_info*/, uint64_t /*thread_count*/>;

class InMemoryLabelIndex : public storage::LabelIndex {
 private:
  struct Entry {
    Vertex *vertex;
    uint64_t timestamp;

    bool operator<(const Entry &rhs) {
      return std::make_tuple(vertex, timestamp) < std::make_tuple(rhs.vertex, rhs.timestamp);
    }
    bool operator==(const Entry &rhs) const { return vertex == rhs.vertex && timestamp == rhs.timestamp; }
  };

 public:
  InMemoryLabelIndex(Indices *indices, Constraints *constraints, Config config);

  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) override;

  void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update, const Transaction &tx) override {}

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, utils::SkipList<Vertex>::Accessor vertices,
                   const std::optional<ParallelizedIndexCreationInfo> &parallel_exec_info);

  /// Returns false if there was no index to drop
  bool DropIndex(LabelId label) override;

  bool IndexExists(LabelId label) const override;

  std::vector<LabelId> ListIndices() const override;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp);

  class Iterable {
   public:
    Iterable(utils::SkipList<Entry>::Accessor index_accessor, LabelId label, View view, Transaction *transaction,
             Indices *indices, Constraints *constraints, const Config &config);

    class Iterator {
     public:
      Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator);

      VertexAccessor operator*() const { return current_vertex_accessor_; }

      bool operator==(const Iterator &other) const { return index_iterator_ == other.index_iterator_; }
      bool operator!=(const Iterator &other) const { return index_iterator_ != other.index_iterator_; }

      Iterator &operator++();

     private:
      void AdvanceUntilValid();

      Iterable *self_;
      utils::SkipList<Entry>::Iterator index_iterator_;
      VertexAccessor current_vertex_accessor_;
      Vertex *current_vertex_;
    };

    Iterator begin() { return {this, index_accessor_.begin()}; }
    Iterator end() { return {this, index_accessor_.end()}; }

   private:
    utils::SkipList<Entry>::Accessor index_accessor_;
    LabelId label_;
    View view_;
    Transaction *transaction_;
    Indices *indices_;
    Constraints *constraints_;
    Config config_;
  };

  uint64_t ApproximateVertexCount(LabelId label) const override;

  void RunGC();

  Iterable Vertices(LabelId label, View view, Transaction *transaction);

  void SetIndexStats(const storage::LabelId &label, const storage::LabelIndexStats &stats);

  std::optional<storage::LabelIndexStats> GetIndexStats(const storage::LabelId &label) const;

  std::vector<LabelId> ClearIndexStats();

  std::vector<LabelId> DeleteIndexStats(const storage::LabelId &label);

 private:
  std::map<LabelId, utils::SkipList<Entry>> index_;
  std::map<LabelId, storage::LabelIndexStats> stats_;
};

}  // namespace memgraph::storage
