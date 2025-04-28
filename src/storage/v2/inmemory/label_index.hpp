// Copyright 2025 Memgraph Ltd.
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

#include <span>

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/indices/indices_utils.hpp"
#include "storage/v2/indices/label_index.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

class InMemoryLabelIndex : public LabelIndex {
  struct Entry {
    Vertex *vertex;
    uint64_t timestamp;

    bool operator<(const Entry &rhs) { return std::tie(vertex, timestamp) < std::tie(rhs.vertex, rhs.timestamp); }
    bool operator==(const Entry &rhs) const {
      return std::tie(vertex, timestamp) == std::tie(rhs.vertex, rhs.timestamp);
    }
  };

 public:
  InMemoryLabelIndex() = default;

  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) override;

  void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update, const Transaction &tx) override {}

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, utils::SkipList<Vertex>::Accessor vertices,
                   const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info,
                   std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  /// Returns false if there was no index to drop
  bool DropIndex(LabelId label) override;

  bool IndexExists(LabelId label) const override;

  std::vector<LabelId> ListIndices() const override;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token);

  auto GetAbortProcessor() const -> AbortProcessor;

  /// Surgical removal of entries that was inserted this transaction
  void AbortEntries(std::map<LabelId, std::vector<Vertex *>> const &info, uint64_t start_timestamp) override;

  class Iterable {
   public:
    Iterable(utils::SkipList<Entry>::Accessor index_accessor, utils::SkipList<Vertex>::ConstAccessor vertices_accessor,
             LabelId label, View view, Storage *storage, Transaction *transaction);

    class Iterator {
     public:
      Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator);

      VertexAccessor const &operator*() const { return current_vertex_accessor_; }

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
    utils::SkipList<Vertex>::ConstAccessor pin_accessor_;
    utils::SkipList<Entry>::Accessor index_accessor_;
    LabelId label_;
    View view_;
    Storage *storage_;
    Transaction *transaction_;
  };

  uint64_t ApproximateVertexCount(LabelId label) const override;

  void RunGC();

  Iterable Vertices(LabelId label, View view, Storage *storage, Transaction *transaction);

  Iterable Vertices(LabelId label, memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor vertices_acc,
                    View view, Storage *storage, Transaction *transaction);

  void SetIndexStats(const storage::LabelId &label, const storage::LabelIndexStats &stats);

  std::optional<storage::LabelIndexStats> GetIndexStats(const storage::LabelId &label) const;

  std::vector<LabelId> ClearIndexStats();

  bool DeleteIndexStats(const storage::LabelId &label);

  void DropGraphClearIndices() override;

 private:
  std::map<LabelId, utils::SkipList<Entry>> index_;  // TODO: Auto-index approach means this MUST become synchronised
  utils::Synchronized<std::map<LabelId, storage::LabelIndexStats>, utils::ReadPrioritizedRWLock> stats_;
};

}  // namespace memgraph::storage
