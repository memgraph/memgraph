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

#include "storage/v2/indices/label_property_index.hpp"

namespace memgraph::storage {

struct LabelPropertyIndexStats {
  uint64_t count, distinct_values_count;
  double statistic, avg_group_size, avg_degree;
};

/// TODO: andi. Too many copies, extract at one place
using ParallelizedIndexCreationInfo =
    std::pair<std::vector<std::pair<Gid, uint64_t>> /*vertex_recovery_info*/, uint64_t /*thread_count*/>;

class InMemoryLabelPropertyIndex : public storage::LabelPropertyIndex {
 private:
  struct Entry {
    PropertyValue value;
    Vertex *vertex;
    uint64_t timestamp;

    bool operator<(const Entry &rhs) const;
    bool operator==(const Entry &rhs) const;

    bool operator<(const PropertyValue &rhs) const;
    bool operator==(const PropertyValue &rhs) const;
  };

 public:
  InMemoryLabelPropertyIndex(Indices *indices, Constraints *constraints, const Config &config);

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, PropertyId property, utils::SkipList<Vertex>::Accessor vertices,
                   const std::optional<ParallelizedIndexCreationInfo> &parallel_exec_info);

  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) override;

  void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update, const Transaction &tx) override {}

  /// @throw std::bad_alloc
  void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                           const Transaction &tx) override;

  bool DropIndex(LabelId label, PropertyId property) override;

  bool IndexExists(LabelId label, PropertyId property) const override;

  std::vector<std::pair<LabelId, PropertyId>> ListIndices() const override;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp);

  class Iterable {
   public:
    Iterable(utils::SkipList<Entry>::Accessor index_accessor, LabelId label, PropertyId property,
             const std::optional<utils::Bound<PropertyValue>> &lower_bound,
             const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Transaction *transaction,
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

    Iterator begin();
    Iterator end();

   private:
    utils::SkipList<Entry>::Accessor index_accessor_;
    LabelId label_;
    PropertyId property_;
    std::optional<utils::Bound<PropertyValue>> lower_bound_;
    std::optional<utils::Bound<PropertyValue>> upper_bound_;
    bool bounds_valid_{true};
    View view_;
    Transaction *transaction_;
    Indices *indices_;
    Constraints *constraints_;
    Config config_;
  };

  uint64_t ApproximateVertexCount(LabelId label, PropertyId property) const override;

  /// Supplying a specific value into the count estimation function will return
  /// an estimated count of nodes which have their property's value set to
  /// `value`. If the `value` specified is `Null`, then an average number of
  /// equal elements is returned.
  uint64_t ApproximateVertexCount(LabelId label, PropertyId property, const PropertyValue &value) const override;

  uint64_t ApproximateVertexCount(LabelId label, PropertyId property,
                                  const std::optional<utils::Bound<PropertyValue>> &lower,
                                  const std::optional<utils::Bound<PropertyValue>> &upper) const override;

  std::vector<std::pair<LabelId, PropertyId>> ClearIndexStats();

  std::vector<std::pair<LabelId, PropertyId>> DeleteIndexStats(const storage::LabelId &label);

  void SetIndexStats(const std::pair<storage::LabelId, storage::PropertyId> &key,
                     const storage::LabelPropertyIndexStats &stats);

  std::optional<storage::LabelPropertyIndexStats> GetIndexStats(
      const std::pair<storage::LabelId, storage::PropertyId> &key) const;

  void RunGC();

  Iterable Vertices(LabelId label, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Transaction *transaction);

 private:
  std::map<std::pair<LabelId, PropertyId>, utils::SkipList<Entry>> index_;
  std::unordered_map<PropertyId, std::unordered_map<LabelId, utils::SkipList<Entry> *>> indices_by_property_;
  std::map<std::pair<LabelId, PropertyId>, storage::LabelPropertyIndexStats> stats_;
};

}  // namespace memgraph::storage
