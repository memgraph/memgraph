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

#include <span>

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/label_property_index.hpp"
#include "storage/v2/indices/label_property_index_stats.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

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
  InMemoryLabelPropertyIndex() = default;

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, PropertyId property, utils::SkipList<Vertex>::Accessor vertices,
                   const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info);

  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) override;

  void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update, const Transaction &tx) override {}

  /// @throw std::bad_alloc
  void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                           const Transaction &tx) override;

  bool DropIndex(LabelId label, PropertyId property) override;

  bool IndexExists(LabelId label, PropertyId property) const override;

  std::vector<std::pair<LabelId, PropertyId>> ListIndices() const override;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token,
                             const std::set<LabelId> &labels = {});

  void AbortEntries(PropertyId property, std::span<std::pair<PropertyValue, Vertex *> const> vertices,
                    uint64_t exact_start_timestamp);
  void AbortEntries(LabelId label, std::span<std::pair<PropertyValue, Vertex *> const> vertices,
                    uint64_t exact_start_timestamp);

  IndexStats Analysis() const {
    IndexStats res{};
    for (const auto &[lp, _] : index_) {
      const auto &[label, property] = lp;
      res.l2p[label].emplace_back(property);
      res.p2l[property].emplace_back(label);
    }
    return res;
  }

  class Iterable {
   public:
    Iterable(utils::SkipList<Entry>::Accessor index_accessor, utils::SkipList<Vertex>::ConstAccessor vertices_accessor,
             LabelId label, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
             const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
             Transaction *transaction);

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

    Iterator begin();
    Iterator end();

   private:
    utils::SkipList<Vertex>::ConstAccessor pin_accessor_;
    utils::SkipList<Entry>::Accessor index_accessor_;
    LabelId label_;
    PropertyId property_;
    std::optional<utils::Bound<PropertyValue>> lower_bound_;
    std::optional<utils::Bound<PropertyValue>> upper_bound_;
    bool bounds_valid_{true};
    View view_;
    Storage *storage_;
    Transaction *transaction_;
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
                    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
                    Transaction *transaction);

  Iterable Vertices(LabelId label, PropertyId property,
                    memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor vertices_acc,
                    const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
                    Transaction *transaction);

  void DropGraphClearIndices() override;

 private:
  std::map<std::pair<LabelId, PropertyId>, utils::SkipList<Entry>> index_;
  std::unordered_map<PropertyId, std::unordered_map<LabelId, utils::SkipList<Entry> *>> indices_by_property_;
  utils::Synchronized<std::map<std::pair<LabelId, PropertyId>, storage::LabelPropertyIndexStats>,
                      utils::ReadPrioritizedRWLock>
      stats_;
};

}  // namespace memgraph::storage
