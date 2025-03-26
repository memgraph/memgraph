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
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/indices_utils.hpp"
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

  struct NewEntry {
    IndexOrderedPropertyValues values;
    Vertex *vertex;
    uint64_t timestamp;

    bool operator<(const NewEntry &rhs) const;
    bool operator==(const NewEntry &rhs) const;

    bool operator<(std::vector<PropertyValue> const &rhs) const;
    bool operator==(std::vector<PropertyValue> const &rhs) const;
  };

 public:
  InMemoryLabelPropertyIndex() = default;

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, std::vector<PropertyId> const &properties, utils::SkipList<Vertex>::Accessor vertices,
                   const std::optional<durability::ParallelizedSchemaCreationInfo> &it,
                   std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) override;

  void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update, const Transaction &tx) override {}

  /// @throw std::bad_alloc
  void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                           const Transaction &tx) override;

  bool DropIndex(LabelId label, std::vector<PropertyId> const &properties) override;

  bool IndexExists(LabelId label, std::span<PropertyId const> properties) const override;

  auto RelevantLabelPropertiesIndicesInfo(std::span<LabelId const> labels, std::span<PropertyId const> properties) const
      -> std::vector<LabelPropertiesIndicesInfo> override;

  std::vector<std::pair<LabelId, std::vector<PropertyId>>> ListIndices() const override;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token);

  IndexStats Analysis() const;

  void AbortEntries(AbortableInfo const &info, uint64_t start_timestamp) override;

  class Iterable {
   public:
    Iterable(utils::SkipList<NewEntry>::Accessor index_accessor,
             utils::SkipList<Vertex>::ConstAccessor vertices_accessor, LabelId label, PropertiesIds const *properties,
             PropertiesPermutationHelper const *permutation_helper, std::span<PropertyValueRange const> ranges,
             View view, Storage *storage, Transaction *transaction);

    class Iterator {
     public:
      Iterator(Iterable *self, utils::SkipList<NewEntry>::Iterator index_iterator);

      VertexAccessor const &operator*() const { return current_vertex_accessor_; }

      bool operator==(const Iterator &other) const { return index_iterator_ == other.index_iterator_; }
      bool operator!=(const Iterator &other) const { return index_iterator_ != other.index_iterator_; }

      Iterator &operator++();

     private:
      void AdvanceUntilValid();

      Iterable *self_;
      utils::SkipList<NewEntry>::Iterator index_iterator_;
      VertexAccessor current_vertex_accessor_;
      Vertex *current_vertex_;
    };

    Iterator begin();
    Iterator end();

   private:
    utils::SkipList<Vertex>::ConstAccessor pin_accessor_;
    utils::SkipList<NewEntry>::Accessor index_accessor_;

    // These describe the composite index
    LabelId label_;
    PropertiesIds const *properties_;
    PropertiesPermutationHelper const *permutation_helper_;
    //    std::vector<PropertyId> properties_; //TODO: PropertiesIds *
    // TODO: PropertiesPermutationHelper *

    std::vector<std::optional<utils::Bound<PropertyValue>>> lower_bound_;
    std::vector<std::optional<utils::Bound<PropertyValue>>> upper_bound_;
    bool bounds_valid_{true};
    View view_;
    Storage *storage_;
    Transaction *transaction_;
  };

  uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyId const> properties) const override;

  /// Supplying a specific value into the count estimation function will return
  /// an estimated count of nodes which have their property's value set to
  /// `value`. If the `values` specified are all `Null`, then an average number
  /// equal elements is returned.
  uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyId const> properties,
                                  std::span<PropertyValue const> values) const override;

  uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyId const> properties,
                                  std::span<PropertyValueRange const> bounds) const override;

  std::vector<std::pair<LabelId, std::vector<PropertyId>>> ClearIndexStats();

  std::vector<std::pair<LabelId, std::vector<PropertyId>>> DeleteIndexStats(const storage::LabelId &label);

  void SetIndexStats(storage::LabelId label, std::span<storage::PropertyId const> properties,
                     storage::LabelPropertyIndexStats const &stats);

  std::optional<storage::LabelPropertyIndexStats> GetIndexStats(
      std::pair<storage::LabelId, std::span<storage::PropertyId const>> const &key) const;

  void RunGC();

  Iterable Vertices(LabelId label, PropertyId property, PropertyValueRange range, View view, Storage *storage,
                    Transaction *transaction);

  Iterable Vertices(LabelId label, std::span<PropertyId const> properties, std::span<PropertyValueRange const> range,
                    View view, Storage *storage, Transaction *transaction);

  Iterable Vertices(LabelId label, PropertyId property,
                    memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor vertices_acc,
                    PropertyValueRange range, View view, Storage *storage, Transaction *transaction);

  void DropGraphClearIndices() override;

  struct IndividualIndex {
    PropertiesPermutationHelper permutations_helper;
    utils::SkipList<NewEntry> skiplist;
  };

 private:
  struct Compare {
    template <std::ranges::forward_range T, std::ranges::forward_range U>
    bool operator()(T const &lhs, U const &rhs) const {
      return std::ranges::lexicographical_compare(lhs, rhs);
    }

    using is_transparent = void;
  };

  using PropertiesIndices = std::map<PropertiesIds, IndividualIndex, Compare>;
  std::map<LabelId, PropertiesIndices, std::less<>> new_index_;

  using EntryDetail = std::tuple<PropertiesIds const *, IndividualIndex *>;
  using PropToIndexLookup = std::multimap<LabelId, EntryDetail>;
  std::unordered_map<PropertyId, PropToIndexLookup> new_indices_by_property_;

  using PropertiesIndicesStats = std::map<PropertiesIds, storage::LabelPropertyIndexStats, Compare>;
  utils::Synchronized<std::map<LabelId, PropertiesIndicesStats>, utils::ReadPrioritizedRWLock> new_stats_;
};

}  // namespace memgraph::storage
