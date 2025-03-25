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

struct IndexOrderedPropertyValues {
  IndexOrderedPropertyValues(std::vector<PropertyValue> value) : values_{std::move(value)} {}

  friend auto operator<=>(IndexOrderedPropertyValues const &, IndexOrderedPropertyValues const &) = default;

  std::vector<PropertyValue> values_;
};

struct PropertiesPermutationHelper {
  using permutation_cycles = std::vector<std::vector<std::size_t>>;
  explicit PropertiesPermutationHelper(std::span<PropertyId const> properties);

  void apply_permutation(std::span<PropertyValue> values) const;

  auto extract(PropertyStore const &properties) const -> std::vector<PropertyValue>;

  auto matches_value(PropertyId property_id, PropertyValue const &value, IndexOrderedPropertyValues const &values) const
      -> std::optional<std::pair<std::ptrdiff_t, bool>>;

  /// returned match results are in sorted properties ordering
  auto matches_values(PropertyStore const &properties, IndexOrderedPropertyValues const &values) const
      -> std::vector<bool>;

  auto with_property_id(IndexOrderedPropertyValues const &values) const {
    return ranges::views::enumerate(sorted_properties_) | std::views::transform([&](auto &&p) {
             return std::tuple{p.first, p.second, std::cref(values.values_[position_lookup_[p.first]])};
           });
  }

 private:
  std::vector<PropertyId> sorted_properties_;
  std::vector<std::size_t> position_lookup_;
  permutation_cycles cycles_;
};

/** Representation for a range of property values, which may be:
 * - BOUNDED: including only values between a lower and upper bounds
 * - IS_NOT_NULL: including every non-null value
 */
enum class PropertyRangeType { BOUNDED, IS_NOT_NULL, INVALID };
struct PropertyValueRange {
  using Type = PropertyRangeType;

  static auto InValid() -> PropertyValueRange { return {Type::INVALID, std::nullopt, std::nullopt}; };

  static auto Bounded(std::optional<utils::Bound<PropertyValue>> lower,
                      std::optional<utils::Bound<PropertyValue>> upper) -> PropertyValueRange {
    return {Type::BOUNDED, std::move(lower), std::move(upper)};
  }
  static auto IsNotNull() -> PropertyValueRange { return {Type::IS_NOT_NULL, std::nullopt, std::nullopt}; }

  Type type_;
  std::optional<utils::Bound<PropertyValue>> lower_;
  std::optional<utils::Bound<PropertyValue>> upper_;

 private:
  PropertyValueRange(Type type, std::optional<utils::Bound<PropertyValue>> lower,
                     std::optional<utils::Bound<PropertyValue>> upper)
      : type_{type}, lower_{std::move(lower)}, upper_{std::move(upper)} {}
};

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

  void AbortEntries(PropertyId property, std::span<std::pair<PropertyValue, Vertex *> const> vertices,
                    uint64_t exact_start_timestamp);
  void AbortEntries(LabelId label, std::span<std::pair<PropertyValue, Vertex *> const> vertices,
                    uint64_t exact_start_timestamp);

  IndexStats Analysis() const;

  using PropertiesIds = std::vector<PropertyId>;

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
                                  std::span<std::optional<utils::Bound<PropertyValue>> const> lowers,
                                  std::span<std::optional<utils::Bound<PropertyValue>> const> upper) const override;

  std::vector<std::pair<LabelId, PropertyId>> ClearIndexStats();
  std::vector<std::pair<LabelId, std::vector<PropertyId>>> ClearIndexStatsNew();

  std::vector<std::pair<LabelId, PropertyId>> DeleteIndexStats(const storage::LabelId &label);
  std::vector<std::pair<LabelId, std::vector<PropertyId>>> DeleteIndexStatsNew(const storage::LabelId &label);

  void SetIndexStats(const std::pair<storage::LabelId, storage::PropertyId> &key,
                     const storage::LabelPropertyIndexStats &stats);

  void SetIndexStats(storage::LabelId label, std::span<storage::PropertyId const> properties,
                     storage::LabelPropertyIndexStats const &stats);

  std::optional<storage::LabelPropertyIndexStats> GetIndexStats(
      const std::pair<storage::LabelId, storage::PropertyId> &key) const;

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

  //*** OLD to remove
  std::map<std::tuple<LabelId, PropertyId>, utils::SkipList<Entry>> index_;  // TODO: remove
  std::unordered_map<PropertyId, std::unordered_map<LabelId, utils::SkipList<Entry> *>>
      indices_by_property_;  // TODO: remove
  utils::Synchronized<std::map<std::pair<LabelId, PropertyId>, storage::LabelPropertyIndexStats>,
                      utils::ReadPrioritizedRWLock>
      stats_;  // TODO: remove
  //***
};

}  // namespace memgraph::storage
