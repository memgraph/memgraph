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

#include <atomic>
#include <cstdint>
#include <span>

#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/label_property_index.hpp"
#include "storage/v2/indices/label_property_index_stats.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

class InMemoryLabelPropertyIndex : public storage::LabelPropertyIndex {
 private:
  struct Entry {
    IndexOrderedPropertyValues values;
    Vertex *vertex;
    uint64_t timestamp;

    friend auto operator<=>(Entry const &, Entry const &) = default;
    friend bool operator==(Entry const &, Entry const &) = default;

    bool operator<(std::vector<PropertyValue> const &rhs) const;
    bool operator==(std::vector<PropertyValue> const &rhs) const;
  };

 public:
  struct IndividualIndex {
    IndividualIndex(PropertiesPermutationHelper permutations_helper)
        : permutations_helper(std::move(permutations_helper)) {}

    PropertiesPermutationHelper const permutations_helper;
    utils::SkipList<Entry> skiplist{};
    std::atomic<Status> status = Status::POPULATING;
  };
  struct Compare {
    template <std::ranges::forward_range T, std::ranges::forward_range U>
    bool operator()(T const &lhs, U const &rhs) const {
      return std::ranges::lexicographical_compare(lhs, rhs);
    }

    using is_transparent = void;
  };

  using PropertiesIndices = std::map<PropertiesIds, std::shared_ptr<IndividualIndex>, Compare>;
  using IndexContainer = std::map<LabelId, PropertiesIndices, std::less<>>;

  using EntryDetail = std::tuple<PropertiesIds const *, std::shared_ptr<IndividualIndex>>;
  using PropToIndexLookup = std::multimap<LabelId, EntryDetail>;
  using ReverseIndexContainer = std::unordered_map<PropertyId, PropToIndexLookup>;

  using PropertiesIndicesStats = std::map<PropertiesIds, storage::LabelPropertyIndexStats, Compare>;

  class Iterable {
   public:
    Iterable(utils::SkipList<Entry>::Accessor index_accessor, utils::SkipList<Vertex>::ConstAccessor vertices_accessor,
             LabelId label, PropertiesIds const *properties, PropertiesPermutationHelper const *permutation_helper,
             std::span<PropertyValueRange const> ranges, View view, Storage *storage, Transaction *transaction);

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
      bool skip_lower_bound_check_{false};
    };

    Iterator begin();
    Iterator end();

   private:
    utils::SkipList<Vertex>::ConstAccessor pin_accessor_;
    utils::SkipList<Entry>::Accessor index_accessor_;

    // These describe the composite index
    LabelId label_;
    PropertiesIds const *properties_;
    PropertiesPermutationHelper const *permutation_helper_;

    std::vector<std::optional<utils::Bound<PropertyValue>>> lower_bound_;
    std::vector<std::optional<utils::Bound<PropertyValue>>> upper_bound_;
    bool bounds_valid_{true};
    View view_;
    Storage *storage_;
    Transaction *transaction_;
  };

  struct ActiveIndices : LabelPropertyIndex::ActiveIndices {
    ActiveIndices(IndexContainer index_container, ReverseIndexContainer reverse_index_container)
        : index_container(std::move(index_container)), reverse_index_container(std::move(reverse_index_container)) {}

    void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) override;
    void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                             const Transaction &tx) override;

    bool IndexExists(LabelId label, std::span<PropertyId const> properties) const override;

    auto RelevantLabelPropertiesIndicesInfo(std::span<LabelId const> labels,
                                            std::span<PropertyId const> properties) const
        -> std::vector<LabelPropertiesIndicesInfo> override;

    // Not used for in-memory
    void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update, const Transaction &tx) override {}

    uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyId const> properties) const override;

    /// Supplying a specific value into the count estimation function will return
    /// an estimated count of nodes which have their property's value set to
    /// `value`. If the `values` specified are all `Null`, then an average number
    /// equal elements is returned.
    uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyId const> properties,
                                    std::span<PropertyValue const> values) const override;

    uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyId const> properties,
                                    std::span<PropertyValueRange const> bounds) const override;

    std::vector<std::pair<LabelId, std::vector<PropertyId>>> ListIndices() const override;

    void AbortEntries(AbortableInfo const &info, uint64_t start_timestamp) override;

    Iterable Vertices(LabelId label, std::span<PropertyId const> properties, std::span<PropertyValueRange const> range,
                      View view, Storage *storage, Transaction *transaction);

    Iterable Vertices(LabelId label, std::span<PropertyId const> properties, std::span<PropertyValueRange const> range,
                      memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor vertices_acc, View view,
                      Storage *storage, Transaction *transaction);

    auto GetAbortProcessor() const -> AbortProcessor;

    IndexContainer index_container;
    ReverseIndexContainer reverse_index_container;
  };

  InMemoryLabelPropertyIndex() = default;

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, std::vector<PropertyId> const &properties);

  bool DropIndex(LabelId label, std::vector<PropertyId> const &properties) override;

  void UpdateIndexStatus(UpdateStatus status);

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token);

  std::vector<std::pair<LabelId, std::vector<PropertyId>>> ClearIndexStats();

  std::vector<std::pair<LabelId, std::vector<PropertyId>>> DeleteIndexStats(const storage::LabelId &label);

  void SetIndexStats(storage::LabelId label, std::span<storage::PropertyId const> properties,
                     storage::LabelPropertyIndexStats const &stats);

  std::optional<storage::LabelPropertyIndexStats> GetIndexStats(
      std::pair<storage::LabelId, std::span<storage::PropertyId const>> const &key) const;

  void RunGC();

  void DropGraphClearIndices() override;

  auto GetActiveIndices() const -> std::unique_ptr<LabelPropertyIndex::ActiveIndices> override;

  auto PopulateIndex(LabelId label, std::vector<PropertyId> const &properties,
                     utils::SkipList<Vertex>::Accessor vertices,
                     const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info,
                     std::function<bool()> cancel_check,
                     std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt,
                     const Transaction *tx = nullptr) -> bool;

 private:
  utils::Synchronized<IndexContainer, utils::WritePrioritizedRWLock> index_;
  utils::Synchronized<ReverseIndexContainer, utils::WritePrioritizedRWLock> indices_by_property_;
  utils::Synchronized<std::map<LabelId, PropertiesIndicesStats>, utils::ReadPrioritizedRWLock> stats_;
};

}  // namespace memgraph::storage
