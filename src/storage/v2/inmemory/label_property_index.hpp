// Copyright 2026 Memgraph Ltd.
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

#include "storage/v2/common_function_signatures.hpp"
#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/errors.hpp"
#include "storage/v2/indices/indices_utils.hpp"
#include "storage/v2/indices/label_property_index.hpp"
#include "storage/v2/indices/label_property_index_stats.hpp"
#include "storage/v2/inmemory/indices_mvcc.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

class InMemoryLabelPropertyIndex : public storage::LabelPropertyIndex {
 public:
  struct Entry {
    IndexOrderedPropertyValues values;
    Vertex *vertex;
    uint64_t timestamp;

    friend auto operator<=>(Entry const &, Entry const &) = default;
    friend bool operator==(Entry const &, Entry const &) = default;

    bool operator<(std::vector<PropertyValue> const &rhs) const;
    bool operator==(std::vector<PropertyValue> const &rhs) const;
    bool operator<=(std::vector<PropertyValue> const &rhs) const;
  };

  // TODO: A cleaner approach would be to add a comparator template parameter to SkipList
  // (e.g. SkipList<TObj, Compare = std::compare_three_way>) instead of duplicating Entry.
  // This would allow using the same Entry type with a reverse comparator for DESC ordering.
  // We avoid modifying SkipList (a core concurrent data structure) for now.
  struct DescEntry {
    IndexOrderedPropertyValues values;
    Vertex *vertex;
    uint64_t timestamp;

    friend std::weak_ordering operator<=>(DescEntry const &lhs, DescEntry const &rhs) {
      // Reverse ONLY the values comparison; vertex+timestamp stay same for MVCC correctness
      if (auto cmp = (rhs.values <=> lhs.values); cmp != 0) return cmp;
      if (auto cmp = std::compare_three_way{}(lhs.vertex, rhs.vertex); cmp != 0) return cmp;
      return lhs.timestamp <=> rhs.timestamp;
    }

    friend bool operator==(DescEntry const &, DescEntry const &) = default;

    bool operator<(std::vector<PropertyValue> const &rhs) const;
    bool operator==(std::vector<PropertyValue> const &rhs) const;
    bool operator<=(std::vector<PropertyValue> const &rhs) const;
  };

  template <typename EntryT = Entry>
  struct IndividualIndex {
    explicit IndividualIndex(PropertiesPermutationHelper permutations_helper)
        : permutations_helper(std::move(permutations_helper)) {}

    ~IndividualIndex();
    void Publish(uint64_t commit_timestamp);

    PropertiesPermutationHelper const permutations_helper;
    utils::SkipList<EntryT> skiplist{};
    IndexStatus status{};
  };

  struct Compare {
    template <std::ranges::forward_range T, std::ranges::forward_range U>
    bool operator()(T const &lhs, U const &rhs) const {
      return std::ranges::lexicographical_compare(lhs, rhs);
    }

    using is_transparent = void;
  };

  // ASC index types
  using AscPropertiesIndices = std::map<PropertiesPaths, std::shared_ptr<IndividualIndex<Entry>>, Compare>;
  using AscLabelPropertiesIndices = std::map<LabelId, AscPropertiesIndices, std::less<>>;
  using AscEntryDetail = std::tuple<PropertiesPaths const *, IndividualIndex<Entry> *>;
  using AscReverseLookup = std::unordered_map<PropertyId, std::multimap<LabelId, AscEntryDetail>>;

  // DESC index types
  using DescPropertiesIndices = std::map<PropertiesPaths, std::shared_ptr<IndividualIndex<DescEntry>>, Compare>;
  using DescLabelPropertiesIndices = std::map<LabelId, DescPropertiesIndices, std::less<>>;
  using DescEntryDetail = std::tuple<PropertiesPaths const *, IndividualIndex<DescEntry> *>;
  using DescReverseLookup = std::unordered_map<PropertyId, std::multimap<LabelId, DescEntryDetail>>;

  // Backward-compatible aliases
  using PropertiesIndices = AscPropertiesIndices;
  using LabelPropertiesIndices = AscLabelPropertiesIndices;
  using EntryDetail = AscEntryDetail;
  using ReverseLabelPropertiesIndices = AscReverseLookup;

  struct IndexContainer {
    IndexContainer(IndexContainer const &other) : asc_indices_(other.asc_indices_), desc_indices_(other.desc_indices_) {
      for (auto const &[label, by_label] : asc_indices_) {
        for (auto const &[propertyPaths, entry] : by_label) {
          auto const ed = AscEntryDetail{&propertyPaths, entry.get()};
          for (auto const &prop : propertyPaths) {
            asc_reverse_lookup_[prop[0]].emplace(label, ed);
          }
        }
      }
      for (auto const &[label, by_label] : desc_indices_) {
        for (auto const &[propertyPaths, entry] : by_label) {
          auto const ed = DescEntryDetail{&propertyPaths, entry.get()};
          for (auto const &prop : propertyPaths) {
            desc_reverse_lookup_[prop[0]].emplace(label, ed);
          }
        }
      }
    }

    IndexContainer(IndexContainer &&) = default;
    IndexContainer &operator=(IndexContainer const &) = delete;
    IndexContainer &operator=(IndexContainer &&) = default;
    IndexContainer() = default;
    ~IndexContainer() = default;

    AscLabelPropertiesIndices asc_indices_;
    DescLabelPropertiesIndices desc_indices_;
    // Keyed on the top-level property of a nested property. So for
    // nested property `a.b.c`, only a key for the top-most `a` exists.
    // Used to make UpdateOnSetProperty faster
    AscReverseLookup asc_reverse_lookup_;
    DescReverseLookup desc_reverse_lookup_;
  };

  struct AscAllIndicesEntry {
    std::shared_ptr<IndividualIndex<Entry>> index_;
    LabelId label_;
    PropertiesPaths properties_;
  };

  struct DescAllIndicesEntry {
    std::shared_ptr<IndividualIndex<DescEntry>> index_;
    LabelId label_;
    PropertiesPaths properties_;
  };

  // Backward-compatible alias
  using AllIndicesEntry = AscAllIndicesEntry;

  using PropertiesIndicesStats = std::map<PropertiesPaths, storage::LabelPropertyIndexStats, Compare>;

  // Convience function that does Register + Populate + direct Publish
  // TODO: direct Publish...should it be for a particular timestamp?
  bool CreateIndexOnePass(LabelId label, PropertiesPaths const &properties, utils::SkipList<Vertex>::Accessor vertices,
                          const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info,
                          ActiveIndicesUpdater const &updater,
                          std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt,
                          IndexOrder order = IndexOrder::ASC);

  bool RegisterIndex(LabelId label, PropertiesPaths const &properties, ActiveIndicesUpdater const &updater,
                     IndexOrder order = IndexOrder::ASC);

  auto PopulateIndex(LabelId label, PropertiesPaths const &properties, utils::SkipList<Vertex>::Accessor vertices,
                     const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info,
                     ActiveIndicesUpdater const &updater,
                     std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt,
                     IndexOrder order = IndexOrder::ASC, Transaction const *tx = nullptr,
                     CheckCancelFunction cancel_check = neverCancel) -> std::expected<void, IndexPopulateError>;

  bool PublishIndex(LabelId label, PropertiesPaths const &properties, uint64_t commit_timestamp,
                    IndexOrder order = IndexOrder::ASC);

  template <typename EntryT = Entry>
  class Iterable {
   public:
    Iterable(typename utils::SkipList<EntryT>::Accessor index_accessor,
             utils::SkipList<Vertex>::ConstAccessor vertices_accessor, LabelId label, PropertiesPaths const *properties,
             PropertiesPermutationHelper const *permutation_helper, std::span<PropertyValueRange const> ranges,
             View view, Storage *storage, Transaction *transaction);

    class Iterator {
     public:
      Iterator(Iterable *self, typename utils::SkipList<EntryT>::Iterator index_iterator);

      VertexAccessor const &operator*() const { return current_vertex_accessor_; }

      bool operator==(const Iterator &other) const { return index_iterator_ == other.index_iterator_; }

      bool operator!=(const Iterator &other) const { return index_iterator_ != other.index_iterator_; }

      Iterator &operator++();

     private:
      void AdvanceUntilValid();

      Iterable *self_;
      typename utils::SkipList<EntryT>::Iterator index_iterator_;
      VertexAccessor current_vertex_accessor_;
      Vertex *current_vertex_;
      bool skip_lower_bound_check_{false};
    };

    Iterator begin();
    Iterator end();

   private:
    utils::SkipList<Vertex>::ConstAccessor pin_accessor_;
    typename utils::SkipList<EntryT>::Accessor index_accessor_;

    // These describe the composite index
    LabelId label_;
    [[maybe_unused]] PropertiesPaths const *properties_;
    PropertiesPermutationHelper const *permutation_helper_;

    std::vector<std::optional<utils::Bound<PropertyValue>>> lower_bound_;
    std::vector<std::optional<utils::Bound<PropertyValue>>> upper_bound_;
    bool bounds_valid_{true};
    View view_;
    Storage *storage_;
    Transaction *transaction_;
  };

  template <typename EntryT = Entry>
  class ChunkedIterable {
   public:
    ChunkedIterable(typename utils::SkipList<EntryT>::Accessor index_accessor,
                    utils::SkipList<Vertex>::ConstAccessor vertices_accessor, LabelId label,
                    PropertiesPaths const *properties, PropertiesPermutationHelper const *permutation_helper,
                    std::span<PropertyValueRange const> ranges, View view, Storage *storage, Transaction *transaction,
                    size_t num_chunks);

    class Iterator {
     public:
      Iterator(ChunkedIterable *self, typename utils::SkipList<EntryT>::ChunkedIterator index_iterator)
          : self_(self), index_iterator_(index_iterator), current_vertex_accessor_(nullptr, self_->storage_, nullptr) {
        AdvanceUntilValid();
      }

      VertexAccessor const &operator*() const { return current_vertex_accessor_; }

      bool operator==(const Iterator &other) const { return index_iterator_ == other.index_iterator_; }

      bool operator!=(const Iterator &other) const { return index_iterator_ != other.index_iterator_; }

      Iterator &operator++() {
        ++index_iterator_;
        AdvanceUntilValid();
        return *this;
      }

     private:
      void AdvanceUntilValid();

      ChunkedIterable *self_;
      typename utils::SkipList<EntryT>::ChunkedIterator index_iterator_;
      VertexAccessor current_vertex_accessor_;
      Vertex *current_vertex_{nullptr};
      bool skip_lower_bound_check_{false};
    };

    class Chunk {
      Iterator begin_;
      Iterator end_;

     public:
      Chunk(ChunkedIterable *self, typename utils::SkipList<EntryT>::Chunk &chunk)
          : begin_{self, chunk.begin()}, end_{self, chunk.end()} {}

      Iterator begin() { return begin_; }

      Iterator end() { return end_; }
    };

    Chunk get_chunk(size_t id) { return {this, chunks_[id]}; }

    size_t size() const { return chunks_.size(); }

   private:
    utils::SkipList<Vertex>::ConstAccessor pin_accessor_;
    typename utils::SkipList<EntryT>::Accessor index_accessor_;
    LabelId label_;
    [[maybe_unused]] PropertiesPaths const *properties_;
    PropertiesPermutationHelper const *permutation_helper_;
    std::vector<std::optional<utils::Bound<PropertyValue>>> lower_bound_;
    std::vector<std::optional<utils::Bound<PropertyValue>>> upper_bound_;
    bool bounds_valid_{true};
    View view_;
    Storage *storage_;
    Transaction *transaction_;
    typename utils::SkipList<EntryT>::ChunkCollection chunks_;
  };

  struct ActiveIndices : LabelPropertyIndex::ActiveIndices {
    explicit ActiveIndices(std::shared_ptr<const IndexContainer> index_container = std::make_shared<IndexContainer>())
        : index_container_{std::move(index_container)} {}

    void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) override;

    void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update, const Transaction &tx) override {}

    void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                             const Transaction &tx) override;

    bool IndexExists(LabelId label, std::span<PropertyPath const> properties) const override;

    bool IndexReady(LabelId label, std::span<PropertyPath const> properties) const override;

    auto RelevantLabelPropertiesIndicesInfo(std::span<LabelId const> labels,
                                            std::span<PropertyPath const> properties) const
        -> std::vector<LabelPropertiesIndicesInfo> override;

    auto ListIndices(uint64_t start_timestamp) const
        -> std::vector<std::pair<LabelId, std::vector<PropertyPath>>> override;

    auto ListIndices(uint64_t start_timestamp, IndexOrder order) const
        -> std::vector<std::pair<LabelId, std::vector<PropertyPath>>>;

    void AbortEntries(AbortableInfo const &info, uint64_t start_timestamp) override;

    auto GetAbortProcessor() const -> AbortProcessor override;

    auto ApproximateVertexCount(LabelId label, std::span<PropertyPath const> properties) const -> uint64_t override;

    /// Supplying a specific value into the count estimation function will return
    /// an estimated count of nodes which have their property's value set to
    /// `value`. If the `values` specified are all `Null`, then an average number
    /// equal elements is returned.
    auto ApproximateVertexCount(LabelId label, std::span<PropertyPath const> properties,
                                std::span<PropertyValue const> values) const -> uint64_t override;

    auto ApproximateVertexCount(LabelId label, std::span<PropertyPath const> properties,
                                std::span<PropertyValueRange const> bounds) const -> uint64_t override;

    auto Vertices(LabelId label, std::span<PropertyPath const> properties, std::span<PropertyValueRange const> range,
                  View view, Storage *storage, Transaction *transaction, IndexOrder order = IndexOrder::ASC)
        -> Iterable<Entry>;

    auto Vertices(LabelId label, std::span<PropertyPath const> properties, std::span<PropertyValueRange const> range,
                  memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor vertices_acc, View view,
                  Storage *storage, Transaction *transaction, IndexOrder order = IndexOrder::ASC) -> Iterable<Entry>;

    auto DescVertices(LabelId label, std::span<PropertyPath const> properties,
                      std::span<PropertyValueRange const> range, View view, Storage *storage, Transaction *transaction)
        -> Iterable<DescEntry>;

    auto DescVertices(LabelId label, std::span<PropertyPath const> properties,
                      std::span<PropertyValueRange const> range,
                      memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor vertices_acc, View view,
                      Storage *storage, Transaction *transaction) -> Iterable<DescEntry>;

    ChunkedIterable<Entry> ChunkedVertices(
        LabelId label, std::span<PropertyPath const> properties, std::span<PropertyValueRange const> range,
        memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor vertices_acc, View view, Storage *storage,
        Transaction *transaction, size_t num_chunks);

   private:
    std::shared_ptr<IndexContainer const> index_container_;
  };

  auto GetActiveIndices() const -> std::shared_ptr<LabelPropertyIndex::ActiveIndices> override;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token);

  bool DropIndex(LabelId label, std::vector<PropertyPath> const &properties,
                 ActiveIndicesUpdater const &updater) override;

  bool DropIndex(LabelId label, std::vector<PropertyPath> const &properties, ActiveIndicesUpdater const &updater,
                 IndexOrder order);

  std::vector<std::pair<LabelId, std::vector<PropertyPath>>> ClearIndexStats();

  std::vector<std::pair<LabelId, std::vector<PropertyPath>>> DeleteIndexStats(const storage::LabelId &label);

  void SetIndexStats(storage::LabelId label, std::span<storage::PropertyPath const> properties,
                     storage::LabelPropertyIndexStats const &stats);

  std::optional<storage::LabelPropertyIndexStats> GetIndexStats(
      std::pair<storage::LabelId, std::span<storage::PropertyPath const>> const &key) const;

  void RunGC();

  void DropGraphClearIndices() override;

 private:
  void CleanupAllIndices();
  auto GetAscIndividualIndex(LabelId const &label, PropertiesPaths const &properties) const
      -> std::shared_ptr<IndividualIndex<Entry>>;
  auto GetDescIndividualIndex(LabelId const &label, PropertiesPaths const &properties) const
      -> std::shared_ptr<IndividualIndex<DescEntry>>;

  utils::Synchronized<std::shared_ptr<IndexContainer const>, utils::WritePrioritizedRWLock> index_{
      std::make_shared<IndexContainer const>()};
  utils::Synchronized<std::shared_ptr<std::vector<AscAllIndicesEntry> const>, utils::WritePrioritizedRWLock>
      asc_all_indices_{std::make_shared<std::vector<AscAllIndicesEntry> const>()};
  utils::Synchronized<std::shared_ptr<std::vector<DescAllIndicesEntry> const>, utils::WritePrioritizedRWLock>
      desc_all_indices_{std::make_shared<std::vector<DescAllIndicesEntry> const>()};
  utils::Synchronized<std::map<LabelId, PropertiesIndicesStats>, utils::ReadPrioritizedRWLock> stats_;
};

}  // namespace memgraph::storage
