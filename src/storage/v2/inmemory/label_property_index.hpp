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
  /// Index entry stored in the SkipList. Parameterized on sort direction:
  /// Reverse=false gives ASC ordering, Reverse=true gives DESC (reversed values comparison).
  template <bool Reverse = false>
  struct BasicEntry {
    IndexOrderedPropertyValues values;
    Vertex *vertex;
    uint64_t timestamp;

    friend std::weak_ordering operator<=>(BasicEntry const &lhs, BasicEntry const &rhs) {
      // Reverse ONLY the values comparison; vertex+timestamp stay same for MVCC correctness
      if constexpr (Reverse) {
        if (auto cmp = (rhs.values <=> lhs.values); cmp != 0) return cmp;
      } else {
        if (auto cmp = (lhs.values <=> rhs.values); cmp != 0) return cmp;
      }
      if (auto cmp = std::compare_three_way{}(lhs.vertex, rhs.vertex); cmp != 0) return cmp;
      return lhs.timestamp <=> rhs.timestamp;
    }

    friend bool operator==(BasicEntry const &, BasicEntry const &) = default;

    bool operator<(std::vector<PropertyValue> const &rhs) const;
    bool operator==(std::vector<PropertyValue> const &rhs) const;
    bool operator<=(std::vector<PropertyValue> const &rhs) const;
  };

  using Entry = BasicEntry<false>;
  using DescEntry = BasicEntry<true>;

  template <typename EntryT = Entry>
  struct IndividualIndex {
    using EntryType = EntryT;

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

  struct IndexContainer {
   private:
    template <typename IndicesMap, typename ReverseLookup>
    static void BuildReverseLookup(IndicesMap const &indices, ReverseLookup &reverse_lookup) {
      for (auto const &[label, by_label] : indices) {
        for (auto const &[propertyPaths, entry] : by_label) {
          using EntryDetailT = std::tuple<PropertiesPaths const *, typename std::decay_t<decltype(*entry)> *>;
          auto const ed = EntryDetailT{&propertyPaths, entry.get()};
          for (auto const &prop : propertyPaths) {
            reverse_lookup[prop[0]].emplace(label, ed);
          }
        }
      }
    }

   public:
    IndexContainer(IndexContainer const &other) : asc_indices_(other.asc_indices_), desc_indices_(other.desc_indices_) {
      BuildReverseLookup(asc_indices_, asc_reverse_lookup_);
      BuildReverseLookup(desc_indices_, desc_reverse_lookup_);
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

    template <typename Fn>
    void ForEachIndicesMap(Fn &&fn) const {
      fn(asc_indices_);
      fn(desc_indices_);
    }

    template <typename Fn>
    void ForEachIndicesMap(Fn &&fn) {
      fn(asc_indices_);
      fn(desc_indices_);
    }

    template <typename Fn>
    void ForEachReverseLookup(Fn &&fn) const {
      fn(asc_reverse_lookup_);
      fn(desc_reverse_lookup_);
    }
  };

  template <typename EntryT>
  struct BasicAllIndicesEntry {
    std::shared_ptr<IndividualIndex<EntryT>> index_;
    LabelId label_;
    PropertiesPaths properties_;
  };

  using AscAllIndicesEntry = BasicAllIndicesEntry<Entry>;
  using DescAllIndicesEntry = BasicAllIndicesEntry<DescEntry>;

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

    template <typename EntryT>
    auto Vertices(LabelId label, std::span<PropertyPath const> properties, std::span<PropertyValueRange const> range,
                  View view, Storage *storage, Transaction *transaction) -> Iterable<EntryT>;

    template <typename EntryT>
    auto Vertices(LabelId label, std::span<PropertyPath const> properties, std::span<PropertyValueRange const> range,
                  memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor vertices_acc, View view,
                  Storage *storage, Transaction *transaction) -> Iterable<EntryT>;

    template <typename EntryT = Entry>
    ChunkedIterable<EntryT> ChunkedVertices(
        LabelId label, std::span<PropertyPath const> properties, std::span<PropertyValueRange const> range,
        memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor vertices_acc, View view, Storage *storage,
        Transaction *transaction, size_t num_chunks);

   private:
    // Finds the index for (label, properties) in asc_indices_ or desc_indices_ and applies fn to it.
    // Returns std::nullopt if the index doesn't exist in either map.
    template <typename Fn>
    auto WithFoundIndex(LabelId label, std::span<PropertyPath const> properties, Fn &&fn) const
        -> std::optional<uint64_t> {
      auto const try_find = [&](auto &indices_map) -> std::optional<uint64_t> {
        auto it = indices_map.find(label);
        if (it == indices_map.end()) return std::nullopt;
        auto it2 = it->second.find(properties);
        if (it2 == it->second.end()) return std::nullopt;
        return fn(*it2->second);
      };
      if (auto r = try_find(index_container_->asc_indices_)) return r;
      if (auto r = try_find(index_container_->desc_indices_)) return r;
      return std::nullopt;
    }

    // Returns the indices map (asc or desc) based on EntryT.
    template <typename EntryT>
    auto const &IndicesMap() const {
      if constexpr (std::same_as<EntryT, DescEntry>) {
        return index_container_->desc_indices_;
      } else {
        return index_container_->asc_indices_;
      }
    }

    std::shared_ptr<IndexContainer const> index_container_;
  };

  auto GetActiveIndices() const -> std::shared_ptr<LabelPropertyIndex::ActiveIndices> override;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token);

  DropResult DropIndex(LabelId label, std::vector<PropertyPath> const &properties,
                       ActiveIndicesUpdater const &updater) override;

  std::vector<std::pair<LabelId, std::vector<PropertyPath>>> ClearIndexStats();

  std::vector<std::pair<LabelId, std::vector<PropertyPath>>> DeleteIndexStats(const storage::LabelId &label);

  void SetIndexStats(storage::LabelId label, std::span<storage::PropertyPath const> properties,
                     storage::LabelPropertyIndexStats const &stats);

  std::optional<storage::LabelPropertyIndexStats> GetIndexStats(
      std::pair<storage::LabelId, std::span<storage::PropertyPath const>> const &key) const;

  void RunGC();

  void DropGraphClearIndices() override;

 private:
  void DropSingleOrder(LabelId label, std::vector<PropertyPath> const &properties, ActiveIndicesUpdater const &updater,
                       IndexOrder order);
  void CleanupAllIndices();
  void CleanupStatsForDrop(IndexContainer const &new_index, LabelId label, std::vector<PropertyPath> const &properties);

  template <typename EntryT>
  auto GetIndividualIndex(LabelId const &label, PropertiesPaths const &properties) const
      -> std::shared_ptr<IndividualIndex<EntryT>>;

  template <typename Fn>
  void ForEachAllIndices(Fn &&fn) {
    fn(asc_all_indices_);
    fn(desc_all_indices_);
  }

  utils::Synchronized<std::shared_ptr<IndexContainer const>, utils::WritePrioritizedRWLock> index_{
      std::make_shared<IndexContainer const>()};
  utils::Synchronized<std::shared_ptr<std::vector<AscAllIndicesEntry> const>, utils::WritePrioritizedRWLock>
      asc_all_indices_{std::make_shared<std::vector<AscAllIndicesEntry> const>()};
  utils::Synchronized<std::shared_ptr<std::vector<DescAllIndicesEntry> const>, utils::WritePrioritizedRWLock>
      desc_all_indices_{std::make_shared<std::vector<DescAllIndicesEntry> const>()};
  utils::Synchronized<std::map<LabelId, PropertiesIndicesStats>, utils::ReadPrioritizedRWLock> stats_;
};

}  // namespace memgraph::storage
