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

#include <array>
#include <cstdint>
#include <memory>
#include <span>
#include <tuple>
#include <variant>

#include "memory/db_arena_fwd.hpp"
#include "metrics/prometheus_metrics.hpp"
#include "metrics/scoped_gauge.hpp"
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

// Type-erased iterable wrappers the query executor consumes. Forward-declared
// to keep the seam: vertices_iterable.hpp includes this header, so the erased
// query methods take these by value in their declarations (incomplete type is
// fine for a by-value return declaration) and are defined in the .cpp.
class VerticesIterable;
class VerticesChunkedIterable;

class InMemoryLabelPropertyIndex : public storage::LabelPropertyIndex {
 public:
  /// Index entry stored in the SkipList. Parameterized on sort direction and
  /// composite arity: Order=IndexOrder::ASC sorts ascending, IndexOrder::DESC
  /// descending (the values comparison flips); N is the fixed composite arity
  /// (array-backed) or dynamic_extent for the vector-backed dynamic storage.
  template <IndexOrder Order = IndexOrder::ASC, std::size_t N = std::dynamic_extent>
  struct BasicEntry {
    static constexpr IndexOrder kOrder = Order;
    static constexpr std::size_t kArity = N;
    using ValuesType = IndexOrderedValues<N>;

    IndexOrderedValues<N> values;
    Vertex *vertex;
    uint64_t timestamp;

    friend std::weak_ordering operator<=>(BasicEntry const &lhs, BasicEntry const &rhs) {
      // Flip ONLY the values comparison for DESC; vertex+timestamp stay same for MVCC correctness
      if constexpr (Order == IndexOrder::DESC) {
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

  template <std::size_t N = std::dynamic_extent>
  using Entry = BasicEntry<IndexOrder::ASC, N>;
  template <std::size_t N = std::dynamic_extent>
  using DescEntry = BasicEntry<IndexOrder::DESC, N>;

  explicit InMemoryLabelPropertyIndex(metrics::GaugeHandle gauge = {}) : gauge_{gauge} {}

  template <typename EntryT = Entry<>>
  struct IndividualIndex {
    using EntryType = EntryT;

    explicit IndividualIndex(PropertiesPermutationHelper permutations_helper)
        : permutations_helper(std::move(permutations_helper)), skiplist{} {}

    ~IndividualIndex() = default;
    void Publish(uint64_t commit_timestamp, metrics::GaugeHandle gauge);

    PropertiesPermutationHelper const permutations_helper;
    utils::SkipListDb<EntryT> skiplist{};
    IndexStatus status{};
    metrics::ScopedGauge gauge_{};
  };

  // An index of any supported composite arity, for a fixed sort direction. The
  // array-backed fixed arities (1, 2) inline their values into the skiplist
  // node; the dynamic_extent alternative is the vector-backed fallback for any
  // other arity. The arity is fixed at index creation, so a given map entry
  // holds exactly one alternative.
  template <IndexOrder Order>
  using IndexPtrVariant = std::variant<std::shared_ptr<IndividualIndex<BasicEntry<Order, 1>>>,
                                       std::shared_ptr<IndividualIndex<BasicEntry<Order, 2>>>,
                                       std::shared_ptr<IndividualIndex<BasicEntry<Order, std::dynamic_extent>>>>;

  using AscIndexPtrVariant = IndexPtrVariant<IndexOrder::ASC>;
  using DescIndexPtrVariant = IndexPtrVariant<IndexOrder::DESC>;

  struct Compare {
    template <std::ranges::forward_range T, std::ranges::forward_range U>
    bool operator()(T const &lhs, U const &rhs) const {
      return std::ranges::lexicographical_compare(lhs, rhs);
    }

    using is_transparent = void;
  };

  // ASC index types
  using AscPropertiesIndices = std::map<PropertiesPaths, AscIndexPtrVariant, Compare,
                                        memory::DbAwareAllocator<std::pair<const PropertiesPaths, AscIndexPtrVariant>>>;
  using AscLabelPropertiesIndices = std::map<LabelId, AscPropertiesIndices, std::less<>,
                                             memory::DbAwareAllocator<std::pair<const LabelId, AscPropertiesIndices>>>;
  // Non-owning back-reference into the index map: the paths key and the owning
  // index variant, both pointing into the same map node.
  using AscEntryDetail = std::tuple<PropertiesPaths const *, AscIndexPtrVariant const *>;
  using AscReverseLookup = std::unordered_map<PropertyId, std::multimap<LabelId, AscEntryDetail>>;

  // DESC index types
  using DescPropertiesIndices =
      std::map<PropertiesPaths, DescIndexPtrVariant, Compare,
               memory::DbAwareAllocator<std::pair<const PropertiesPaths, DescIndexPtrVariant>>>;
  using DescLabelPropertiesIndices =
      std::map<LabelId, DescPropertiesIndices, std::less<>,
               memory::DbAwareAllocator<std::pair<const LabelId, DescPropertiesIndices>>>;
  using DescEntryDetail = std::tuple<PropertiesPaths const *, DescIndexPtrVariant const *>;
  using DescReverseLookup = std::unordered_map<PropertyId, std::multimap<LabelId, DescEntryDetail>>;

  struct IndexContainer {
   private:
    template <typename IndicesMap, typename ReverseLookup>
    static void BuildReverseLookup(IndicesMap const &indices, ReverseLookup &reverse_lookup) {
      using EntryDetailT = typename ReverseLookup::mapped_type::mapped_type;
      for (auto const &[label, by_label] : indices) {
        for (auto const &[propertyPaths, entry] : by_label) {
          auto const ed = EntryDetailT{&propertyPaths, &entry};
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

    // Select the asc or desc member by sort direction, so direction-generic
    // code names it once.
    template <IndexOrder Order>
    auto &Indices(this auto &self) {
      if constexpr (Order == IndexOrder::DESC) {
        return self.desc_indices_;
      } else {
        return self.asc_indices_;
      }
    }

    template <IndexOrder Order>
    auto &ReverseLookup() {
      if constexpr (Order == IndexOrder::DESC) {
        return desc_reverse_lookup_;
      } else {
        return asc_reverse_lookup_;
      }
    }
  };

  template <IndexOrder Order>
  struct BasicAllIndicesEntry {
    IndexPtrVariant<Order> index_;
    LabelId label_;
    PropertiesPaths properties_;
  };

  using AscAllIndicesEntry = BasicAllIndicesEntry<IndexOrder::ASC>;
  using DescAllIndicesEntry = BasicAllIndicesEntry<IndexOrder::DESC>;

  struct AllIndicesData {
    std::shared_ptr<std::vector<AscAllIndicesEntry> const> asc{
        std::make_shared<std::vector<AscAllIndicesEntry> const>()};
    std::shared_ptr<std::vector<DescAllIndicesEntry> const> desc{
        std::make_shared<std::vector<DescAllIndicesEntry> const>()};

    template <typename Fn>
    void ForEach(Fn &&fn) {
      fn(asc);
      fn(desc);
    }

    template <typename Fn>
    void ForEach(Fn &&fn) const {
      fn(asc);
      fn(desc);
    }

    // Select the asc or desc tracking list by sort direction.
    template <IndexOrder Order>
    auto &Entries() {
      if constexpr (Order == IndexOrder::DESC) {
        return desc;
      } else {
        return asc;
      }
    }
  };

  using PropertiesIndicesStats = std::map<PropertiesPaths, storage::LabelPropertyIndexStats, Compare>;

  // Convience function that does Register + Populate + direct Publish
  // TODO: direct Publish...should it be for a particular timestamp?
  bool CreateIndexOnePass(LabelId label, PropertiesPaths const &properties,
                          utils::SkipListDb<Vertex>::Accessor vertices,
                          const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info,
                          ActiveIndicesUpdater const &updater,
                          std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt,
                          IndexOrder order = IndexOrder::ASC);

  bool RegisterIndex(LabelId label, PropertiesPaths const &properties, ActiveIndicesUpdater const &updater,
                     IndexOrder order = IndexOrder::ASC);

  auto PopulateIndex(LabelId label, PropertiesPaths const &properties, utils::SkipListDb<Vertex>::Accessor vertices,
                     const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info,
                     ActiveIndicesUpdater const &updater,
                     std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt,
                     IndexOrder order = IndexOrder::ASC, Transaction const *tx = nullptr,
                     CheckCancelFunction cancel_check = neverCancel) -> std::expected<void, IndexPopulateError>;

  bool PublishIndex(LabelId label, PropertiesPaths const &properties, uint64_t commit_timestamp,
                    IndexOrder order = IndexOrder::ASC);

  template <typename EntryT = Entry<>>
  class Iterable {
   public:
    Iterable(typename utils::SkipListDb<EntryT>::Accessor index_accessor,
             utils::SkipListDb<Vertex>::ConstAccessor vertices_accessor, LabelId label,
             PropertiesPaths const *properties, PropertiesPermutationHelper const *permutation_helper,
             std::span<PropertyValueRange const> ranges, View view, Storage *storage, Transaction *transaction,
             Gid max_gid);

    class Iterator {
     public:
      Iterator(Iterable *self, typename utils::SkipListDb<EntryT>::Iterator index_iterator);

      VertexAccessor const &operator*() const { return current_vertex_accessor_; }

      bool operator==(const Iterator &other) const { return index_iterator_ == other.index_iterator_; }

      bool operator!=(const Iterator &other) const { return index_iterator_ != other.index_iterator_; }

      Iterator &operator++();

     private:
      void AdvanceUntilValid();

      Iterable *self_;
      typename utils::SkipListDb<EntryT>::Iterator index_iterator_;
      VertexAccessor current_vertex_accessor_;
      Vertex *current_vertex_;
    };

    Iterator begin();
    Iterator end();

   private:
    utils::SkipListDb<Vertex>::ConstAccessor pin_accessor_;
    typename utils::SkipListDb<EntryT>::Accessor index_accessor_;

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
    Gid max_gid_;
  };

  template <typename EntryT = Entry<>>
  class ChunkedIterable {
   public:
    ChunkedIterable(typename utils::SkipListDb<EntryT>::Accessor index_accessor,
                    utils::SkipListDb<Vertex>::ConstAccessor vertices_accessor, LabelId label,
                    PropertiesPaths const *properties, PropertiesPermutationHelper const *permutation_helper,
                    std::span<PropertyValueRange const> ranges, View view, Storage *storage, Transaction *transaction,
                    size_t num_chunks, Gid max_gid);

    class Iterator {
     public:
      Iterator(ChunkedIterable *self, typename utils::SkipListDb<EntryT>::ChunkedIterator index_iterator)
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
      typename utils::SkipListDb<EntryT>::ChunkedIterator index_iterator_;
      VertexAccessor current_vertex_accessor_;
      Vertex *current_vertex_{nullptr};
    };

    class Chunk {
      Iterator begin_;
      Iterator end_;

     public:
      Chunk(ChunkedIterable *self, typename utils::SkipListDb<EntryT>::Chunk &chunk)
          : begin_{self, chunk.begin()}, end_{self, chunk.end()} {}

      Iterator begin() { return begin_; }

      Iterator end() { return end_; }
    };

    Chunk get_chunk(size_t id) { return {this, chunks_[id]}; }

    size_t size() const { return chunks_.size(); }

   private:
    utils::SkipListDb<Vertex>::ConstAccessor pin_accessor_;
    typename utils::SkipListDb<EntryT>::Accessor index_accessor_;
    LabelId label_;
    [[maybe_unused]] PropertiesPaths const *properties_;
    PropertiesPermutationHelper const *permutation_helper_;
    std::vector<std::optional<utils::Bound<PropertyValue>>> lower_bound_;
    std::vector<std::optional<utils::Bound<PropertyValue>>> upper_bound_;
    bool bounds_valid_{true};
    View view_;
    Storage *storage_;
    Transaction *transaction_;
    typename utils::SkipListDb<EntryT>::ChunkCollection chunks_;
    Gid max_gid_;
  };

  struct ActiveIndices : LabelPropertyIndex::ActiveIndices {
    explicit ActiveIndices(std::shared_ptr<const IndexContainer> index_container = std::make_shared<IndexContainer>())
        : index_container_{std::move(index_container)} {}

    void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) override;

    void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update, const Transaction &tx) override;

    void UpdateOnSetProperty(PropertyId property, const PropertyValue &old_value, const PropertyValue &new_value,
                             Vertex *vertex, const Transaction &tx) override;

    bool IndexExists(LabelId label, std::span<PropertyPath const> properties) const override;

    bool IndexReady(LabelId label, std::span<PropertyPath const> properties) const override;

    auto RelevantLabelPropertiesIndicesInfo(std::span<LabelId const> labels,
                                            std::span<PropertyPath const> properties) const
        -> std::vector<LabelPropertiesIndicesInfo> override;

    auto ListIndices(uint64_t start_timestamp) const -> std::vector<LabelPropertyIndexEntry> override {
      return ListIndicesImpl(start_timestamp, std::nullopt);
    }

    auto ListIndices(uint64_t start_timestamp, IndexOrder order) const -> std::vector<LabelPropertyIndexEntry> {
      return ListIndicesImpl(start_timestamp, order);
    }

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

    // Type-erased query entry points. They pick the entry type from the index's
    // composite arity and sort direction internally and hand back the variant
    // wrapper the executor consumes, so callers never name a per-arity type.
    auto Vertices(LabelId label, std::span<PropertyPath const> properties, std::span<PropertyValueRange const> range,
                  View view, Storage *storage, Transaction *transaction, IndexOrder order) -> VerticesIterable;

    auto ChunkedVertices(LabelId label, std::span<PropertyPath const> properties,
                         std::span<PropertyValueRange const> range,
                         utils::SkipListDb<Vertex>::ConstAccessor vertices_acc, View view, Storage *storage,
                         Transaction *transaction, size_t num_chunks, IndexOrder order) -> VerticesChunkedIterable;

    template <typename EntryT>
    auto Vertices(LabelId label, std::span<PropertyPath const> properties, std::span<PropertyValueRange const> range,
                  View view, Storage *storage, Transaction *transaction) -> Iterable<EntryT>;

    template <typename EntryT>
    auto Vertices(LabelId label, std::span<PropertyPath const> properties, std::span<PropertyValueRange const> range,
                  utils::SkipListDb<Vertex>::ConstAccessor vertices_acc, View view, Storage *storage,
                  Transaction *transaction) -> Iterable<EntryT>;

    template <typename EntryT = Entry<>>
    ChunkedIterable<EntryT> ChunkedVertices(LabelId label, std::span<PropertyPath const> properties,
                                            std::span<PropertyValueRange const> range,
                                            utils::SkipListDb<Vertex>::ConstAccessor vertices_acc, View view,
                                            Storage *storage, Transaction *transaction, size_t num_chunks);

   private:
    auto ListIndicesImpl(uint64_t start_timestamp, std::optional<IndexOrder> order) const
        -> std::vector<LabelPropertyIndexEntry>;

    // Finds the index for (label, properties) and applies fn to it.
    // Tries ASC first then DESC — count is the same regardless of order.
    template <typename Fn>
    auto WithFoundIndex(LabelId label, std::span<PropertyPath const> properties, Fn &&fn) const
        -> std::optional<uint64_t> {
      auto const try_find = [&](auto &indices_map) -> std::optional<uint64_t> {
        auto it = indices_map.find(label);
        if (it == indices_map.end()) return std::nullopt;
        auto it2 = it->second.find(properties);
        if (it2 == it->second.end()) return std::nullopt;
        return std::visit([&](auto const &index_ptr) { return fn(*index_ptr); }, it2->second);
      };
      if (auto r = try_find(index_container_->asc_indices_)) return r;
      if (auto r = try_find(index_container_->desc_indices_)) return r;
      return std::nullopt;
    }

    // Returns the indices map (asc or desc) based on EntryT.
    template <typename EntryT>
    auto const &IndicesMap() const {
      return index_container_->Indices<EntryT::kOrder>();
    }

    std::shared_ptr<IndexContainer const> index_container_;
  };

  auto GetActiveIndices() const -> std::shared_ptr<LabelPropertyIndex::ActiveIndices> override;

  void RemoveObsoleteEntries(Storage *storage, uint64_t oldest_active_start_timestamp, std::stop_token token);

  // Captures the evicted asc/desc IndividualIndex shared_ptrs so the caller can
  // re-insert them on abort. Pair with RestoreIndex. The captured shared_ptrs
  // also keep their entries alive in `all_indices_` past CleanupAllIndices, so
  // RestoreIndex must NOT re-insert there.
  struct DropCapture {
    DropResult result;
    std::optional<AscIndexPtrVariant> asc_evicted;    // nullopt if asc not dropped
    std::optional<DescIndexPtrVariant> desc_evicted;  // nullopt if desc not dropped
    // Per-label stats slice captured before CleanupStatsForDrop erased entries.
    // nullopt if the label had no stats at drop time.
    std::optional<PropertiesIndicesStats> stats_evicted;
  };

  // `order == nullopt` drops both ASC and DESC entries for (label, properties).
  [[nodiscard]] auto DropIndex(LabelId label, std::vector<PropertyPath> const &properties,
                               ActiveIndicesUpdater const &updater, std::optional<IndexOrder> order = std::nullopt)
      -> DropCapture;
  void RestoreIndex(LabelId label, std::vector<PropertyPath> properties, std::optional<AscIndexPtrVariant> asc_evicted,
                    std::optional<DescIndexPtrVariant> desc_evicted,
                    std::optional<PropertiesIndicesStats> stats_evicted, ActiveIndicesUpdater const &updater);

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
  void CleanupStatsForDrop(IndexContainer const &new_index, LabelId label, std::vector<PropertyPath> const &properties);

  template <typename EntryT>
  auto GetIndividualIndex(LabelId const &label, PropertiesPaths const &properties) const
      -> std::shared_ptr<IndividualIndex<EntryT>>;

  metrics::GaugeHandle gauge_{};

  utils::Synchronized<std::shared_ptr<IndexContainer const>, utils::WritePrioritizedRWLock> index_{
      std::make_shared<IndexContainer const>()};
  utils::Synchronized<AllIndicesData, utils::WritePrioritizedRWLock> all_indices_;
  utils::Synchronized<std::map<LabelId, PropertiesIndicesStats>, utils::ReadPrioritizedRWLock> stats_;
};

}  // namespace memgraph::storage
