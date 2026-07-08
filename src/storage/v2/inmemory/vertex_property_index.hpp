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

#include <cstdint>
#include <map>
#include <utility>

#include "memory/db_arena_fwd.hpp"
#include "metrics/prometheus_metrics.hpp"
#include "metrics/scoped_gauge.hpp"
#include "storage/v2/common_function_signatures.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/errors.hpp"
#include "storage/v2/indices/vertex_property_index.hpp"
#include "storage/v2/inmemory/indices_mvcc.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/rw_lock.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

class InMemoryVertexPropertyIndex : public VertexPropertyIndex {
 private:
  struct Entry {
    PropertyValue value;
    Vertex *vertex;

    uint64_t timestamp;

    friend bool operator<(Entry const &lhs, Entry const &rhs) {
      return std::tie(lhs.value, lhs.vertex, lhs.timestamp) < std::tie(rhs.value, rhs.vertex, rhs.timestamp);
    };

    friend bool operator==(Entry const &lhs, Entry const &rhs) {
      return std::tie(lhs.value, lhs.vertex, lhs.timestamp) == std::tie(rhs.value, rhs.vertex, rhs.timestamp);
    }

    bool operator==(PropertyValue const &rhs) const { return value == rhs; }

    auto operator<=>(PropertyValue const &rhs) const { return value <=> rhs; }
  };

 public:
  explicit InMemoryVertexPropertyIndex(metrics::GaugeHandle gauge = {}) : gauge_{gauge} {}

  struct IndividualIndex {
    explicit IndividualIndex() : skip_list_{} {}

    ~IndividualIndex();
    void Publish(uint64_t commit_timestamp, metrics::GaugeHandle gauge);

    utils::SkipListDb<Entry> skip_list_;
    IndexStatus status_{};
    metrics::ScopedGauge gauge_{};
  };

  struct IndicesContainer {
    IndicesContainer(IndicesContainer const &other) : indices_(other.indices_) {}

    IndicesContainer(IndicesContainer &&) = default;
    IndicesContainer &operator=(IndicesContainer const &) = default;
    IndicesContainer &operator=(IndicesContainer &&) = default;
    IndicesContainer() = default;
    ~IndicesContainer() = default;

    std::map<PropertyId, std::shared_ptr<IndividualIndex>, std::less<PropertyId>,
             memory::DbAwareAllocator<std::pair<const PropertyId, std::shared_ptr<IndividualIndex>>>>
        indices_;
  };

  class Iterable {
   public:
    Iterable(utils::SkipListDb<Entry>::Accessor index_accessor,
             utils::SkipListDb<Vertex>::ConstAccessor vertex_accessor, PropertyId property,
             std::optional<utils::Bound<PropertyValue>> const &lower_bound,
             std::optional<utils::Bound<PropertyValue>> const &upper_bound, View view, Storage *storage,
             Transaction *transaction, Gid max_gid);

    class Iterator {
     public:
      Iterator(Iterable *self, utils::SkipListDb<Entry>::Iterator index_iterator);

      VertexAccessor const &operator*() const { return current_accessor_; }

      bool operator==(Iterator const &other) const { return index_iterator_ == other.index_iterator_; }

      bool operator!=(Iterator const &other) const { return index_iterator_ != other.index_iterator_; }

      Iterator &operator++();

     private:
      void AdvanceUntilValid();

      Iterable *self_;
      utils::SkipListDb<Entry>::Iterator index_iterator_;
      Vertex *current_vertex_{nullptr};
      VertexAccessor current_accessor_;
    };

    Iterator begin() {
      if (!bounds_valid_) return {this, index_accessor_.end()};
      if (lower_bound_) {
        return {this, index_accessor_.find_equal_or_greater(lower_bound_->value())};
      }
      return {this, index_accessor_.begin()};
    }

    Iterator end() { return {this, index_accessor_.end()}; }

   private:
    utils::SkipListDb<Vertex>::ConstAccessor pin_accessor_;
    utils::SkipListDb<Entry>::Accessor index_accessor_;
    [[maybe_unused]] PropertyId property_;
    std::optional<utils::Bound<PropertyValue>> lower_bound_;
    std::optional<utils::Bound<PropertyValue>> upper_bound_;
    bool bounds_valid_{true};
    View view_;
    Storage *storage_;
    Transaction *transaction_;
    Gid max_gid_;
  };

  class ChunkedIterable {
   public:
    ChunkedIterable(utils::SkipListDb<Entry>::Accessor index_accessor,
                    utils::SkipListDb<Vertex>::ConstAccessor vertex_accessor, PropertyId property,
                    std::optional<utils::Bound<PropertyValue>> const &lower_bound,
                    std::optional<utils::Bound<PropertyValue>> const &upper_bound, View view, Storage *storage,
                    Transaction *transaction, size_t num_chunks, Gid max_gid);

    class Iterator {
     public:
      Iterator(ChunkedIterable *self, utils::SkipListDb<Entry>::ChunkedIterator index_iterator)
          : self_(self),
            index_iterator_(index_iterator),
            current_accessor_(nullptr, self_->storage_, self_->transaction_) {
        AdvanceUntilValid();
      }

      VertexAccessor const &operator*() const { return current_accessor_; }

      bool operator==(Iterator const &other) const { return index_iterator_ == other.index_iterator_; }

      bool operator!=(Iterator const &other) const { return index_iterator_ != other.index_iterator_; }

      Iterator &operator++() {
        ++index_iterator_;
        AdvanceUntilValid();
        return *this;
      }

     private:
      void AdvanceUntilValid();

      ChunkedIterable *self_;
      utils::SkipListDb<Entry>::ChunkedIterator index_iterator_;
      VertexAccessor current_accessor_;
      Vertex *current_vertex_{nullptr};
    };

    class Chunk {
      Iterator begin_;
      Iterator end_;

     public:
      Chunk(ChunkedIterable *self, utils::SkipListDb<Entry>::Chunk &chunk)
          : begin_{self, chunk.begin()}, end_{self, chunk.end()} {}

      Iterator begin() { return begin_; }

      Iterator end() { return end_; }
    };

    Chunk get_chunk(size_t id) { return {this, chunks_[id]}; }

    size_t size() const { return chunks_.size(); }

   private:
    utils::SkipListDb<Vertex>::ConstAccessor pin_accessor_;
    utils::SkipListDb<Entry>::Accessor index_accessor_;
    [[maybe_unused]] PropertyId property_;
    std::optional<utils::Bound<PropertyValue>> lower_bound_;
    std::optional<utils::Bound<PropertyValue>> upper_bound_;
    [[maybe_unused]] bool bounds_valid_{true};
    View view_;
    Storage *storage_;
    Transaction *transaction_;
    utils::SkipListDb<Entry>::ChunkCollection chunks_;
    Gid max_gid_;
  };

  struct ActiveIndices : VertexPropertyIndex::ActiveIndices {
    explicit ActiveIndices(std::shared_ptr<IndicesContainer const> indices = std::make_shared<IndicesContainer>())
        : index_container_{std::move(indices)} {}

    void UpdateOnSetProperty(PropertyId property, PropertyValue value, Vertex *vertex, uint64_t timestamp) override;

    uint64_t ApproximateVertexCount(PropertyId property) const override;

    uint64_t ApproximateVertexCount(PropertyId property, PropertyValue const &value) const override;

    uint64_t ApproximateVertexCount(PropertyId property, std::optional<utils::Bound<PropertyValue>> const &lower,
                                    std::optional<utils::Bound<PropertyValue>> const &upper) const override;

    bool IndexExists(PropertyId property) const override;

    bool IndexReady(PropertyId property) const override;

    std::vector<PropertyId> ListIndices(uint64_t start_timestamp) const override;

    Iterable Vertices(PropertyId property, utils::SkipListDb<Vertex>::ConstAccessor vertex_accessor,
                      std::optional<utils::Bound<PropertyValue>> const &lower_bound,
                      std::optional<utils::Bound<PropertyValue>> const &upper_bound, View view, Storage *storage,
                      Transaction *transaction);

    ChunkedIterable ChunkedVertices(PropertyId property, utils::SkipListDb<Vertex>::ConstAccessor vertex_accessor,
                                    std::optional<utils::Bound<PropertyValue>> const &lower_bound,
                                    std::optional<utils::Bound<PropertyValue>> const &upper_bound, View view,
                                    Storage *storage, Transaction *transaction, size_t num_chunks);

    auto GetAbortProcessor() const -> AbortProcessor override;
    void AbortEntries(AbortableInfo const &info, uint64_t start_timestamp) override;

   private:
    std::shared_ptr<IndicesContainer const> index_container_;
  };

  InMemoryVertexPropertyIndex() = default;

  bool CreateIndexOnePass(PropertyId property, utils::SkipListDb<Vertex>::Accessor vertices,
                          ActiveIndicesUpdater const &updater,
                          std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  bool RegisterIndex(PropertyId property, ActiveIndicesUpdater const &updater);
  auto PopulateIndex(PropertyId property, utils::SkipListDb<Vertex>::Accessor vertices,
                     ActiveIndicesUpdater const &updater,
                     std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt,
                     Transaction const *tx = nullptr, CheckCancelFunction cancel_check = neverCancel)
      -> std::expected<void, IndexPopulateError>;
  bool PublishIndex(PropertyId property, uint64_t commit_timestamp);

  [[nodiscard]] auto DropIndex(PropertyId property, ActiveIndicesUpdater const &updater)
      -> std::shared_ptr<IndividualIndex>;
  void RestoreIndex(PropertyId property, std::shared_ptr<IndividualIndex> evicted, ActiveIndicesUpdater const &updater);

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token);

  void DropGraphClearIndices() override;

  void RunGC();

  auto GetActiveIndices() const -> std::shared_ptr<VertexPropertyIndex::ActiveIndices> override;

 private:
  auto GetIndividualIndex(PropertyId property) const -> std::shared_ptr<IndividualIndex>;

  bool InstallIndividualIndex_(PropertyId property, std::shared_ptr<IndividualIndex> entry,
                               ActiveIndicesUpdater const &updater, bool register_in_all_indices);
  void CleanupAllIndices();

  metrics::GaugeHandle gauge_{};

  utils::Synchronized<std::shared_ptr<IndicesContainer const>, utils::WritePrioritizedRWLock> index_{
      std::make_shared<IndicesContainer const>()};

  using AllIndicesEntry = std::pair<PropertyId, std::shared_ptr<IndividualIndex>>;
  utils::Synchronized<std::shared_ptr<std::vector<AllIndicesEntry> const>, utils::WritePrioritizedRWLock> all_indices_{
      std::make_shared<std::vector<AllIndicesEntry> const>()};
};

}  // namespace memgraph::storage
