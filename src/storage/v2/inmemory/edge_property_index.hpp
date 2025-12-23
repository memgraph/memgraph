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

#include <cstdint>
#include <map>
#include <utility>

#include "storage/v2/common_function_signatures.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/edge_property_index.hpp"
#include "storage/v2/indices/errors.hpp"
#include "storage/v2/inmemory/indices_mvcc.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/rw_lock.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

class InMemoryEdgePropertyIndex : public EdgePropertyIndex {
 private:
  struct Entry {
    PropertyValue value;
    Vertex *from_vertex;
    Vertex *to_vertex;
    Edge *edge;
    EdgeTypeId edge_type;

    uint64_t timestamp;

    friend bool operator<(Entry const &lhs, Entry const &rhs) {
      return std::tie(lhs.value, lhs.edge, lhs.from_vertex, lhs.to_vertex, lhs.edge_type, lhs.timestamp) <
             std::tie(rhs.value, rhs.edge, rhs.from_vertex, rhs.to_vertex, rhs.edge_type, rhs.timestamp);
    };

    friend bool operator==(Entry const &lhs, Entry const &rhs) {
      return std::tie(lhs.value, lhs.edge, lhs.from_vertex, lhs.to_vertex, lhs.edge_type, lhs.timestamp) ==
             std::tie(rhs.value, rhs.edge, rhs.from_vertex, rhs.to_vertex, rhs.edge_type, rhs.timestamp);
    }

    bool operator==(const PropertyValue &rhs) const { return value == rhs; }
    auto operator<=>(const PropertyValue &rhs) const { return value <=> rhs; }
  };

 public:
  struct IndividualIndex {
    ~IndividualIndex();
    void Publish(uint64_t commit_timestamp);

    utils::SkipList<Entry> skip_list_;
    IndexStatus status_{};
  };

  struct IndicesContainer {
    IndicesContainer(IndicesContainer const &other) : indices_(other.indices_) {}
    IndicesContainer(IndicesContainer &&) = default;
    IndicesContainer &operator=(IndicesContainer const &) = default;
    IndicesContainer &operator=(IndicesContainer &&) = default;
    IndicesContainer() = default;
    ~IndicesContainer() = default;

    std::map<PropertyId, std::shared_ptr<IndividualIndex>> indices_;
  };

  class Iterable {
   public:
    Iterable(utils::SkipList<Entry>::Accessor index_accessor, utils::SkipList<Vertex>::ConstAccessor vertex_accessor,
             utils::SkipList<Edge>::ConstAccessor edge_accessor, PropertyId property,
             const std::optional<utils::Bound<PropertyValue>> &lower_bound,
             const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
             Transaction *transaction);

    class Iterator {
     public:
      Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator);

      EdgeAccessor const &operator*() const { return current_accessor_; }

      bool operator==(const Iterator &other) const { return index_iterator_ == other.index_iterator_; }
      bool operator!=(const Iterator &other) const { return index_iterator_ != other.index_iterator_; }

      Iterator &operator++();

     private:
      void AdvanceUntilValid();

      Iterable *self_;
      utils::SkipList<Entry>::Iterator index_iterator_;
      EdgeRef current_edge_{nullptr};
      EdgeAccessor current_accessor_;
    };

    Iterator begin() { return {this, index_accessor_.begin()}; }
    Iterator end() { return {this, index_accessor_.end()}; }

   private:
    utils::SkipList<Edge>::ConstAccessor pin_accessor_edge_;
    utils::SkipList<Vertex>::ConstAccessor pin_accessor_vertex_;
    utils::SkipList<Entry>::Accessor index_accessor_;
    [[maybe_unused]] EdgeTypeId edge_type_;
    [[maybe_unused]] PropertyId property_;
    std::optional<utils::Bound<PropertyValue>> lower_bound_;
    std::optional<utils::Bound<PropertyValue>> upper_bound_;
    bool bounds_valid_{true};
    View view_;
    Storage *storage_;
    Transaction *transaction_;
  };

  class ChunkedIterable {
   public:
    ChunkedIterable(utils::SkipList<Entry>::Accessor index_accessor,
                    utils::SkipList<Vertex>::ConstAccessor vertex_accessor,
                    utils::SkipList<Edge>::ConstAccessor edge_accessor, PropertyId property,
                    const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
                    Transaction *transaction, size_t num_chunks);

    class Iterator {
     public:
      Iterator(ChunkedIterable *self, utils::SkipList<Entry>::ChunkedIterator index_iterator)
          : self_(self),
            index_iterator_(index_iterator),
            current_edge_accessor_(EdgeRef{nullptr}, EdgeTypeId{}, nullptr, nullptr, self_->storage_,
                                   self_->transaction_),
            current_edge_(nullptr) {
        AdvanceUntilValid();
      }

      EdgeAccessor const &operator*() const { return current_edge_accessor_; }
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
      utils::SkipList<Entry>::ChunkedIterator index_iterator_;
      EdgeAccessor current_edge_accessor_;
      EdgeRef current_edge_{nullptr};
    };

    class Chunk {
      Iterator begin_;
      Iterator end_;

     public:
      Chunk(ChunkedIterable *self, utils::SkipList<Entry>::Chunk &chunk)
          : begin_{self, chunk.begin()}, end_{self, chunk.end()} {}

      Iterator begin() { return begin_; }
      Iterator end() { return end_; }
    };

    Chunk get_chunk(size_t id) { return {this, chunks_[id]}; }
    size_t size() const { return chunks_.size(); }

   private:
    utils::SkipList<Edge>::ConstAccessor pin_accessor_edge_;
    utils::SkipList<Vertex>::ConstAccessor pin_accessor_vertex_;
    utils::SkipList<Entry>::Accessor index_accessor_;
    [[maybe_unused]] PropertyId property_;
    std::optional<utils::Bound<PropertyValue>> lower_bound_;
    std::optional<utils::Bound<PropertyValue>> upper_bound_;
    [[maybe_unused]] bool bounds_valid_{true};
    View view_;
    Storage *storage_;
    Transaction *transaction_;
    utils::SkipList<Entry>::ChunkCollection chunks_;
  };

  struct ActiveIndices : EdgePropertyIndex::ActiveIndices {
    explicit ActiveIndices(std::shared_ptr<IndicesContainer const> indices = std::make_shared<IndicesContainer>())
        : index_container_{std::move(indices)} {}

    void UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge, EdgeTypeId edge_type,
                             PropertyId property, PropertyValue value, uint64_t timestamp) override;

    uint64_t ApproximateEdgeCount(PropertyId property) const override;

    uint64_t ApproximateEdgeCount(PropertyId property, const PropertyValue &value) const override;

    uint64_t ApproximateEdgeCount(PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower,
                                  const std::optional<utils::Bound<PropertyValue>> &upper) const override;

    bool IndexExists(PropertyId property) const override;

    bool IndexReady(PropertyId property) const override;

    std::vector<PropertyId> ListIndices(uint64_t start_timestamp) const override;

    Iterable Edges(PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                   const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
                   Transaction *transaction);

    ChunkedIterable ChunkedEdges(PropertyId property, utils::SkipList<Vertex>::ConstAccessor vertex_accessor,
                                 utils::SkipList<Edge>::ConstAccessor edge_accessor,
                                 const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                 const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view,
                                 Storage *storage, Transaction *transaction, size_t num_chunks);

    auto GetAbortProcessor() const -> AbortProcessor override;
    void AbortEntries(AbortableInfo const &info, uint64_t start_timestamp) override;

   private:
    std::shared_ptr<IndicesContainer const> index_container_;
  };

  InMemoryEdgePropertyIndex() = default;

  /// @throw std::bad_alloc
  bool CreateIndexOnePass(PropertyId property, utils::SkipList<Vertex>::Accessor vertices,
                          std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  bool RegisterIndex(PropertyId property);
  auto PopulateIndex(PropertyId property, utils::SkipList<Vertex>::Accessor vertices,
                     std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt,
                     Transaction const *tx = nullptr, CheckCancelFunction cancel_check = neverCancel)
      -> std::expected<void, IndexPopulateError>;
  bool PublishIndex(PropertyId property, uint64_t commit_timestamp);

  /// Returns false if there was no index to drop
  bool DropIndex(PropertyId property) override;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token);

  void DropGraphClearIndices() override;

  void RunGC();

  auto GetActiveIndices() const -> std::unique_ptr<EdgePropertyIndex::ActiveIndices> override;

 private:
  auto GetIndividualIndex(PropertyId property) const -> std::shared_ptr<IndividualIndex>;
  void CleanupAllIndicies();

  utils::Synchronized<std::shared_ptr<IndicesContainer const>, utils::WritePrioritizedRWLock> index_{
      std::make_shared<IndicesContainer const>()};

  // For correct GC we need a copy of all indexes, even if dropped, this is so we can ensure dangling ptr are removed
  // even for dropped indices
  using AllIndicesEntry = std::pair<PropertyId, std::shared_ptr<IndividualIndex>>;
  utils::Synchronized<std::shared_ptr<std::vector<AllIndicesEntry> const>, utils::WritePrioritizedRWLock> all_indices_{
      std::make_shared<std::vector<AllIndicesEntry> const>()};
};

}  // namespace memgraph::storage
