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

#include <map>
#include <utility>

#include "storage/v2/common_function_signatures.hpp"
#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/edge_type_index.hpp"
#include "storage/v2/indices/errors.hpp"
#include "storage/v2/inmemory/indices_mvcc.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/rw_lock.hpp"

namespace memgraph::storage {

class InMemoryEdgeTypeIndex : public storage::EdgeTypeIndex {
 private:
  struct Entry {
    Vertex *from_vertex;
    Vertex *to_vertex;

    Edge *edge;  // TODO: Generalise to EdgeRef?

    uint64_t timestamp;

    friend bool operator<(Entry const &lhs, Entry const &rhs) {
      return std::tie(lhs.edge, lhs.from_vertex, lhs.to_vertex, lhs.timestamp) <
             std::tie(rhs.edge, rhs.from_vertex, rhs.to_vertex, rhs.timestamp);
    }

    friend bool operator==(Entry const &lhs, Entry const &rhs) {
      return std::tie(lhs.edge, lhs.from_vertex, lhs.to_vertex, lhs.timestamp) ==
             std::tie(rhs.edge, rhs.from_vertex, rhs.to_vertex, rhs.timestamp);
    }
  };

 public:
  class Iterable {
   public:
    Iterable(utils::SkipList<Entry>::Accessor index_accessor, utils::SkipList<Vertex>::ConstAccessor vertex_accessor,
             utils::SkipList<Edge>::ConstAccessor edge_accessor, EdgeTypeId edge_type, View view, Storage *storage,
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
    View view_;
    Storage *storage_;
    Transaction *transaction_;
  };

 private:
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

    std::map<EdgeTypeId, std::shared_ptr<IndividualIndex>> indices_;  // This should be a std::map because we use it
                                                                      // with assumption that it's sorted
  };

 public:
  struct ActiveIndices : storage::EdgeTypeIndex::ActiveIndices {
    explicit ActiveIndices(std::shared_ptr<IndicesContainer const> indices = std::make_shared<IndicesContainer>())
        : ptr_{std::move(indices)} {}

    bool IndexReady(EdgeTypeId edge_type) const override;

    bool IndexRegistered(EdgeTypeId edge_type) const override;

    auto ListIndices(uint64_t start_timestamp) const -> std::vector<EdgeTypeId> override;

    auto ApproximateEdgeCount(EdgeTypeId edge_type) const -> uint64_t override;

    void UpdateOnEdgeCreation(Vertex *from, Vertex *to, EdgeRef edge_ref, EdgeTypeId edge_type,
                              const Transaction &tx) override;

    void UpdateOnEdgeModification(Vertex *old_from, Vertex *old_to, Vertex *new_from, Vertex *new_to, EdgeRef edge_ref,
                                  EdgeTypeId edge_type, const Transaction &tx) override;
    void AbortEntries(AbortableInfo const &info, uint64_t exact_start_timestamp) override;
    auto GetAbortProcessor() const -> AbortProcessor override;

    Iterable Edges(EdgeTypeId edge_type, View view, Storage *storage, Transaction *transaction);

   private:
    std::shared_ptr<IndicesContainer const> ptr_;
  };

  InMemoryEdgeTypeIndex() = default;

  /// @throw std::bad_alloc
  bool CreateIndexOnePass(EdgeTypeId edge_type, utils::SkipList<Vertex>::Accessor vertices,
                          std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  bool RegisterIndex(EdgeTypeId edge_type);
  auto PopulateIndex(EdgeTypeId insert_function, utils::SkipList<Vertex>::Accessor vertices,
                     std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt,
                     Transaction const *tx = nullptr,
                     CheckCancelFunction cancel_check = neverCancel) -> utils::BasicResult<IndexPopulateError>;

  bool PublishIndex(EdgeTypeId edge_type, uint64_t commit_timestamp);

  /// Returns false if there was no index to drop
  bool DropIndex(EdgeTypeId edge_type) override;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token);

  void DropGraphClearIndices() override;

  void RunGC();

  auto GetActiveIndices() const -> std::unique_ptr<EdgeTypeIndex::ActiveIndices> override;

 private:
  void CleanupAllIndicies();
  auto GetIndividualIndex(EdgeTypeId edge_type) const -> std::shared_ptr<IndividualIndex>;

  utils::Synchronized<std::shared_ptr<IndicesContainer const>, utils::WritePrioritizedRWLock> index_{
      std::make_shared<IndicesContainer>()};

  // For correct GC we need a copy of all indexes, even if dropped, this is so we can ensure dangling ptr are removed
  // even for dropped indices
  utils::Synchronized<std::vector<std::shared_ptr<IndividualIndex>>, utils::WritePrioritizedRWLock> all_indexes_{};
};

}  // namespace memgraph::storage
