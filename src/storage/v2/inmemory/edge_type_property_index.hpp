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
#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/edge_type_property_index.hpp"
#include "storage/v2/indices/errors.hpp"
#include "storage/v2/inmemory/indices_mvcc.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/rw_lock.hpp"

namespace memgraph::storage {

class InMemoryEdgeTypePropertyIndex : public storage::EdgeTypePropertyIndex {
 private:
  struct Entry {
    PropertyValue value;
    Vertex *from_vertex;
    Vertex *to_vertex;
    Edge *edge;

    uint64_t timestamp;

    friend bool operator<(Entry const &lhs, Entry const &rhs) {
      return std::tie(lhs.value, lhs.edge, lhs.from_vertex, lhs.to_vertex, lhs.timestamp) <
             std::tie(rhs.value, rhs.edge, rhs.from_vertex, rhs.to_vertex, rhs.timestamp);
    };

    friend bool operator==(Entry const &lhs, Entry const &rhs) {
      return std::tie(lhs.value, lhs.edge, lhs.from_vertex, lhs.to_vertex, lhs.timestamp) ==
             std::tie(rhs.value, rhs.edge, rhs.from_vertex, rhs.to_vertex, rhs.timestamp);
    }

    bool operator<(const PropertyValue &rhs) const;
    bool operator==(const PropertyValue &rhs) const;
  };

 public:
  class Iterable {
   public:
    Iterable(utils::SkipList<Entry>::Accessor index_accessor, utils::SkipList<Vertex>::ConstAccessor vertex_accessor,
             utils::SkipList<Edge>::ConstAccessor edge_accessor, EdgeTypeId edge_type, PropertyId property,
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

  struct IndividualIndex {
    IndividualIndex() {}
    ~IndividualIndex();
    void Publish(uint64_t commit_timestamp);
    utils::SkipList<Entry> skiplist{};
    IndexStatus status{};
  };

  // TODO: change to map of maps as in label property index
  using IndexContainer = std::map<std::pair<EdgeTypeId, PropertyId>, std::shared_ptr<IndividualIndex>>;

  struct AllIndicesEntry {
    std::shared_ptr<IndividualIndex> index_;
    PropertyId property_;
    // edge type can't be changed so not needed here
  };
  struct ActiveIndices : storage::EdgeTypePropertyIndex::ActiveIndices {
    explicit ActiveIndices(std::shared_ptr<IndexContainer const> indices = std::make_shared<IndexContainer>())
        : index_container_{std::move(indices)} {}

    void UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge, EdgeTypeId edge_type,
                             PropertyId property, PropertyValue value, uint64_t timestamp) override;

    void UpdateOnEdgeModification(Vertex *old_from, Vertex *old_to, Vertex *new_from, Vertex *new_to, EdgeRef edge_ref,
                                  EdgeTypeId edge_type, PropertyId property, const PropertyValue &value,
                                  const Transaction &tx) override;

    uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property) const override;

    uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property, const PropertyValue &value) const override;

    uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property,
                                  const std::optional<utils::Bound<PropertyValue>> &lower,
                                  const std::optional<utils::Bound<PropertyValue>> &upper) const override;

    bool IndexReady(EdgeTypeId edge_type, PropertyId property) const override;

    auto ListIndices(uint64_t start_timestamp) const -> std::vector<std::pair<EdgeTypeId, PropertyId>> override;

    Iterable Edges(EdgeTypeId edge_type, PropertyId property,
                   const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                   const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
                   Transaction *transaction);

    void AbortEntries(std::pair<EdgeTypeId, PropertyId> edge_type_property,
                      std::span<std::tuple<Vertex *const, Vertex *const, Edge *const, PropertyValue> const> edges,
                      uint64_t exact_start_timestamp);

   private:
    std::shared_ptr<IndexContainer const> index_container_;
  };

  InMemoryEdgeTypePropertyIndex() = default;

  /// @throw std::bad_alloc
  bool CreateIndexOnePass(EdgeTypeId edge_type, PropertyId property, utils::SkipList<Vertex>::Accessor vertices,
                          std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  /// Returns false if there was no index to drop
  bool DropIndex(EdgeTypeId edge_type, PropertyId property) override;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token);

  void DropGraphClearIndices() override;

  // TODO (ivan): what is this used for
  IndexStats Analysis() const {
    IndexStats stats;
    index_.WithReadLock([&](auto const &index_accessor) {
      for (const auto &[key, index] : *index_accessor) {
        stats.et2p[key.first].push_back(key.second);
        stats.p2et[key.second].push_back(key.first);
      }
    });
    return stats;
  }

  auto GetActiveIndices() const -> std::unique_ptr<EdgeTypePropertyIndex::ActiveIndices> override;

  auto RegisterIndex(EdgeTypeId edge_type, PropertyId property) -> bool;
  auto PopulateIndex(EdgeTypeId edge_type, PropertyId property, utils::SkipList<Vertex>::Accessor vertices,
                     std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt,
                     Transaction const *tx = nullptr,
                     CheckCancelFunction cancel_check = neverCancel) -> utils::BasicResult<IndexPopulateError>;
  bool PublishIndex(EdgeTypeId edge_type, PropertyId property, uint64_t commit_timestamp);

  void RunGC();

 private:
  auto CleanupAllIndices() -> void;
  auto GetIndividualIndex(EdgeTypeId edge_type, PropertyId property) const -> std::shared_ptr<IndividualIndex>;
  auto RemoveIndividualIndex(EdgeTypeId edge_type, PropertyId property) -> bool;

  utils::Synchronized<std::shared_ptr<IndexContainer const>, utils::WritePrioritizedRWLock> index_{
      std::make_shared<IndexContainer>()};
  utils::Synchronized<std::shared_ptr<std::vector<AllIndicesEntry>>, utils::WritePrioritizedRWLock> all_indices_{
      std::make_shared<std::vector<AllIndicesEntry>>()};
};

}  // namespace memgraph::storage
