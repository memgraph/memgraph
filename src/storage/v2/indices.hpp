// Copyright 2023 Memgraph Ltd.
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

#include <optional>
#include <tuple>
#include <utility>

#include "storage/v2/config.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/bound.hpp"
#include "utils/logging.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

struct Indices;
struct Constraints;

using ParalellizedIndexCreationInfo =
    std::pair<std::vector<std::pair<Gid, uint64_t>> /*vertex_recovery_info*/, uint64_t /*thread_count*/>;

class LabelIndex {
 private:
  struct Entry {
    Vertex *vertex;
    uint64_t timestamp;

    bool operator<(const Entry &rhs) {
      return std::make_tuple(vertex, timestamp) < std::make_tuple(rhs.vertex, rhs.timestamp);
    }
    bool operator==(const Entry &rhs) { return vertex == rhs.vertex && timestamp == rhs.timestamp; }
  };

  struct LabelStorage {
    LabelId label;
    utils::SkipList<Entry> vertices;

    bool operator<(const LabelStorage &rhs) { return label < rhs.label; }
    bool operator<(LabelId rhs) { return label < rhs; }
    bool operator==(const LabelStorage &rhs) { return label == rhs.label; }
    bool operator==(LabelId rhs) { return label == rhs; }
  };

 public:
  LabelIndex(Indices *indices, Constraints *constraints, Config::Items config)
      : indices_(indices), constraints_(constraints), config_(config) {}

  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx);

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, utils::SkipList<Vertex>::Accessor vertices,
                   const std::optional<ParalellizedIndexCreationInfo> &parallel_exec_info = std::nullopt);

  /// Returns false if there was no index to drop
  bool DropIndex(LabelId label) { return index_.erase(label) > 0; }

  bool IndexExists(LabelId label) const { return index_.find(label) != index_.end(); }

  std::vector<LabelId> ListIndices() const;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp);

  class Iterable {
   public:
    Iterable(utils::SkipList<Entry>::Accessor index_accessor, LabelId label, View view, Transaction *transaction,
             Indices *indices, Constraints *constraints, Config::Items config);

    class Iterator {
     public:
      Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator);

      VertexAccessor operator*() const { return current_vertex_accessor_; }

      bool operator==(const Iterator &other) const { return index_iterator_ == other.index_iterator_; }
      bool operator!=(const Iterator &other) const { return index_iterator_ != other.index_iterator_; }

      Iterator &operator++();

     private:
      void AdvanceUntilValid();

      Iterable *self_;
      utils::SkipList<Entry>::Iterator index_iterator_;
      VertexAccessor current_vertex_accessor_;
      Vertex *current_vertex_;
    };

    Iterator begin() { return Iterator(this, index_accessor_.begin()); }
    Iterator end() { return Iterator(this, index_accessor_.end()); }

   private:
    utils::SkipList<Entry>::Accessor index_accessor_;
    LabelId label_;
    View view_;
    Transaction *transaction_;
    Indices *indices_;
    Constraints *constraints_;
    Config::Items config_;
  };

  /// Returns an self with vertices visible from the given transaction.
  Iterable Vertices(LabelId label, View view, Transaction *transaction) {
    auto it = index_.find(label);
    MG_ASSERT(it != index_.end(), "Index for label {} doesn't exist", label.AsUint());
    return Iterable(it->second.access(), label, view, transaction, indices_, constraints_, config_);
  }

  int64_t ApproximateVertexCount(LabelId label) {
    auto it = index_.find(label);
    MG_ASSERT(it != index_.end(), "Index for label {} doesn't exist", label.AsUint());
    return it->second.size();
  }

  void Clear() { index_.clear(); }

  void RunGC();

 private:
  std::map<LabelId, utils::SkipList<Entry>> index_;
  Indices *indices_;
  Constraints *constraints_;
  Config::Items config_;
};

struct IndexStats {
  double statistic, avg_group_size;
};

class LabelPropertyIndex {
 private:
  struct Entry {
    PropertyValue value;
    Vertex *vertex;
    uint64_t timestamp;

    bool operator<(const Entry &rhs);
    bool operator==(const Entry &rhs);

    bool operator<(const PropertyValue &rhs);
    bool operator==(const PropertyValue &rhs);
  };

 public:
  LabelPropertyIndex(Indices *indices, Constraints *constraints, Config::Items config)
      : indices_(indices), constraints_(constraints), config_(config) {}

  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx);

  /// @throw std::bad_alloc
  void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex, const Transaction &tx);

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, PropertyId property, utils::SkipList<Vertex>::Accessor vertices,
                   const std::optional<ParalellizedIndexCreationInfo> &parallel_exec_info = std::nullopt);

  bool DropIndex(LabelId label, PropertyId property) { return index_.erase({label, property}) > 0; }

  bool IndexExists(LabelId label, PropertyId property) const { return index_.find({label, property}) != index_.end(); }

  std::vector<std::pair<LabelId, PropertyId>> ListIndices() const;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp);

  class Iterable {
   public:
    Iterable(utils::SkipList<Entry>::Accessor index_accessor, LabelId label, PropertyId property,
             const std::optional<utils::Bound<PropertyValue>> &lower_bound,
             const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Transaction *transaction,
             Indices *indices, Constraints *constraints, Config::Items config);

    class Iterator {
     public:
      Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator);

      VertexAccessor operator*() const { return current_vertex_accessor_; }

      bool operator==(const Iterator &other) const { return index_iterator_ == other.index_iterator_; }
      bool operator!=(const Iterator &other) const { return index_iterator_ != other.index_iterator_; }

      Iterator &operator++();

     private:
      void AdvanceUntilValid();

      Iterable *self_;
      utils::SkipList<Entry>::Iterator index_iterator_;
      VertexAccessor current_vertex_accessor_;
      Vertex *current_vertex_;
    };

    Iterator begin();
    Iterator end();

   private:
    utils::SkipList<Entry>::Accessor index_accessor_;
    LabelId label_;
    PropertyId property_;
    std::optional<utils::Bound<PropertyValue>> lower_bound_;
    std::optional<utils::Bound<PropertyValue>> upper_bound_;
    bool bounds_valid_{true};
    View view_;
    Transaction *transaction_;
    Indices *indices_;
    Constraints *constraints_;
    Config::Items config_;
  };

  Iterable Vertices(LabelId label, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view,
                    Transaction *transaction) {
    auto it = index_.find({label, property});
    MG_ASSERT(it != index_.end(), "Index for label {} and property {} doesn't exist", label.AsUint(),
              property.AsUint());
    return Iterable(it->second.access(), label, property, lower_bound, upper_bound, view, transaction, indices_,
                    constraints_, config_);
  }

  int64_t ApproximateVertexCount(LabelId label, PropertyId property) const {
    auto it = index_.find({label, property});
    MG_ASSERT(it != index_.end(), "Index for label {} and property {} doesn't exist", label.AsUint(),
              property.AsUint());
    return it->second.size();
  }

  /// Supplying a specific value into the count estimation function will return
  /// an estimated count of nodes which have their property's value set to
  /// `value`. If the `value` specified is `Null`, then an average number of
  /// equal elements is returned.
  int64_t ApproximateVertexCount(LabelId label, PropertyId property, const PropertyValue &value) const;

  int64_t ApproximateVertexCount(LabelId label, PropertyId property,
                                 const std::optional<utils::Bound<PropertyValue>> &lower,
                                 const std::optional<utils::Bound<PropertyValue>> &upper) const;

  std::vector<std::pair<LabelId, PropertyId>> ClearIndexStats();

  std::vector<std::pair<LabelId, PropertyId>> DeleteIndexStatsForLabel(const storage::LabelId &label);

  void SetIndexStats(const storage::LabelId &label, const storage::PropertyId &property,
                     const storage::IndexStats &stats);

  std::optional<storage::IndexStats> GetIndexStats(const storage::LabelId &label,
                                                   const storage::PropertyId &property) const;

  void Clear() { index_.clear(); }

  void RunGC();

 private:
  std::map<std::pair<LabelId, PropertyId>, utils::SkipList<Entry>> index_;
  std::map<std::pair<LabelId, PropertyId>, storage::IndexStats> stats_;
  Indices *indices_;
  Constraints *constraints_;
  Config::Items config_;
};

struct Indices {
  Indices(Constraints *constraints, Config::Items config)
      : label_index(this, constraints, config), label_property_index(this, constraints, config) {}

  // Disable copy and move because members hold pointer to `this`.
  Indices(const Indices &) = delete;
  Indices(Indices &&) = delete;
  Indices &operator=(const Indices &) = delete;
  Indices &operator=(Indices &&) = delete;
  ~Indices() = default;

  LabelIndex label_index;
  LabelPropertyIndex label_property_index;
};

/// This function should be called from garbage collection to clean-up the
/// index.
void RemoveObsoleteEntries(Indices *indices, uint64_t oldest_active_start_timestamp);

// Indices are updated whenever an update occurs, instead of only on commit or
// advance command. This is necessary because we want indices to support `NEW`
// view for use in Merge.

/// This function should be called whenever a label is added to a vertex.
/// @throw std::bad_alloc
void UpdateOnAddLabel(Indices *indices, LabelId label, Vertex *vertex, const Transaction &tx);

/// This function should be called whenever a property is modified on a vertex.
/// @throw std::bad_alloc
void UpdateOnSetProperty(Indices *indices, PropertyId property, const PropertyValue &value, Vertex *vertex,
                         const Transaction &tx);
}  // namespace memgraph::storage
