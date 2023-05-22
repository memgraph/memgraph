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

#include "storage/v2/indices/label_property_index.hpp"

namespace memgraph::storage {

/// TODO: andi. Too many copies, extract at one place
using ParalellizedIndexCreationInfo =
    std::pair<std::vector<std::pair<Gid, uint64_t>> /*vertex_recovery_info*/, uint64_t /*thread_count*/>;

class DiskLabelPropertyIndex : public storage::LabelPropertyIndex {
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
  DiskLabelPropertyIndex(Indices *indices, Constraints *constraints, Config::Items config)
      : LabelPropertyIndex(indices, constraints, config) {}

  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx) override;

  /// @throw std::bad_alloc
  void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                           const Transaction &tx) override;

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, PropertyId property, utils::SkipList<Vertex>::Accessor vertices,
                   const std::optional<ParalellizedIndexCreationInfo> &paralell_exec_info);

  bool DropIndex(LabelId label, PropertyId property) override { return index_.erase({label, property}) > 0; }

  bool IndexExists(LabelId label, PropertyId property) const override {
    return index_.find({label, property}) != index_.end();
  }

  std::vector<std::pair<LabelId, PropertyId>> ListIndices() const override;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp) override;

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

  int64_t ApproximateVertexCount(LabelId label, PropertyId property) const override {
    auto it = index_.find({label, property});
    MG_ASSERT(it != index_.end(), "Index for label {} and property {} doesn't exist", label.AsUint(),
              property.AsUint());
    return it->second.size();
  }

  /// Supplying a specific value into the count estimation function will return
  /// an estimated count of nodes which have their property's value set to
  /// `value`. If the `value` specified is `Null`, then an average number of
  /// equal elements is returned.
  int64_t ApproximateVertexCount(LabelId label, PropertyId property, const PropertyValue &value) const override;

  int64_t ApproximateVertexCount(LabelId label, PropertyId property,
                                 const std::optional<utils::Bound<PropertyValue>> &lower,
                                 const std::optional<utils::Bound<PropertyValue>> &upper) const override;

  std::vector<std::pair<LabelId, PropertyId>> ClearIndexStats() override;

  std::vector<std::pair<LabelId, PropertyId>> DeleteIndexStatsForLabel(const storage::LabelId &label) override;

  void SetIndexStats(const storage::LabelId &label, const storage::PropertyId &property,
                     const storage::IndexStats &stats) override;

  std::optional<storage::IndexStats> GetIndexStats(const storage::LabelId &label,
                                                   const storage::PropertyId &property) const override;

  void Clear() override { index_.clear(); }

  void RunGC() override;

 private:
  std::map<std::pair<LabelId, PropertyId>, utils::SkipList<Entry>> index_;
  std::map<std::pair<LabelId, PropertyId>, storage::IndexStats> stats_;
  Indices *indices_;
  Constraints *constraints_;
  Config::Items config_;
};

}  // namespace memgraph::storage
