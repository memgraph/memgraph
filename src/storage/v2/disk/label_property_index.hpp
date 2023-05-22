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

#include "storage/v2/disk/rocksdb_storage.hpp"
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
  DiskLabelPropertyIndex(Indices *indices, Constraints *constraints, Config::Items config);

  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx) override;

  /// @throw std::bad_alloc
  void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                           const Transaction &tx) override;

  // Key: INDEX_LABEL,INDEX_PROPERTY_KEY,OTHER_LABEL_1,OTHER_LABEL_2, ..|GID
  // Value: VERTEX.PROPERTIES
  /// TODO: andi Whenever vertex is updated you should go to the disk if it is indexed.
  /// Optimize by using prefixed Bloom filters
  bool CreateIndex(LabelId label, PropertyId property,
                   const std::vector<std::pair<std::string, std::string>> &vertices);

  bool DropIndex(LabelId label, PropertyId property) override;

  bool IndexExists(LabelId label, PropertyId property) const override;

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

  int64_t ApproximateVertexCount(LabelId label, PropertyId property) const override;

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

  std::unique_ptr<rocksdb::Iterator> CreateRocksDBIterator() {
    return std::unique_ptr<rocksdb::Iterator>(kvstore_->db_->NewIterator(rocksdb::ReadOptions()));
  }

  void Clear() override;

  void RunGC() override;

 private:
  /// TODO: andi Optimize by using unordered_set
  std::set<std::pair<LabelId, PropertyId>> index_;
  std::map<std::pair<LabelId, PropertyId>, storage::IndexStats> stats_;
  Indices *indices_;
  Constraints *constraints_;
  Config::Items config_;
  std::unique_ptr<RocksDBStorage> kvstore_;
};

}  // namespace memgraph::storage
