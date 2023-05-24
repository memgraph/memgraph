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
 public:
  DiskLabelPropertyIndex(Indices *indices, Constraints *constraints, const Config &config);

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
  std::unique_ptr<RocksDBStorage> kvstore_;
};

}  // namespace memgraph::storage
