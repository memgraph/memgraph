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

  bool CreateIndex(LabelId label, PropertyId property,
                   const std::vector<std::pair<std::string, std::string>> &vertices);

  std::unique_ptr<rocksdb::Transaction> CreateRocksDBTransaction();

  [[nodiscard]] bool SyncVertexToLabelPropertyIndexStorage(const Vertex &vertex, uint64_t commit_timestamp) const;

  [[nodiscard]] bool ClearDeletedVertex(std::string_view gid, uint64_t transaction_commit_timestamp) const;

  [[nodiscard]] bool DeleteVerticesWithRemovedIndexingLabel(uint64_t transaction_start_timestamp,
                                                            uint64_t transaction_commit_timestamp);

  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_before_update, const Transaction &tx) override;

  void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update, const Transaction &tx) override;

  /// @throw std::bad_alloc
  void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                           const Transaction &tx) override;

  bool DropIndex(LabelId label, PropertyId property) override;

  bool IndexExists(LabelId label, PropertyId property) const override;

  std::vector<std::pair<LabelId, PropertyId>> ListIndices() const override;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp) override;

  uint64_t ApproximateVertexCount(LabelId label, PropertyId property) const override;

  uint64_t ApproximateVertexCount(LabelId label, PropertyId property, const PropertyValue &value) const override;

  uint64_t ApproximateVertexCount(LabelId label, PropertyId property,
                                  const std::optional<utils::Bound<PropertyValue>> &lower,
                                  const std::optional<utils::Bound<PropertyValue>> &upper) const override;

  std::vector<std::pair<LabelId, PropertyId>> ClearIndexStats() override;

  std::vector<std::pair<LabelId, PropertyId>> DeleteIndexStatsForLabel(const storage::LabelId &label) override;

  void SetIndexStats(const storage::LabelId &label, const storage::PropertyId &property,
                     const storage::IndexStats &stats) override;

  std::optional<storage::IndexStats> GetIndexStats(const storage::LabelId &label,
                                                   const storage::PropertyId &property) const override;

  void Clear() override;

  void RunGC() override;

  RocksDBStorage *GetRocksDBStorage() const;

 private:
  utils::Synchronized<std::map<uint64_t, std::map<Gid, std::vector<std::pair<LabelId, PropertyId>>>>>
      entries_for_deletion;
  std::set<std::pair<LabelId, PropertyId>> index_;
  std::map<std::pair<LabelId, PropertyId>, storage::IndexStats> stats_;
  std::unique_ptr<RocksDBStorage> kvstore_;
};

}  // namespace memgraph::storage
