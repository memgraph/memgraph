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

#include "storage/v2/disk/rocksdb_storage.hpp"
#include "storage/v2/indices/label_property_index.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

class DiskLabelPropertyIndex : public storage::LabelPropertyIndex {
 public:
  enum class Status : uint8_t { POPULATE, READY, DROPPING };
  struct IndexInformation {
    LabelId label;
    PropertyId property;
    Status status;

    IndexInformation(LabelId label, PropertyId property) : label(label), property(property), status(Status::POPULATE) {}
    IndexInformation(LabelId label, PropertyId property, Status status)
        : label(label), property(property), status(status) {}

    friend bool operator<(IndexInformation const &lhs, IndexInformation const &rhs) {
      return std::tie(lhs.label, lhs.property) < std::tie(rhs.label, rhs.property);
    }
    friend bool operator==(IndexInformation const &lhs, IndexInformation const &rhs) {
      return std::tie(lhs.label, lhs.property) == std::tie(rhs.label, rhs.property);
    }
  };
  struct ActiveIndices : LabelPropertyIndex::ActiveIndices {
    ActiveIndices(std::set<IndexInformation> index) : index_(std::move(index)), entries_for_deletion{} {}

    void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) override;
    void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                             const Transaction &tx) override;

    bool IndexExists(LabelId label, std::span<PropertyId const> properties) const override;

    auto RelevantLabelPropertiesIndicesInfo(std::span<LabelId const> labels, std::span<PropertyId const> properties)
        const -> std::vector<LabelPropertiesIndicesInfo> override;

    // Not used for in-memory
    void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update, const Transaction &tx) override;

    std::vector<std::pair<LabelId, std::vector<PropertyId>>> ListIndices() const override;

    uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyId const> properties) const override;

    uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyId const> properties,
                                    std::span<PropertyValue const> values) const override;

    uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyId const> properties,
                                    std::span<PropertyValueRange const> bounds) const override;

    std::set<IndexInformation> index_;
    std::map<uint64_t, std::map<Gid, std::vector<std::pair<LabelId, PropertyId>>>> entries_for_deletion;
  };

  explicit DiskLabelPropertyIndex(const Config &config);

  bool CreateIndex(LabelId label, PropertyId property,
                   const std::vector<std::pair<std::string, std::string>> &vertices);

  std::unique_ptr<rocksdb::Transaction> CreateRocksDBTransaction() const;

  std::unique_ptr<rocksdb::Transaction> CreateAllReadingRocksDBTransaction() const;

  [[nodiscard]] bool SyncVertexToLabelPropertyIndexStorage(const Vertex &vertex, uint64_t commit_timestamp) const;

  [[nodiscard]] bool ClearDeletedVertex(std::string_view gid, uint64_t transaction_commit_timestamp) const;

  [[nodiscard]] bool DeleteVerticesWithRemovedIndexingLabel(uint64_t transaction_start_timestamp,
                                                            uint64_t transaction_commit_timestamp);

  bool DropIndex(LabelId label, std::vector<PropertyId> const &properties) override;

  RocksDBStorage *GetRocksDBStorage() const;

  void LoadIndexInfo(const std::vector<std::string> &keys);

  std::set<IndexInformation> GetInfo() const;

  void DropGraphClearIndices() override {};

  auto GetActiveIndices() const -> std::unique_ptr<LabelPropertyIndex::ActiveIndices> override {
    std::set<IndexInformation> index;
    std::map<Gid, std::vector<std::pair<LabelId, PropertyId>>> entries_for_deletion;

    for (auto const &[label, property, status] : index_) {
      if (status != Status::DROPPING) {
        index.emplace(label, property, status);
      }
    }

    return std::make_unique<ActiveIndices>(index);
  }

  void AbortEntries(AbortableInfo const &info, uint64_t start_timestamp) override;

 private:
  // TODO: shouldn't exist
  utils::Synchronized<std::map<uint64_t, std::map<Gid, std::vector<std::pair<LabelId, PropertyId>>>>>
      entries_for_deletion;
  std::set<IndexInformation> index_;
  std::unique_ptr<RocksDBStorage> kvstore_;
};

}  // namespace memgraph::storage
