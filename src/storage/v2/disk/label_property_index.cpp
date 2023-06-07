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

/// TODO: clear dependencies

#include "storage/v2/disk/label_property_index.hpp"
#include "storage/v2/inmemory/indices_utils.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

namespace {

bool IsVertexIndexedByLabelProperty(const Vertex &vertex, LabelId label, PropertyId property) {
  return utils::Contains(vertex.labels, label) && vertex.properties.HasProperty(property);
}

}  // namespace

DiskLabelPropertyIndex::DiskLabelPropertyIndex(Indices *indices, Constraints *constraints, const Config &config)
    : LabelPropertyIndex(indices, constraints, config) {
  kvstore_ = std::make_unique<RocksDBStorage>();
  utils::EnsureDirOrDie(config.disk.label_property_index_directory);
  kvstore_->options_.create_if_missing = true;
  kvstore_->options_.comparator = new ComparatorWithU64TsImpl();
  logging::AssertRocksDBStatus(rocksdb::TransactionDB::Open(
      kvstore_->options_, rocksdb::TransactionDBOptions(), config.disk.label_property_index_directory, &kvstore_->db_));
}

bool DiskLabelPropertyIndex::CreateIndex(LabelId label, PropertyId property,
                                         const std::vector<std::pair<std::string, std::string>> &vertices) {
  auto [it, emplaced] = index_.emplace(label, property);
  if (!emplaced) {
    return false;
  }

  /// TODO: how to deal with commit timestamp in a better way
  auto disk_transaction = std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
  for (const auto &[key, value] : vertices) {
    disk_transaction->Put(key, value);
  }
  /// TODO: figure out a better way to handle this since it is a duplicate of InsertConstraint
  disk_transaction->SetCommitTimestamp(0);
  auto status = disk_transaction->Commit();
  if (!status.ok()) {
    spdlog::error("rocksdb: {}", status.getState());
  }
  return status.ok();
}

std::unique_ptr<rocksdb::Transaction> DiskLabelPropertyIndex::CreateRocksDBTransaction() {
  return std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
}

bool DiskLabelPropertyIndex::SyncVertexToLabelPropertyIndexStorage(const Vertex &vertex,
                                                                   uint64_t commit_timestamp) const {
  auto disk_transaction = std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
  for (const auto &[index_label, index_property] : index_) {
    if (IsVertexIndexedByLabelProperty(vertex, index_label, index_property)) {
      if (!disk_transaction
               ->Put(utils::SerializeVertexAsKeyForLabelPropertyIndex(index_label, index_property, vertex.gid),
                     utils::SerializeVertexAsValueForLabelPropertyIndex(index_label, vertex.labels, vertex.properties))
               .ok()) {
        return false;
      }
    }
  }
  disk_transaction->SetCommitTimestamp(commit_timestamp);
  auto status = disk_transaction->Commit();
  if (!status.ok()) {
    spdlog::error("rocksdb: {}", status.getState());
  }
  return status.ok();
}

bool DiskLabelPropertyIndex::ClearDeletedVertex(std::string_view gid, uint64_t transaction_commit_timestamp) const {
  throw utils::NotYetImplemented("DiskLabelPropertyIndex::ClearDeletedVertex");
}

bool DiskLabelPropertyIndex::DeleteVerticesWithRemovedIndexingLabel(uint64_t transaction_start_timestamp,
                                                                    uint64_t transaction_commit_timestamp) {
  throw utils::NotYetImplemented("DiskLabelPropertyIndex::DeleteVerticesWithRemovedIndexingLabel");
}

void DiskLabelPropertyIndex::UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx) {}

void DiskLabelPropertyIndex::UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update,
                                                 const Transaction &tx) {}

/// TODO: andi If stays the same, move it to the hpp
void DiskLabelPropertyIndex::UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                                                 const Transaction &tx) {}

bool DiskLabelPropertyIndex::DropIndex(LabelId label, PropertyId property) {
  return index_.erase({label, property}) > 0;
}

bool DiskLabelPropertyIndex::IndexExists(LabelId label, PropertyId property) const {
  return utils::Contains(index_, std::make_pair(label, property));
}

std::vector<std::pair<LabelId, PropertyId>> DiskLabelPropertyIndex::ListIndices() const {
  return std::vector<std::pair<LabelId, PropertyId>>(index_.begin(), index_.end());
}

void DiskLabelPropertyIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp) {
  throw utils::NotYetImplemented("DiskLabelPropertyIndex::RemoveObsoleteEntries");
}

// These constants represent the smallest possible value of each type that is
// contained in a `PropertyValue`. Note that numbers (integers and doubles) are
// treated as the same "type" in `PropertyValue`.
const PropertyValue kSmallestBool = PropertyValue(false);
static_assert(-std::numeric_limits<double>::infinity() < std::numeric_limits<int64_t>::min());
const PropertyValue kSmallestNumber = PropertyValue(-std::numeric_limits<double>::infinity());
const PropertyValue kSmallestString = PropertyValue("");
const PropertyValue kSmallestList = PropertyValue(std::vector<PropertyValue>());
const PropertyValue kSmallestMap = PropertyValue(std::map<std::string, PropertyValue>());
const PropertyValue kSmallestTemporalData =
    PropertyValue(TemporalData{static_cast<TemporalType>(0), std::numeric_limits<int64_t>::min()});

uint64_t DiskLabelPropertyIndex::ApproximateVertexCount(LabelId label, PropertyId property) const {
  throw utils::NotYetImplemented("DiskLabelPropertyIndex::ApproximateVertexCount");
}

uint64_t DiskLabelPropertyIndex::ApproximateVertexCount(LabelId label, PropertyId property,
                                                        const PropertyValue &value) const {
  throw utils::NotYetImplemented("DiskLabelPropertyIndex::ApproximateVertexCount");
}

uint64_t DiskLabelPropertyIndex::ApproximateVertexCount(LabelId label, PropertyId property,
                                                        const std::optional<utils::Bound<PropertyValue>> &lower,
                                                        const std::optional<utils::Bound<PropertyValue>> &upper) const {
  throw utils::NotYetImplemented("DiskLabelPropertyIndex::ApproximateVertexCount");
}

/// TODO: clear whole RocksDB instance
void DiskLabelPropertyIndex::Clear() { index_.clear(); }

void DiskLabelPropertyIndex::RunGC() { throw utils::NotYetImplemented("DiskLabelPropertyIndex::RunGC"); }

std::vector<std::pair<LabelId, PropertyId>> DiskLabelPropertyIndex::ClearIndexStats() {
  std::vector<std::pair<LabelId, PropertyId>> deleted_indexes;
  deleted_indexes.reserve(stats_.size());
  std::transform(stats_.begin(), stats_.end(), std::back_inserter(deleted_indexes),
                 [](const auto &elem) { return elem.first; });
  stats_.clear();
  return deleted_indexes;
}

std::vector<std::pair<LabelId, PropertyId>> DiskLabelPropertyIndex::DeleteIndexStatsForLabel(
    const storage::LabelId &label) {
  std::vector<std::pair<LabelId, PropertyId>> deleted_indexes;
  for (auto it = stats_.cbegin(); it != stats_.cend();) {
    if (it->first.first == label) {
      deleted_indexes.push_back(it->first);
      it = stats_.erase(it);
    } else {
      ++it;
    }
  }
  return deleted_indexes;
}

void DiskLabelPropertyIndex::SetIndexStats(const storage::LabelId &label, const storage::PropertyId &property,
                                           const IndexStats &stats) {
  stats_[{label, property}] = stats;
}

std::optional<IndexStats> DiskLabelPropertyIndex::GetIndexStats(const storage::LabelId &label,
                                                                const storage::PropertyId &property) const {
  if (auto it = stats_.find({label, property}); it != stats_.end()) {
    return it->second;
  }
  return {};
}

}  // namespace memgraph::storage
