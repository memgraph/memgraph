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

#include "storage/v2/disk/label_property_index.hpp"
#include "storage/v2/inmemory/indices_utils.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

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
  index_.emplace(label, property);
  rocksdb::WriteOptions wo;
  for (const auto &[key, value] : vertices) {
    kvstore_->db_->Put(wo, key, value);
  }
  return true;
}

void DiskLabelPropertyIndex::UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx) {
  /// TODO: andi iterate over whole set
  // throw utils::NotYetImplemented("DiskLabelPropertyIndex::UpdateOnAddLabel");
}

void DiskLabelPropertyIndex::UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                                                 const Transaction &tx) {
  if (value.IsNull()) {
    return;
  }
  /// TODO: andi iterate over whole set
  // throw utils::NotYetImplemented("DiskLabelPropertyIndex::UpdateOnSetProperty");
}

bool DiskLabelPropertyIndex::DropIndex(LabelId label, PropertyId property) {
  return index_.erase({label, property}) > 0;
}

bool DiskLabelPropertyIndex::IndexExists(LabelId label, PropertyId property) const {
  return index_.find({label, property}) != index_.end();
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

void DiskLabelPropertyIndex::Clear() { index_.clear(); }

void DiskLabelPropertyIndex::RunGC() { throw utils::NotYetImplemented("DiskLabelPropertyIndex::RunGC"); }

std::unique_ptr<rocksdb::Iterator> DiskLabelPropertyIndex::CreateRocksDBIterator() {
  return std::unique_ptr<rocksdb::Iterator>(kvstore_->db_->NewIterator(rocksdb::ReadOptions()));
}

}  // namespace memgraph::storage
