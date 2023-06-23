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
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/indices_utils.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/disk_utils.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

namespace {

bool IsVertexIndexedByLabelProperty(const Vertex &vertex, LabelId label, PropertyId property) {
  return utils::Contains(vertex.labels, label) && vertex.properties.HasProperty(property);
}

[[nodiscard]] bool ClearTransactionEntriesWithRemovedIndexingLabel(
    rocksdb::Transaction &disk_transaction,
    const std::map<Gid, std::vector<std::pair<LabelId, PropertyId>>> &transaction_entries) {
  for (const auto &[vertex_gid, index] : transaction_entries) {
    for (const auto &[indexing_label, indexing_property] : index) {
      if (auto status = disk_transaction.Delete(
              utils::SerializeVertexAsKeyForLabelPropertyIndex(indexing_label, indexing_property, vertex_gid));
          !status.ok()) {
        return false;
      }
    }
  }
  return true;
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
  if (!index_.emplace(label, property).second) {
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

  if (auto maybe_old_disk_key = utils::GetOldDiskKeyOrNull(vertex.delta); maybe_old_disk_key.has_value()) {
    spdlog::debug("Found old disk key {} for vertex {}", maybe_old_disk_key.value(),
                  utils::SerializeIdType(vertex.gid));
    if (auto status = disk_transaction->Delete(maybe_old_disk_key.value()); !status.ok()) {
      return false;
    }
  }
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
  auto disk_transaction = std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
  disk_transaction->SetReadTimestampForValidation(std::numeric_limits<uint64_t>::max());

  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it = std::unique_ptr<rocksdb::Iterator>(disk_transaction->GetIterator(ro));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    if (std::string key = it->key().ToString(); gid == utils::ExtractGidFromLabelPropertyIndexStorage(key)) {
      if (!disk_transaction->Delete(key).ok()) {
        return false;
      }
    }
  }
  disk_transaction->SetCommitTimestamp(transaction_commit_timestamp);
  auto status = disk_transaction->Commit();
  if (!status.ok()) {
    spdlog::error("rocksdb: {}", status.getState());
  }
  return status.ok();
}

bool DiskLabelPropertyIndex::DeleteVerticesWithRemovedIndexingLabel(uint64_t transaction_start_timestamp,
                                                                    uint64_t transaction_commit_timestamp) {
  auto disk_transaction = std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
  disk_transaction->SetReadTimestampForValidation(std::numeric_limits<uint64_t>::max());

  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  bool deletion_success = true;
  entries_for_deletion.WithLock([&deletion_success, transaction_start_timestamp,
                                 disk_transaction_ptr = disk_transaction.get()](auto &tx_to_entries_for_deletion) {
    if (auto tx_it = tx_to_entries_for_deletion.find(transaction_start_timestamp);
        tx_it != tx_to_entries_for_deletion.end()) {
      deletion_success = ClearTransactionEntriesWithRemovedIndexingLabel(*disk_transaction_ptr, tx_it->second);
      tx_to_entries_for_deletion.erase(tx_it);
    }
  });
  if (deletion_success) {
    /// TODO: Extract to some useful method
    disk_transaction->SetCommitTimestamp(transaction_commit_timestamp);
    auto status = disk_transaction->Commit();
    if (!status.ok()) {
      /// TODO: better naming
      spdlog::error("rocksdb: {}", status.getState());
    }
    return status.ok();
  }
  spdlog::error("Deletetion of vertices with removed indexing label failed.");
  return false;
}

void DiskLabelPropertyIndex::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_before_update,
                                              const Transaction &tx) {
  entries_for_deletion.WithLock([added_label, vertex_before_update, &tx](auto &tx_to_entries_for_deletion) {
    if (auto tx_it = tx_to_entries_for_deletion.find(tx.start_timestamp); tx_it != tx_to_entries_for_deletion.end()) {
      if (auto vertex_label_index_it = tx_it->second.find(vertex_before_update->gid);
          vertex_label_index_it != tx_it->second.end()) {
        std::erase_if(vertex_label_index_it->second, [added_label](const std::pair<LabelId, PropertyId> &index) {
          return index.first == added_label;
        });
      }
    }
  });
}

void DiskLabelPropertyIndex::UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update,
                                                 const Transaction &tx) {
  for (const auto &index_entry : index_) {
    if (index_entry.first == removed_label) {
      entries_for_deletion.WithLock([&index_entry, &tx, vertex_before_update](auto &tx_to_entries_for_deletion) {
        const auto &[indexing_label, indexing_property] = index_entry;
        auto [it, _] = tx_to_entries_for_deletion.emplace(
            std::piecewise_construct, std::forward_as_tuple(tx.start_timestamp), std::forward_as_tuple());
        auto &vertex_map_store = it->second;
        auto [it_vertex_map_store, emplaced] = vertex_map_store.emplace(
            std::piecewise_construct, std::forward_as_tuple(vertex_before_update->gid), std::forward_as_tuple());
        it_vertex_map_store->second.emplace_back(indexing_label, indexing_property);
        spdlog::debug("Added to the map label index storage for vertex {} with label {}",
                      utils::SerializeIdType(vertex_before_update->gid), utils::SerializeIdType(indexing_label));
      });
    }
  }
}

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

void DiskLabelPropertyIndex::LoadIndexInfo(const std::vector<std::string> &keys) {
  for (const auto &label_property : keys) {
    std::vector<std::string> label_property_split = utils::Split(label_property, ",");
    index_.emplace(std::make_pair(LabelId::FromUint(std::stoull(label_property_split[0])),
                                  PropertyId::FromUint(std::stoull(label_property_split[1]))));
  }
}

RocksDBStorage *DiskLabelPropertyIndex::GetRocksDBStorage() const { return kvstore_.get(); }

}  // namespace memgraph::storage
