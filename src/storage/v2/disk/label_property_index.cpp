// Copyright 2024 Memgraph Ltd.
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
#include "utils/disk_utils.hpp"
#include "utils/rocksdb_serialization.hpp"

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

bool CommitWithTimestamp(rocksdb::Transaction *disk_transaction, uint64_t commit_ts) {
  disk_transaction->SetCommitTimestamp(commit_ts);
  const auto status = disk_transaction->Commit();
  if (!status.ok()) {
    spdlog::error("rocksdb: {}", status.getState());
  }
  return status.ok();
}

}  // namespace

DiskLabelPropertyIndex::DiskLabelPropertyIndex(const Config &config) {
  utils::EnsureDirOrDie(config.disk.label_property_index_directory);
  kvstore_ = std::make_unique<RocksDBStorage>();
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

  auto disk_transaction = CreateRocksDBTransaction();
  for (const auto &[key, value] : vertices) {
    disk_transaction->Put(key, value);
  }

  return CommitWithTimestamp(disk_transaction.get(), 0);
}

std::unique_ptr<rocksdb::Transaction> DiskLabelPropertyIndex::CreateRocksDBTransaction() const {
  return std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
}

std::unique_ptr<rocksdb::Transaction> DiskLabelPropertyIndex::CreateAllReadingRocksDBTransaction() const {
  auto tx = CreateRocksDBTransaction();
  tx->SetReadTimestampForValidation(std::numeric_limits<uint64_t>::max());
  return tx;
}

bool DiskLabelPropertyIndex::SyncVertexToLabelPropertyIndexStorage(const Vertex &vertex,
                                                                   uint64_t commit_timestamp) const {
  auto disk_transaction = CreateRocksDBTransaction();

  if (auto maybe_old_disk_key = utils::GetOldDiskKeyOrNull(vertex.delta()); maybe_old_disk_key.has_value()) {
    if (!disk_transaction->Delete(maybe_old_disk_key.value()).ok()) {
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
  return CommitWithTimestamp(disk_transaction.get(), commit_timestamp);
}

bool DiskLabelPropertyIndex::ClearDeletedVertex(std::string_view gid, uint64_t transaction_commit_timestamp) const {
  auto disk_transaction = CreateAllReadingRocksDBTransaction();

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
  return CommitWithTimestamp(disk_transaction.get(), transaction_commit_timestamp);
}

bool DiskLabelPropertyIndex::DeleteVerticesWithRemovedIndexingLabel(uint64_t transaction_start_timestamp,
                                                                    uint64_t transaction_commit_timestamp) {
  if (entries_for_deletion->empty()) {
    return true;
  }
  auto disk_transaction = CreateAllReadingRocksDBTransaction();

  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  bool deletion_success = entries_for_deletion.WithLock(
      [transaction_start_timestamp, disk_transaction_ptr = disk_transaction.get()](auto &tx_to_entries_for_deletion) {
        if (auto tx_it = tx_to_entries_for_deletion.find(transaction_start_timestamp);
            tx_it != tx_to_entries_for_deletion.end()) {
          bool res = ClearTransactionEntriesWithRemovedIndexingLabel(*disk_transaction_ptr, tx_it->second);
          tx_to_entries_for_deletion.erase(tx_it);
          return res;
        }
        return true;
      });
  if (deletion_success) {
    return CommitWithTimestamp(disk_transaction.get(), transaction_commit_timestamp);
  }
  return false;
}

void DiskLabelPropertyIndex::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) {
  entries_for_deletion.WithLock([added_label, vertex_after_update, &tx](auto &tx_to_entries_for_deletion) {
    auto tx_it = tx_to_entries_for_deletion.find(tx.start_timestamp);
    if (tx_it == tx_to_entries_for_deletion.end()) {
      return;
    }
    auto vertex_label_index_it = tx_it->second.find(vertex_after_update->gid);
    if (vertex_label_index_it == tx_it->second.end()) {
      return;
    }
    std::erase_if(vertex_label_index_it->second,
                  [added_label](const std::pair<LabelId, PropertyId> &index) { return index.first == added_label; });
  });
}

void DiskLabelPropertyIndex::UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update,
                                                 const Transaction &tx) {
  for (const auto &index_entry : index_) {
    if (index_entry.first != removed_label) {
      continue;
    }
    entries_for_deletion.WithLock([&index_entry, &tx, vertex_after_update](auto &tx_to_entries_for_deletion) {
      const auto &[indexing_label, indexing_property] = index_entry;
      auto [it, _] = tx_to_entries_for_deletion.emplace(
          std::piecewise_construct, std::forward_as_tuple(tx.start_timestamp), std::forward_as_tuple());
      auto &vertex_map_store = it->second;
      auto [it_vertex_map_store, emplaced] = vertex_map_store.emplace(
          std::piecewise_construct, std::forward_as_tuple(vertex_after_update->gid), std::forward_as_tuple());
      it_vertex_map_store->second.emplace_back(indexing_label, indexing_property);
    });
  }
}

bool DiskLabelPropertyIndex::DropIndex(LabelId label, PropertyId property) {
  return index_.erase({label, property}) > 0U;
}

bool DiskLabelPropertyIndex::IndexExists(LabelId label, PropertyId property) const {
  return utils::Contains(index_, std::make_pair(label, property));
}

std::vector<std::pair<LabelId, PropertyId>> DiskLabelPropertyIndex::ListIndices() const {
  return {index_.begin(), index_.end()};
}

uint64_t DiskLabelPropertyIndex::ApproximateVertexCount(LabelId /*label*/, PropertyId /*property*/) const { return 10; }

uint64_t DiskLabelPropertyIndex::ApproximateVertexCount(LabelId /*label*/, PropertyId /*property*/,
                                                        const PropertyValue & /*value*/) const {
  return 10;
}

uint64_t DiskLabelPropertyIndex::ApproximateVertexCount(
    LabelId /*label*/, PropertyId /*property*/, const std::optional<utils::Bound<PropertyValue>> & /*lower*/,
    const std::optional<utils::Bound<PropertyValue>> & /*upper*/) const {
  return 10;
}

void DiskLabelPropertyIndex::LoadIndexInfo(const std::vector<std::string> &keys) {
  for (const auto &label_property : keys) {
    std::vector<std::string> label_property_split = utils::Split(label_property, ",");
    index_.emplace(LabelId::FromString(label_property_split[0]), PropertyId::FromString(label_property_split[1]));
  }
}

RocksDBStorage *DiskLabelPropertyIndex::GetRocksDBStorage() const { return kvstore_.get(); }

std::set<std::pair<LabelId, PropertyId>> DiskLabelPropertyIndex::GetInfo() const { return index_; }

}  // namespace memgraph::storage
