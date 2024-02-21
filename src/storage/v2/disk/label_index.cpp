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

#include <rocksdb/options.h>
#include <rocksdb/utilities/transaction.h>

#include "storage/v2/disk/label_index.hpp"
#include "utils/disk_utils.hpp"
#include "utils/rocksdb_serialization.hpp"

namespace memgraph::storage {

namespace {

[[nodiscard]] bool ClearTransactionEntriesWithRemovedIndexingLabel(
    rocksdb::Transaction &disk_transaction, const std::map<Gid, std::vector<LabelId>> &transaction_entries) {
  for (const auto &[vertex_gid, labels] : transaction_entries) {
    for (const auto &indexing_label : labels) {
      if (auto status = disk_transaction.Delete(utils::SerializeVertexAsKeyForLabelIndex(indexing_label, vertex_gid));
          !status.ok()) {
        return false;
      }
    }
  }
  return true;
}

/// TODO: duplication with label_property_index.cpp
bool CommitWithTimestamp(rocksdb::Transaction *disk_transaction, uint64_t commit_ts) {
  disk_transaction->SetCommitTimestamp(commit_ts);
  const auto status = disk_transaction->Commit();
  if (!status.ok()) {
    spdlog::error("rocksdb: {}", status.getState());
  }
  return status.ok();
}

}  // namespace

DiskLabelIndex::DiskLabelIndex(const Config &config) {
  utils::EnsureDirOrDie(config.disk.label_index_directory);
  kvstore_ = std::make_unique<RocksDBStorage>();
  kvstore_->options_.create_if_missing = true;
  kvstore_->options_.comparator = new ComparatorWithU64TsImpl();
  logging::AssertRocksDBStatus(rocksdb::TransactionDB::Open(kvstore_->options_, rocksdb::TransactionDBOptions(),
                                                            config.disk.label_index_directory, &kvstore_->db_));
}

bool DiskLabelIndex::CreateIndex(LabelId label, const std::vector<std::pair<std::string, std::string>> &vertices) {
  if (!index_.emplace(label).second) {
    return false;
  }

  auto disk_transaction = CreateRocksDBTransaction();
  for (const auto &[key, value] : vertices) {
    disk_transaction->Put(key, value);
  }
  return CommitWithTimestamp(disk_transaction.get(), 0);
}

std::unique_ptr<rocksdb::Transaction> DiskLabelIndex::CreateRocksDBTransaction() const {
  return std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
}

std::unique_ptr<rocksdb::Transaction> DiskLabelIndex::CreateAllReadingRocksDBTransaction() const {
  auto tx = CreateRocksDBTransaction();
  tx->SetReadTimestampForValidation(std::numeric_limits<uint64_t>::max());
  return tx;
}

bool DiskLabelIndex::SyncVertexToLabelIndexStorage(const Vertex &vertex, uint64_t commit_timestamp) const {
  auto disk_transaction = CreateRocksDBTransaction();

  if (auto maybe_old_disk_key = utils::GetOldDiskKeyOrNull(vertex.delta()); maybe_old_disk_key.has_value()) {
    if (!disk_transaction->Delete(maybe_old_disk_key.value()).ok()) {
      return false;
    }
  }

  for (const LabelId index_label : index_) {
    if (!utils::Contains(vertex.labels, index_label)) {
      continue;
    }
    if (!disk_transaction
             ->Put(utils::SerializeVertexAsKeyForLabelIndex(index_label, vertex.gid),
                   utils::SerializeVertexAsValueForLabelIndex(index_label, vertex.labels, vertex.properties))
             .ok()) {
      return false;
    }
  }

  return CommitWithTimestamp(disk_transaction.get(), commit_timestamp);
}

/// TODO: this can probably be optimized
bool DiskLabelIndex::ClearDeletedVertex(std::string_view gid, uint64_t transaction_commit_timestamp) const {
  auto disk_transaction = CreateAllReadingRocksDBTransaction();

  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it = std::unique_ptr<rocksdb::Iterator>(disk_transaction->GetIterator(ro));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    if (std::string key = it->key().ToString(); gid == utils::ExtractGidFromLabelIndexStorage(key)) {
      if (!disk_transaction->Delete(key).ok()) {
        return false;
      }
    }
  }

  return CommitWithTimestamp(disk_transaction.get(), transaction_commit_timestamp);
}

bool DiskLabelIndex::DeleteVerticesWithRemovedIndexingLabel(uint64_t transaction_start_timestamp,
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

void DiskLabelIndex::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_before_update, const Transaction &tx) {
  entries_for_deletion.WithLock([added_label, vertex_before_update, &tx](auto &tx_to_entries_for_deletion) {
    auto tx_it = tx_to_entries_for_deletion.find(tx.start_timestamp);
    if (tx_it == tx_to_entries_for_deletion.end()) {
      return;
    }
    auto vertex_label_index_it = tx_it->second.find(vertex_before_update->gid);
    if (vertex_label_index_it == tx_it->second.end()) {
      return;
    }
    std::erase_if(vertex_label_index_it->second,
                  [added_label](const LabelId &indexed_label) { return indexed_label == added_label; });
  });
}

void DiskLabelIndex::UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update, const Transaction &tx) {
  if (!IndexExists(removed_label)) {
    return;
  }
  entries_for_deletion.WithLock([&removed_label, &tx, vertex_before_update](auto &tx_to_entries_for_deletion) {
    auto [it, _] = tx_to_entries_for_deletion.emplace(
        std::piecewise_construct, std::forward_as_tuple(tx.start_timestamp), std::forward_as_tuple());
    auto &vertex_map_store = it->second;
    auto [it_vertex_map_store, emplaced] = vertex_map_store.emplace(
        std::piecewise_construct, std::forward_as_tuple(vertex_before_update->gid), std::forward_as_tuple());
    it_vertex_map_store->second.emplace_back(removed_label);
  });
}

/// TODO: andi Here will come Bloom filter deletion
bool DiskLabelIndex::DropIndex(LabelId label) {
  if (!(index_.erase(label) > 0)) {
    return false;
  }
  auto disk_transaction = CreateAllReadingRocksDBTransaction();

  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it = std::unique_ptr<rocksdb::Iterator>(disk_transaction->GetIterator(ro));
  const std::string serialized_label = label.ToString();
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string key = it->key().ToString();
    if (key.starts_with(serialized_label)) {
      disk_transaction->Delete(it->key().ToString());
    }
  }

  return CommitWithTimestamp(disk_transaction.get(), 0);
}

bool DiskLabelIndex::IndexExists(LabelId label) const { return index_.find(label) != index_.end(); }

std::vector<LabelId> DiskLabelIndex::ListIndices() const { return {index_.begin(), index_.end()}; }

uint64_t DiskLabelIndex::ApproximateVertexCount(LabelId /*label*/) const { return 10; }

void DiskLabelIndex::LoadIndexInfo(const std::vector<std::string> &labels) {
  for (const std::string &label : labels) {
    LabelId label_id = LabelId::FromString(label);
    index_.insert(label_id);
  }
}

RocksDBStorage *DiskLabelIndex::GetRocksDBStorage() const { return kvstore_.get(); }

std::unordered_set<LabelId> DiskLabelIndex::GetInfo() const { return index_; }

}  // namespace memgraph::storage
