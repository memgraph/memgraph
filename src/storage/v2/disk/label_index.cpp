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

#include <rocksdb/options.h>
#include <rocksdb/utilities/transaction.h>
#include <tuple>
#include <utility>

#include "storage/v2/disk/label_index.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/indices_utils.hpp"
#include "utils/file.hpp"
#include "utils/rocksdb_serialization.hpp"

namespace memgraph::storage {

namespace {

bool VertexHasLabel(const Vertex &vertex, LabelId label) {
  return std::find(vertex.labels.begin(), vertex.labels.end(), label) != vertex.labels.end();
}

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

}  // namespace

DiskLabelIndex::DiskLabelIndex(Indices *indices, Constraints *constraints, const Config &config)
    : LabelIndex(indices, constraints, config) {
  kvstore_ = std::make_unique<RocksDBStorage>();
  utils::EnsureDirOrDie(config.disk.label_index_directory);
  kvstore_->options_.create_if_missing = true;
  kvstore_->options_.comparator = new ComparatorWithU64TsImpl();
  logging::AssertRocksDBStatus(rocksdb::TransactionDB::Open(kvstore_->options_, rocksdb::TransactionDBOptions(),
                                                            config.disk.label_index_directory, &kvstore_->db_));
}

bool DiskLabelIndex::CreateIndex(LabelId label, const std::vector<std::pair<std::string, std::string>> &vertices) {
  index_.emplace(label);
  /// How to remove duplication
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

std::unique_ptr<rocksdb::Transaction> DiskLabelIndex::CreateRocksDBTransaction() {
  return std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
}

bool DiskLabelIndex::SyncVertexToLabelIndexStorage(const Vertex &vertex, uint64_t commit_timestamp) const {
  auto disk_transaction = std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
  for (const LabelId index_label : index_) {
    if (VertexHasLabel(vertex, index_label)) {
      /// TODO: andi, probably no need to transfer separately labels and gid
      if (!disk_transaction
               ->Put(utils::SerializeVertexAsKeyForLabelIndex(index_label, vertex.gid),
                     utils::SerializeVertexAsValueForLabelIndex(index_label, vertex.labels, vertex.properties))
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

/// TODO: this can probably be optimized
bool DiskLabelIndex::ClearDeletedVertex(std::string_view gid, uint64_t transaction_commit_timestamp) const {
  auto disk_transaction = std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
  disk_transaction->SetReadTimestampForValidation(std::numeric_limits<uint64_t>::max());

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
  disk_transaction->SetCommitTimestamp(transaction_commit_timestamp);
  auto status = disk_transaction->Commit();
  if (!status.ok()) {
    spdlog::error("rocksdb: {}", status.getState());
  }
  return status.ok();
}

bool DiskLabelIndex::DeleteVerticesWithRemovedIndexingLabel(uint64_t transaction_start_timestamp,
                                                            uint64_t transaction_commit_timestamp) {
  spdlog::debug("Called delete vertices with removed indexing label");
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
      spdlog::debug("Found tx with removed indexing label, deleting entries");
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

void DiskLabelIndex::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_before_update, const Transaction &tx) {
  entries_for_deletion.WithLock([added_label, vertex_before_update, &tx](auto &tx_to_entries_for_deletion) {
    if (auto tx_it = tx_to_entries_for_deletion.find(tx.start_timestamp); tx_it != tx_to_entries_for_deletion.end()) {
      if (auto vertex_label_index_it = tx_it->second.find(vertex_before_update->gid);
          vertex_label_index_it != tx_it->second.end()) {
        std::erase_if(vertex_label_index_it->second,
                      [added_label](const LabelId &indexed_label) { return indexed_label == added_label; });
      }
    }
  });
}

void DiskLabelIndex::UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update, const Transaction &tx) {
  if (IndexExists(removed_label)) {
    entries_for_deletion.WithLock([&removed_label, &tx, vertex_before_update](auto &tx_to_entries_for_deletion) {
      auto [it, _] = tx_to_entries_for_deletion.emplace(
          std::piecewise_construct, std::forward_as_tuple(tx.start_timestamp), std::forward_as_tuple());
      auto &vertex_map_store = it->second;
      auto [it_vertex_map_store, emplaced] = vertex_map_store.emplace(
          std::piecewise_construct, std::forward_as_tuple(vertex_before_update->gid), std::forward_as_tuple());
      it_vertex_map_store->second.emplace_back(removed_label);
      spdlog::debug("Added to the map label index storage for vertex {} with label {}",
                    utils::SerializeIdType(vertex_before_update->gid), utils::SerializeIdType(removed_label));
    });
  }
}

/// TODO: andi Here will come Bloom filter deletion
bool DiskLabelIndex::DropIndex(LabelId label) {
  if (!(index_.erase(label) > 0)) {
    return false;
  }
  auto disk_transaction = std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
  disk_transaction->SetReadTimestampForValidation(std::numeric_limits<uint64_t>::max());

  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it = std::unique_ptr<rocksdb::Iterator>(disk_transaction->GetIterator(ro));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string key = it->key().ToString();
    if (key.starts_with(utils::SerializeIdType(label))) {
      disk_transaction->Delete(it->key().ToString());
    }
  }

  disk_transaction->SetCommitTimestamp(0);
  auto status = disk_transaction->Commit();
  if (!status.ok()) {
    spdlog::error("rocksdb: {}", status.getState());
  }
  return status.ok();
}

bool DiskLabelIndex::IndexExists(LabelId label) const { return index_.find(label) != index_.end(); }

std::vector<LabelId> DiskLabelIndex::ListIndices() const { return {index_.begin(), index_.end()}; }

void DiskLabelIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp) {
  throw utils::NotYetImplemented("DiskLabelIndex::RemoveObsoleteEntries");
}

uint64_t DiskLabelIndex::ApproximateVertexCount(LabelId /*label*/) const {
  /// TODO: andi figure out something smarter.
  return 10;
}

/// TODO: delete everything
void DiskLabelIndex::Clear() {
  index_.clear();
  auto disk_transaction = std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
  disk_transaction->SetReadTimestampForValidation(std::numeric_limits<uint64_t>::max());

  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it = std::unique_ptr<rocksdb::Iterator>(disk_transaction->GetIterator(ro));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    disk_transaction->Delete(it->key().ToString());
  }

  disk_transaction->SetCommitTimestamp(0);
  auto status = disk_transaction->Commit();
  if (!status.ok()) {
    spdlog::error("rocksdb: {}", status.getState());
  }
}

void DiskLabelIndex::RunGC() {
  // throw utils::NotYetImplemented("DiskLabelIndex::RunGC");
}

}  // namespace memgraph::storage
