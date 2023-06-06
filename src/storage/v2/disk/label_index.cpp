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
  } else {
    uint64_t estimate_num_keys = 0;
    kvstore_->db_->GetIntProperty("rocksdb.estimate-num-keys", &estimate_num_keys);
    spdlog::debug("Creating index committed, approx size: {}", estimate_num_keys);
  }
  return status.ok();
}

std::unique_ptr<rocksdb::Transaction> DiskLabelIndex::CreateRocksDBTransaction() {
  uint64_t estimate_num_keys = 0;
  kvstore_->db_->GetIntProperty("rocksdb.estimate-num-keys", &estimate_num_keys);
  spdlog::debug("Approx size before reading indexed vertices: {}", estimate_num_keys);
  return std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
  // return std::unique_ptr<rocksdb::Iterator>(kvstore_->db_->NewIterator(rocksdb::ReadOptions()));
}

bool DiskLabelIndex::SyncVertexToLabelIndexStorage(const Vertex &vertex, uint64_t commit_timestamp) const {
  auto disk_transaction = std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
  for (const LabelId index_label : index_) {
    if (VertexHasLabel(vertex, index_label)) {
      /// TODO: andi, probably no need to transfer separately labels and gid
      disk_transaction->Put(utils::SerializeVertexForLabelIndex(index_label, vertex.labels, vertex.gid),
                            utils::SerializeProperties(vertex.properties));
    }
  }
  disk_transaction->SetCommitTimestamp(commit_timestamp);
  auto status = disk_transaction->Commit();
  if (!status.ok()) {
    spdlog::error("rocksdb: {}", status.getState());
  }
  return status.ok();
}

[[nodiscard]] bool DiskLabelIndex::ClearDeletedVertex(std::string_view gid,
                                                      uint64_t transaction_commit_timestamp) const {
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

/// TODO: andi if the vertex is already indexed, we should update the entry, not create a new one.
void DiskLabelIndex::UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx) {
  if (!IndexExists(label)) {
    return;
  }
  /// TODO: andi This change should be done at the commit time not before and under some lock, probably RocksDB lock.
  std::string key = utils::SerializeVertexForLabelIndex(label, vertex->labels, vertex->gid);
  std::string value = utils::SerializeProperties(vertex->properties);
  kvstore_->db_->Put(rocksdb::WriteOptions(), key, value);
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
