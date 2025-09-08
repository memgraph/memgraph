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

/// TODO: clear dependencies

#include "storage/v2/disk/label_property_index.hpp"
#include "utils/disk_utils.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/rocksdb_serialization.hpp"

namespace memgraph::storage {

namespace {

bool IsVertexIndexedByLabelProperty(const Vertex &vertex, LabelId label, PropertyId property) {
  return utils::Contains(vertex.labels, label) && vertex.properties.HasProperty(property);
}

[[nodiscard]] bool ClearTransactionEntriesWithRemovedIndexingLabel(
    rocksdb::Transaction &disk_transaction, const DiskLabelPropertyIndex::EntriesForDeletion &transaction_entries) {
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

  if (auto maybe_old_disk_key = utils::GetOldDiskKeyOrNull(vertex.delta); maybe_old_disk_key.has_value()) {
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
                                                                    uint64_t transaction_commit_timestamp,
                                                                    EntriesForDeletion const &entries_for_deletion) {
  if (entries_for_deletion.empty()) {
    return true;
  }
  auto disk_transaction = CreateAllReadingRocksDBTransaction();

  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  bool deletion_success = ClearTransactionEntriesWithRemovedIndexingLabel(*disk_transaction, entries_for_deletion);
  if (deletion_success) {
    return CommitWithTimestamp(disk_transaction.get(), transaction_commit_timestamp);
  }
  return false;
}

void DiskLabelPropertyIndex::ActiveIndices::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update,
                                                             const Transaction &tx) {
  auto vertex_label_index_it = entries_for_deletion_.find(vertex_after_update->gid);
  if (vertex_label_index_it == entries_for_deletion_.end()) {
    return;
  }
  std::erase_if(vertex_label_index_it->second,
                [added_label](const LabelProperty &index) { return index.label == added_label; });
}

void DiskLabelPropertyIndex::ActiveIndices::UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update,
                                                                const Transaction &tx) {
  for (const auto &entry : index_) {
    if (entry.label != removed_label) {
      continue;
    }
    auto [it_vertex_map_store, emplaced] = entries_for_deletion_.emplace(
        std::piecewise_construct, std::forward_as_tuple(vertex_after_update->gid), std::forward_as_tuple());
    it_vertex_map_store->second.emplace_back(entry);
  }
}

bool DiskLabelPropertyIndex::DropIndex(LabelId label, std::vector<PropertyPath> const &properties) {
  return index_.erase({label, properties[0][0]}) > 0U;
}

bool DiskLabelPropertyIndex::ActiveIndices::IndexExists(LabelId label, std::span<PropertyPath const> properties) const {
  return utils::Contains(index_, LabelProperty{label, properties[0][0]});
}

bool DiskLabelPropertyIndex::ActiveIndices::IndexReady(LabelId label, std::span<PropertyPath const> properties) const {
  return utils::Contains(index_, LabelProperty{label, properties[0][0]});
}

auto DiskLabelPropertyIndex::ActiveIndices::ListIndices(uint64_t start_timestamp) const
    -> std::vector<std::pair<LabelId, std::vector<PropertyPath>>> {
  auto const convert = [](auto &&index) -> std::pair<LabelId, std::vector<PropertyPath>> {
    auto [label, property] = index;
    return {label, {PropertyPath{property}}};
  };

  return index_ | ranges::views::transform(convert) | ranges::to_vector;
}

auto DiskLabelPropertyIndex::ActiveIndices::ApproximateVertexCount(LabelId /*label*/,
                                                                   std::span<PropertyPath const> /*properties*/) const
    -> uint64_t {
  return 10;
}

auto DiskLabelPropertyIndex::ActiveIndices::ApproximateVertexCount(LabelId /*label*/,
                                                                   std::span<PropertyPath const> /*properties*/,
                                                                   std::span<PropertyValue const> /*values*/) const
    -> uint64_t {
  return 10;
}

auto DiskLabelPropertyIndex::ActiveIndices::ApproximateVertexCount(LabelId label,
                                                                   std::span<PropertyPath const> /*properties*/,
                                                                   std::span<PropertyValueRange const> /*bounds*/) const
    -> uint64_t {
  return 10;
}

void DiskLabelPropertyIndex::LoadIndexInfo(const std::vector<std::string> &keys) {
  for (const auto &label_property : keys) {
    std::vector<std::string> label_property_split = utils::Split(label_property, ",");
    index_.emplace(LabelId::FromString(label_property_split[0]), PropertyId::FromString(label_property_split[1]));
  }
}

RocksDBStorage *DiskLabelPropertyIndex::GetRocksDBStorage() const { return kvstore_.get(); }

auto DiskLabelPropertyIndex::GetInfo() const -> std::set<DiskLabelPropertyIndex::LabelProperty> { return index_; }

auto DiskLabelPropertyIndex::GetActiveIndices() const -> std::unique_ptr<LabelPropertyIndex::ActiveIndices> {
  return std::make_unique<DiskLabelPropertyIndex::ActiveIndices>(index_);
}

auto DiskLabelPropertyIndex::ActiveIndices::RelevantLabelPropertiesIndicesInfo(
    std::span<LabelId const> labels, std::span<PropertyPath const> properties) const
    -> std::vector<LabelPropertiesIndicesInfo> {
  auto res = std::vector<LabelPropertiesIndicesInfo>{};
  // NOTE: only looking for singular property index, as disk does not support composite indices
  for (auto &&[l_pos, label] : ranges::views::enumerate(labels)) {
    for (auto [p_pos, property] : ranges::views::enumerate(properties)) {
      if (IndexReady(label, std::array{property})) {
        // NOLINTNEXTLINE(google-runtime-int)
        res.emplace_back(l_pos, std::vector{static_cast<long>(p_pos)}, label, std::vector{property});
      }
    }
  }
  return res;
}

void DiskLabelPropertyIndex::ActiveIndices::AbortEntries(AbortableInfo const &info, uint64_t start_timestamp) {}
LabelPropertyIndex::AbortProcessor DiskLabelPropertyIndex::ActiveIndices::GetAbortProcessor() const {
  return AbortProcessor();
}

}  // namespace memgraph::storage
