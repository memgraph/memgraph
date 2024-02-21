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

#include "storage/v2/disk/unique_constraints.hpp"
#include <rocksdb/utilities/transaction.h>
#include <limits>
#include <optional>
#include <tuple>
#include "spdlog/spdlog.h"
#include "storage/v2/constraints/unique_constraints.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/algorithm.hpp"
#include "utils/disk_utils.hpp"
#include "utils/file.hpp"
namespace memgraph::storage {

namespace {

bool IsVertexUnderConstraint(const Vertex &vertex, const LabelId &constraint_label,
                             const std::set<PropertyId> &constraint_properties) {
  return utils::Contains(vertex.labels, constraint_label) && vertex.properties.HasAllProperties(constraint_properties);
}

bool IsDifferentVertexWithSameConstraintLabel(const std::string &key, const Gid gid, const LabelId constraint_label) {
  const std::vector<std::string> vertex_parts = utils::Split(key, "|");
  if (std::string local_gid = vertex_parts[1]; local_gid == gid.ToString()) {
    return false;
  }
  return utils::DeserializeConstraintLabelFromUniqueConstraintStorage(key) == constraint_label;
}

[[nodiscard]] bool ClearTransactionEntriesWithRemovedConstraintLabel(
    rocksdb::Transaction &disk_transaction,
    const std::map<Gid, std::set<std::pair<LabelId, std::set<PropertyId>>>> &transaction_entries) {
  for (const auto &[vertex_gid, constraints] : transaction_entries) {
    for (const auto &[constraint_label, constraint_properties] : constraints) {
      auto key_to_delete = utils::SerializeVertexAsKeyForUniqueConstraint(constraint_label, constraint_properties,
                                                                          vertex_gid.ToString());
      if (auto status = disk_transaction.Delete(key_to_delete); !status.ok()) {
        return false;
      }
    }
  }
  return true;
}

}  // namespace

DiskUniqueConstraints::DiskUniqueConstraints(const Config &config) {
  kvstore_ = std::make_unique<RocksDBStorage>();
  utils::EnsureDirOrDie(config.disk.unique_constraints_directory);
  kvstore_->options_.create_if_missing = true;
  kvstore_->options_.comparator = new ComparatorWithU64TsImpl();
  logging::AssertRocksDBStatus(rocksdb::TransactionDB::Open(kvstore_->options_, rocksdb::TransactionDBOptions(),
                                                            config.disk.unique_constraints_directory, &kvstore_->db_));
}

bool DiskUniqueConstraints::InsertConstraint(
    LabelId label, const std::set<PropertyId> &properties,
    const std::vector<std::pair<std::string, std::string>> &vertices_under_constraint) {
  if (!constraints_.insert(std::make_pair(label, properties)).second) {
    return false;
  }

  auto disk_transaction = std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
  for (const auto &[key, value] : vertices_under_constraint) {
    disk_transaction->Put(key, value);
  }

  /// TODO: figure out a better way to handle this
  disk_transaction->SetCommitTimestamp(0);
  /// TODO: how about extracting to commit
  auto status = disk_transaction->Commit();
  if (!status.ok()) {
    spdlog::error("rocksdb: {}", status.getState());
  }
  return status.ok();
}

std::optional<ConstraintViolation> DiskUniqueConstraints::Validate(
    const Vertex &vertex, std::vector<std::vector<PropertyValue>> &unique_storage) const {
  for (const auto &[constraint_label, constraint_properties] : constraints_) {
    if (IsVertexUnderConstraint(vertex, constraint_label, constraint_properties)) {
      if (auto vertex_check_result =
              TestIfVertexSatisifiesUniqueConstraint(vertex, unique_storage, constraint_label, constraint_properties);
          vertex_check_result.has_value()) {
        return vertex_check_result.value();
      }
    }
  }
  return std::nullopt;
}

std::optional<ConstraintViolation> DiskUniqueConstraints::TestIfVertexSatisifiesUniqueConstraint(
    const Vertex &vertex, std::vector<std::vector<PropertyValue>> &unique_storage, const LabelId &constraint_label,
    const std::set<PropertyId> &constraint_properties) const {
  auto property_values = vertex.properties.ExtractPropertyValues(constraint_properties);

  /// TODO: better naming. Is vertex unique
  if (property_values.has_value() &&
      VertexIsUnique(property_values.value(), unique_storage, constraint_label, constraint_properties, vertex.gid)) {
    unique_storage.emplace_back(std::move(property_values.value()));
    return std::nullopt;
  }

  return ConstraintViolation{ConstraintViolation::Type::UNIQUE, constraint_label, constraint_properties};
}

bool DiskUniqueConstraints::VertexIsUnique(const std::vector<PropertyValue> &property_values,
                                           const std::vector<std::vector<PropertyValue>> &unique_storage,
                                           const LabelId &constraint_label,
                                           const std::set<PropertyId> &constraint_properties, const Gid gid) const {
  if (utils::Contains(unique_storage, property_values)) {
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
    if (IsDifferentVertexWithSameConstraintLabel(it->key().ToString(), gid, constraint_label)) {
      if (utils::DeserializePropertiesFromUniqueConstraintStorage(it->value().ToString())
              .ExtractPropertyValues(constraint_properties) == property_values) {
        return false;
      }
    }
  }
  return true;
}

bool DiskUniqueConstraints::ClearDeletedVertex(const std::string_view gid,
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
    if (std::string key = it->key().ToString(); gid == utils::ExtractGidFromUniqueConstraintStorage(key)) {
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

bool DiskUniqueConstraints::DeleteVerticesWithRemovedConstraintLabel(uint64_t transaction_start_timestamp,
                                                                     uint64_t transaction_commit_timestamp) {
  if (entries_for_deletion->empty()) {
    return true;
  }

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
      deletion_success = ClearTransactionEntriesWithRemovedConstraintLabel(*disk_transaction_ptr, tx_it->second);
      tx_to_entries_for_deletion.erase(tx_it);
    }
  });
  if (deletion_success) {
    /// TODO: Extract to some useful method
    disk_transaction->SetCommitTimestamp(transaction_commit_timestamp);
    auto status = disk_transaction->Commit();
    if (!status.ok()) {
      spdlog::error("rocksdb: {}", status.getState());
    }
    return status.ok();
  }
  spdlog::error("Deletion of vertices with removed constraint label failed.");
  return false;
}

bool DiskUniqueConstraints::SyncVertexToUniqueConstraintsStorage(const Vertex &vertex,
                                                                 uint64_t commit_timestamp) const {
  /// TODO: create method for writing transaction
  auto disk_transaction = std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));

  if (auto maybe_old_disk_key = utils::GetOldDiskKeyOrNull(vertex.delta()); maybe_old_disk_key.has_value()) {
    spdlog::trace("Found old disk key {} for vertex {}", maybe_old_disk_key.value(), vertex.gid.ToString());
    if (auto status = disk_transaction->Delete(maybe_old_disk_key.value()); !status.ok()) {
      return false;
    }
  }

  for (const auto &[constraint_label, constraint_properties] : constraints_) {
    if (IsVertexUnderConstraint(vertex, constraint_label, constraint_properties)) {
      auto key = utils::SerializeVertexAsKeyForUniqueConstraint(constraint_label, constraint_properties,
                                                                vertex.gid.ToString());
      auto value = utils::SerializeVertexAsValueForUniqueConstraint(constraint_label, vertex.labels, vertex.properties);
      if (!disk_transaction->Put(key, value).ok()) {
        return false;
      }
    }
  }
  /// TODO: extract and better message
  disk_transaction->SetCommitTimestamp(commit_timestamp);
  auto status = disk_transaction->Commit();
  if (!status.ok()) {
    spdlog::error("rocksdb: {}", status.getState());
  }
  return status.ok();
}

DiskUniqueConstraints::CreationStatus DiskUniqueConstraints::CheckIfConstraintCanBeCreated(
    LabelId label, const std::set<PropertyId> &properties) const {
  if (properties.empty()) {
    return CreationStatus::EMPTY_PROPERTIES;
  }
  if (properties.size() > kUniqueConstraintsMaxProperties) {
    return CreationStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED;
  }
  if (utils::Contains(constraints_, std::make_pair(label, properties))) {
    return CreationStatus::ALREADY_EXISTS;
  }
  return CreationStatus::SUCCESS;
};

DiskUniqueConstraints::DeletionStatus DiskUniqueConstraints::DropConstraint(LabelId label,
                                                                            const std::set<PropertyId> &properties) {
  if (auto drop_properties_check_result = UniqueConstraints::CheckPropertiesBeforeDeletion(properties);
      drop_properties_check_result != UniqueConstraints::DeletionStatus::SUCCESS) {
    return drop_properties_check_result;
  }
  if (constraints_.erase({label, properties}) > 0) {
    return UniqueConstraints::DeletionStatus::SUCCESS;
  }
  return UniqueConstraints::DeletionStatus::NOT_FOUND;
}

bool DiskUniqueConstraints::ConstraintExists(LabelId label, const std::set<PropertyId> &properties) const {
  return utils::Contains(constraints_, std::make_pair(label, properties));
}

void DiskUniqueConstraints::UpdateOnRemoveLabel(LabelId removed_label, const Vertex &vertex_before_update,
                                                uint64_t transaction_start_timestamp) {
  for (const auto &constraint : constraints_) {
    if (constraint.first == removed_label) {
      entries_for_deletion.WithLock(
          [&constraint, transaction_start_timestamp, &vertex_before_update](auto &tx_to_entries_for_deletion) {
            const auto &[constraint_label, constraint_properties] = constraint;
            auto [it, _] = tx_to_entries_for_deletion.emplace(
                std::piecewise_construct, std::forward_as_tuple(transaction_start_timestamp), std::forward_as_tuple());
            auto &vertex_map_store = it->second;
            auto [it_vertex_map_store, emplaced] = vertex_map_store.emplace(
                std::piecewise_construct, std::forward_as_tuple(vertex_before_update.gid), std::forward_as_tuple());
            it_vertex_map_store->second.emplace(constraint_label, constraint_properties);
          });
    }
  }
}

void DiskUniqueConstraints::UpdateOnAddLabel(LabelId added_label, const Vertex &vertex_before_update,
                                             uint64_t transaction_start_timestamp) {
  entries_for_deletion.WithLock(
      [transaction_start_timestamp, &vertex_before_update, added_label](auto &tx_to_entries_for_deletion) {
        /// TODO: change to only one if condition and maybe optimize erase if
        if (auto tx_it = tx_to_entries_for_deletion.find(transaction_start_timestamp);
            tx_it != tx_to_entries_for_deletion.end()) {
          if (auto vertex_constraints_it = tx_it->second.find(vertex_before_update.gid);
              vertex_constraints_it != tx_it->second.end()) {
            std::erase_if(vertex_constraints_it->second,
                          [added_label](const auto &constraint) { return constraint.first == added_label; });
          }
        }
      });
}

std::vector<std::pair<LabelId, std::set<PropertyId>>> DiskUniqueConstraints::ListConstraints() const {
  return {constraints_.begin(), constraints_.end()};
}

void DiskUniqueConstraints::Clear() {
  constraints_.clear();

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

RocksDBStorage *DiskUniqueConstraints::GetRocksDBStorage() const { return kvstore_.get(); }

void DiskUniqueConstraints::LoadUniqueConstraints(const std::vector<std::string> &keys) {
  for (const auto &key : keys) {
    std::vector<std::string> key_parts = utils::Split(key, ",");
    LabelId label = LabelId::FromString(key_parts[0]);
    std::set<PropertyId> properties;
    for (int i = 1; i < key_parts.size(); i++) {
      properties.insert(PropertyId::FromString(key_parts[i]));
    }
    constraints_.emplace(label, properties);
  }
}
bool DiskUniqueConstraints::empty() const { return constraints_.empty(); }

}  // namespace memgraph::storage
