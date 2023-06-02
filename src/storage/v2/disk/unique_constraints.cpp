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

#include "storage/v2/disk/unique_constraints.hpp"
#include <limits>
#include <optional>
#include "storage/v2/constraints/unique_constraints.hpp"
#include "utils/file.hpp"
namespace memgraph::storage {

namespace {

bool IsVertexUnderConstraint(const Vertex &vertex, const LabelId &constraint_label,
                             const std::set<PropertyId> &constraint_properties) {
  return utils::Contains(vertex.labels, constraint_label) && vertex.properties.HasAllProperties(constraint_properties);
}

}  // namespace

/// TODO: using for constraint

DiskUniqueConstraints::DiskUniqueConstraints(const Config &config) {
  kvstore_ = std::make_unique<RocksDBStorage>();
  utils::EnsureDirOrDie(config.disk.unique_constraints_directory);
  kvstore_->options_.create_if_missing = true;
  kvstore_->options_.comparator = new ComparatorWithU64TsImpl();
  logging::AssertRocksDBStatus(rocksdb::TransactionDB::Open(kvstore_->options_, rocksdb::TransactionDBOptions(),
                                                            config.disk.unique_constraints_directory, &kvstore_->db_));
}

/// TODO: unit test for this method: it must perform check before constraint is created
bool DiskUniqueConstraints::InsertConstraint(
    LabelId label, const std::set<PropertyId> &properties,
    const std::vector<std::pair<std::string, std::string>> &vertices_under_constraint) {
  constraints_.insert(std::make_pair(label, properties));

  auto disk_transaction_ = std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
  for (const auto &[key, value] : vertices_under_constraint) {
    disk_transaction_->Put(key, value);
  }

  disk_transaction_->SetCommitTimestamp(0);
  auto status = disk_transaction_->Commit();
  if (!status.ok()) {
    spdlog::debug(status.getState());
  }
  return status.ok();
}

std::optional<ConstraintViolation> DiskUniqueConstraints::Validate(
    const Vertex &vertex, std::vector<std::vector<PropertyValue>> &unique_storage) const {
  for (const auto &[constraint_label, constraint_properties] : constraints_) {
    if (IsVertexUnderConstraint(vertex, constraint_label, constraint_properties)) {
      if (auto process_res =
              ProcessVertexUnderUniqueConstraintOnDisk(vertex, unique_storage, constraint_label, constraint_properties);
          process_res.has_value()) {
        return process_res.value();
      }
    }
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<ConstraintViolation> DiskUniqueConstraints::ProcessVertexUnderUniqueConstraintOnDisk(
    const Vertex &vertex, std::vector<std::vector<PropertyValue>> &unique_storage, const LabelId &constraint_label,
    const std::set<PropertyId> &constraint_properties) const {
  auto property_values = vertex.properties.ExtractPropertyValues(constraint_properties);
  if (!TestUniqueConstraintConditionOnDiskForVertex(property_values, unique_storage, constraint_label,
                                                    constraint_properties, vertex.gid)) {
    return ConstraintViolation{ConstraintViolation::Type::UNIQUE, constraint_label, constraint_properties};
  }
  unique_storage.emplace_back(std::move(*property_values));
  return std::nullopt;
}

[[nodiscard]] bool DiskUniqueConstraints::TestUniqueConstraintConditionOnDiskForVertex(
    const std::optional<std::vector<PropertyValue>> &property_values,
    const std::vector<std::vector<PropertyValue>> &unique_storage, const LabelId &constraint_label,
    const std::set<PropertyId> &constraint_properties, const Gid &gid) const {
  return property_values.has_value() &&
         !DifferentVertexExistsWithSameLabelAndPropertyValues(*property_values, unique_storage, constraint_label,
                                                              constraint_properties, gid);
}

/// TODO: andi finish the implementation. FOur parameters sent which is not great, refactor
bool DiskUniqueConstraints::DifferentVertexExistsWithSameLabelAndPropertyValues(
    const std::vector<PropertyValue> property_values, const std::vector<std::vector<PropertyValue>> &unique_storage,
    const LabelId &constraint_label, const std::set<PropertyId> &constraint_properties, const Gid &gid) const {
  /// TODO: function does too many things, refactor
  if (std::find(unique_storage.begin(), unique_storage.end(), property_values) != unique_storage.end()) {
    return true;
  }
  /// make_unique
  auto *disk_transaction = kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions());
  disk_transaction->SetReadTimestampForValidation(std::numeric_limits<uint64_t>::max());

  /// TODO: andi Make use of prefix search to speed up the process.
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it = std::unique_ptr<rocksdb::Iterator>(disk_transaction->GetIterator(ro));
  spdlog::debug("Checking whether there is different vertex from the one with gid: {}", utils::SerializeIdType(gid));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const std::string key = it->key().ToString();
    spdlog::debug("Found vertex with key: {} in unique constraints storage", key);
    const std::vector<std::string> vertex_parts = utils::Split(key, "|");
    if (std::string local_gid = vertex_parts[1]; local_gid == utils::SerializeIdType(gid)) {
      continue;
    }
    LabelId local_constraint_label = utils::DeserializeConstraintLabelFromUniqueConstraintStorage(key);
    if (local_constraint_label != constraint_label) {
      continue;
    }

    PropertyStore property_store = utils::DeserializePropertiesFromUniqueConstraintStorage(it->value().ToString());
    if (property_store.ExtractPropertyValues(constraint_properties) == property_values) {
      delete disk_transaction;
      return true;
    }
  }
  delete disk_transaction;
  return false;
}

bool DiskUniqueConstraints::ClearDeletedVertex(const std::string_view gid,
                                               uint64_t transaction_commit_timestamp) const {
  /// TODO: andi check whether we need to use transaction here and in CheckDifferentVertex.
  auto disk_transaction = std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
  disk_transaction->SetReadTimestampForValidation(std::numeric_limits<uint64_t>::max());

  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it = std::unique_ptr<rocksdb::Iterator>(disk_transaction->GetIterator(ro));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    spdlog::debug("Found vertex with key: {} when clearing deleted vertex with gid: {}", it->key().ToString(), gid);
    if (std::string key = it->key().ToString(); gid == utils::ExtractGidFromUniqueConstraintStorage(key)) {
      if (!disk_transaction->Delete(key).ok()) {
        return false;
      }
    }
  }
  disk_transaction->SetCommitTimestamp(transaction_commit_timestamp);
  /// TODO: improve
  return disk_transaction->Commit().ok();
}

void DiskUniqueConstraints::DeleteVerticesWithRemovedConstraintLabel(uint64_t transaction_start_timestamp,
                                                                     uint64_t transaction_commit_timestamp) {
  auto disk_transaction = std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
  disk_transaction->SetReadTimestampForValidation(std::numeric_limits<uint64_t>::max());

  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  entries_for_deletion.WithLock(
      [transaction_start_timestamp, disk_transaction_ptr = disk_transaction.get()](auto &tx_to_entries_for_deletion) {
        if (auto tx_it = tx_to_entries_for_deletion.find(transaction_start_timestamp);
            tx_it != tx_to_entries_for_deletion.end()) {
          for (const auto &[vertex_gid, constraints] : tx_it->second) {
            for (const auto &[constraint_label, constraint_properties] : constraints) {
              auto key_to_delete = utils::SerializeVertexAsKeyForUniqueConstraint(
                  constraint_label, constraint_properties, utils::SerializeIdType(vertex_gid));
              spdlog::debug("Deleted vertex from unique constraints storage with key: {}", key_to_delete);
              disk_transaction_ptr->Delete(key_to_delete);
            }
          }
          tx_to_entries_for_deletion.erase(tx_it);
        }
      });
  disk_transaction->SetCommitTimestamp(transaction_commit_timestamp);
  /// TODO: how to handle this failure
  if (auto status = disk_transaction->Commit(); !status.ok()) {
    spdlog::debug("Commit failed in clear entries method: {}", status.getState());
  }
}

[[maybe_unused]] bool DiskUniqueConstraints::SyncVertexToUniqueConstraintsStorage(const Vertex &vertex,
                                                                                  uint64_t commit_timestamp) const {
  auto disk_transaction = std::unique_ptr<rocksdb::Transaction>(
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions()));
  for (const auto &[constraint_label, constraint_properties] : constraints_) {
    /// TODO: do this check in a private method, it is to detailed here
    if (utils::Contains(vertex.labels, constraint_label) && vertex.properties.HasAllProperties(constraint_properties)) {
      /// TODO: andi This serialization metrhod accepts too many arguments, refactor it
      /// TODO: change to transaction db usage
      auto key = utils::SerializeVertexAsKeyForUniqueConstraint(constraint_label, constraint_properties,
                                                                utils::SerializeIdType(vertex.gid));
      auto value = utils::SerializeVertexAsValueForUniqueConstraint(constraint_label, vertex.labels, vertex.properties);
      auto status = disk_transaction->Put(key, value);

      std::stringstream ss;
      ss << "{";
      for (const auto &[property_id, property_value] : vertex.properties.Properties()) {
        ss << property_id.AsUint() << ": " << property_value;
      }
      ss << "}";

      if (!status.ok()) {
        return false;
      }
      spdlog::debug("Written vertex to unique constraints storage with key: {} and properties: {} in sync method", key,
                    ss.str());
    }
  }
  disk_transaction->SetCommitTimestamp(commit_timestamp);
  /// TODO: andi Handle this failure
  if (auto s = disk_transaction->Commit(); !s.ok()) {
    spdlog::debug("Commit failed in sync method... {}", s.getState());
  }
  return true;
}

DiskUniqueConstraints::CreationStatus DiskUniqueConstraints::CheckIfConstraintCanBeCreated(
    LabelId label, const std::set<PropertyId> &properties) const {
  if (properties.empty()) {
    return CreationStatus::EMPTY_PROPERTIES;
  }
  if (properties.size() > kUniqueConstraintsMaxProperties) {
    return CreationStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED;
  }
  if (constraints_.find(std::make_pair(label, properties)) != constraints_.end()) {
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
  return constraints_.find({label, properties}) != constraints_.end();
}

/// TODO: optimize by saving vertices instead of serializing them at the moment of label removal
void DiskUniqueConstraints::UpdateOnRemoveLabel(LabelId removed_label, const Vertex &vertex_before_update,
                                                uint64_t transaction_start_timestamp) {
  /// TODO: clean this code
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
  /// TODO: andi clean this code
  entries_for_deletion.WithLock(
      [transaction_start_timestamp, &vertex_before_update, added_label](auto &tx_to_entries_for_deletion) {
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
  return std::vector<std::pair<LabelId, std::set<PropertyId>>>(constraints_.begin(), constraints_.end());
}

/// TODO: andi. Clear RocksDB instance.
void DiskUniqueConstraints::Clear() { constraints_.clear(); }

}  // namespace memgraph::storage
