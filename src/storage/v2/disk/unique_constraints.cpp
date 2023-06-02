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
#include <rocksdb/options.h>
#include <rocksdb/utilities/transaction_db.h>
#include <cstdint>
#include <limits>
#include <sstream>
#include <tuple>
#include <utility>
#include "spdlog/spdlog.h"
#include "storage/v2/constraints/unique_constraints.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/algorithm.hpp"
#include "utils/file.hpp"
#include "utils/rocksdb_serialization.hpp"

namespace memgraph::storage {

/// TODO: if remove label is called multiple times on one vertex => unnecessary writes to disk

DiskUniqueConstraints::DiskUniqueConstraints(const Config &config) {
  kvstore_ = std::make_unique<RocksDBStorage>();
  utils::EnsureDirOrDie(config.disk.unique_constraints_directory);
  kvstore_->options_.create_if_missing = true;
  kvstore_->options_.comparator = new ComparatorWithU64TsImpl();
  logging::AssertRocksDBStatus(rocksdb::TransactionDB::Open(kvstore_->options_, rocksdb::TransactionDBOptions(),
                                                            config.disk.unique_constraints_directory, &kvstore_->db_));
}

/// TODO: andi Timestamp yes/no?
/// TODO: unit test for this method: it must perform check before constraint is created
void DiskUniqueConstraints::InsertConstraint(
    LabelId label, const std::set<PropertyId> &properties,
    const std::vector<std::pair<std::string, std::string>> &vertices_under_constraint,
    uint64_t transaction_start_timestamp) {
  constraints_.insert(std::make_pair(label, properties));
  /// TODO: andi Unique_ptr instead of raw
  rocksdb::Transaction *disk_transaction_ =
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions());
  /// TODO: foreach
  for (const auto &[key, value] : vertices_under_constraint) {
    spdlog::debug("Written vertex to unique constraints storage with key: {} in insert constraint method", key);
    disk_transaction_->Put(key, value);
  }
  /// TODO: how to better handle this?
  if (!disk_transaction_->Commit().ok()) {
    spdlog::debug("Commit failed in insert constraint...");
  }
  delete disk_transaction_;
}

std::optional<ConstraintViolation> DiskUniqueConstraints::Validate(
    const Vertex &vertex, std::vector<std::vector<PropertyValue>> &unique_storage,
    uint64_t transaction_start_timestamp) const {
  for (const auto &[constraint_label, constraint_property_ids] : constraints_) {
    /// TODO: andi Make use of prefix search to speed up the process.
    /// TODO: ugly check ,refactor this to make it more readable
    if (utils::Contains(vertex.labels, constraint_label) &&
        vertex.properties.HasAllProperties(constraint_property_ids)) {
      if (auto property_values = vertex.properties.ExtractPropertyValues(constraint_property_ids);
          property_values.has_value() &&
          DifferentVertexExistsWithSameLabelAndPropertyValues(*property_values, unique_storage, constraint_label,
                                                              vertex.gid, transaction_start_timestamp)) {
        return ConstraintViolation{ConstraintViolation::Type::UNIQUE, constraint_label, constraint_property_ids};
      } else {
        unique_storage.emplace_back(std::move(*property_values));
      }
    }
  }
  return std::nullopt;
}

void DiskUniqueConstraints::ClearEntriesScheduledForDeletion(uint64_t transaction_start_timestamp,
                                                             uint64_t transaction_commit_timestamp) {
  /// TODO: andi Unique_ptr instead of raw
  rocksdb::Transaction *disk_transaction =
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions());
  disk_transaction->SetReadTimestampForValidation(std::numeric_limits<uint64_t>::max());

  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  entries_for_deletion.WithLock([transaction_start_timestamp, disk_transaction](auto &tx_to_entries_for_deletion) {
    if (auto tx_it = tx_to_entries_for_deletion.find(transaction_start_timestamp);
        tx_it != tx_to_entries_for_deletion.end()) {
      for (const auto &[vertex_gid, constraints] : tx_it->second) {
        for (const auto &[constraint_label, constraint_properties] : constraints) {
          auto key_to_delete = utils::SerializeVertexAsKeyForUniqueConstraint(constraint_label, constraint_properties,
                                                                              utils::SerializeIdType(vertex_gid));
          spdlog::debug("Deleted vertex from unique constraints storage with key: {}", key_to_delete);
          disk_transaction->Delete(key_to_delete);
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
  delete disk_transaction;
}

[[maybe_unused]] bool DiskUniqueConstraints::SyncVertexToUniqueConstraintsStorage(const Vertex &vertex,
                                                                                  uint64_t commit_timestamp) const {
  rocksdb::Transaction *disk_transaction =
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions());
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
        delete disk_transaction;
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
  delete disk_transaction;
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

/// TODO: andi finish the implementation. FOur parameters sent which is not great, refactor
bool DiskUniqueConstraints::DifferentVertexExistsWithSameLabelAndPropertyValues(
    const std::vector<PropertyValue> property_values, const std::vector<std::vector<PropertyValue>> &unique_storage,
    const LabelId &constraint_label, const Gid &gid, uint64_t transaction_start_timestamp) const {
  /// TODO: function does too many things, refactor
  if (std::find(unique_storage.begin(), unique_storage.end(), property_values) != unique_storage.end()) {
    return true;
  }
  /// make_unique
  rocksdb::Transaction *disk_transaction =
      kvstore_->db_->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions());
  disk_transaction->SetReadTimestampForValidation(std::numeric_limits<uint64_t>::max());

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
    if (property_store.HasAllPropertyValues(property_values)) {
      delete disk_transaction;
      return true;
    }
  }
  delete disk_transaction;
  return false;
}

}  // namespace memgraph::storage
