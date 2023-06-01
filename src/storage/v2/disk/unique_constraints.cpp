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
#include <sstream>
#include "storage/v2/constraints/unique_constraints.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/algorithm.hpp"
#include "utils/file.hpp"
#include "utils/rocksdb_serialization.hpp"

namespace memgraph::storage {

DiskUniqueConstraints::DiskUniqueConstraints(const Config &config) {
  kvstore_ = std::make_unique<RocksDBStorage>();
  utils::EnsureDirOrDie(config.disk.unique_constraints_directory);
  kvstore_->options_.create_if_missing = true;
  kvstore_->options_.comparator = new ComparatorWithU64TsImpl();
  logging::AssertRocksDBStatus(rocksdb::TransactionDB::Open(kvstore_->options_, rocksdb::TransactionDBOptions(),
                                                            config.disk.unique_constraints_directory, &kvstore_->db_));
}

/// TODO: andi Timestamp yes/no? and transaction support
void DiskUniqueConstraints::InsertConstraint(
    LabelId label, const std::set<PropertyId> &properties,
    const std::vector<std::pair<std::string, std::string>> &vertices_under_constraint) {
  constraints_.insert(std::make_pair(label, properties));
  rocksdb::WriteOptions wo;
  /// TODO: foreach
  for (const auto &[key, value] : vertices_under_constraint) {
    kvstore_->db_->Put(wo, key, value);
  }
}

std::optional<ConstraintViolation> DiskUniqueConstraints::Validate(
    const Vertex &vertex, std::vector<std::vector<PropertyValue>> &unique_storage) const {
  for (const auto &[constraint_label, constraint_property_ids] : constraints_) {
    /// TODO: andi Make use of prefix search to speed up the process.
    /// TODO: ugly check ,refactor this to make it more readable
    if (utils::Contains(vertex.labels, constraint_label) &&
        vertex.properties.HasAllProperties(constraint_property_ids)) {
      if (auto property_values = vertex.properties.ExtractPropertyValues(constraint_property_ids);
          property_values.has_value() && DifferentVertexExistsWithSameLabelAndPropertyValues(
                                             *property_values, unique_storage, constraint_label, vertex.gid)) {
        return ConstraintViolation{ConstraintViolation::Type::UNIQUE, constraint_label, constraint_property_ids};
      } else {
        unique_storage.emplace_back(std::move(*property_values));
      }
    }
  }
  return std::nullopt;
}

void DiskUniqueConstraints::ClearEntriesScheduledForDeletion(uint64_t transaction_id) {
  entries_for_deletion.WithLock([transaction_id, this](auto &tx_to_entries_for_deletion) {
    if (auto it = tx_to_entries_for_deletion.find(transaction_id); it != tx_to_entries_for_deletion.end()) {
      for (const auto &key : it->second) {
        /// TODO: andi change this to transaction db usage
        kvstore_->db_->Delete(rocksdb::WriteOptions(), key);
      }
      tx_to_entries_for_deletion.erase(it);
    }
  });
}

[[maybe_unused]] bool DiskUniqueConstraints::SyncVertexToUniqueConstraintsStorage(const Vertex &vertex) const {
  /// TODO: replace by using for each construct
  for (const auto &[constraint_label, constraint_properties] : constraints_) {
    /// TODO: do this check in a private method, it is to detailed here
    if (utils::Contains(vertex.labels, constraint_label) && vertex.properties.HasAllProperties(constraint_properties)) {
      /// TODO: andi This serialization metrhod accepts too many arguments, refactor it
      /// TODO: change to transaction db usage
      auto key = utils::SerializeVertexAsKeyForUniqueConstraint(constraint_label, constraint_properties,
                                                                utils::SerializeIdType(vertex.gid));
      auto value = utils::SerializeVertexAsValueForUniqueConstraint(constraint_label, vertex.labels, vertex.properties);
      auto status = kvstore_->db_->Put(rocksdb::WriteOptions(), key, value);

      std::stringstream ss;
      ss << "{";
      for (const auto &[property_id, property_value] : vertex.properties.Properties()) {
        ss << property_id.AsUint() << ": " << property_value;
      }
      ss << "}";
      spdlog::debug("Written vertex to unique constraints storage with key: {} and properties: {}", key, ss.str());

      if (!status.ok()) {
        return false;
      }
    }
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
  for (const auto &constraint : constraints_) {
    if (constraint.first == removed_label) {
      entries_for_deletion.WithLock(
          [&constraint, transaction_start_timestamp, &vertex_before_update](auto &tx_to_entries_for_deletion) {
            const auto &[constraint_label, constraint_properties] = constraint;
            if (auto it = tx_to_entries_for_deletion.find(transaction_start_timestamp);
                it == tx_to_entries_for_deletion.end()) {
              tx_to_entries_for_deletion.emplace(transaction_start_timestamp, std::vector<std::string>());
              it->second.emplace_back(utils::SerializeVertexAsKeyForUniqueConstraint(
                  constraint_label, constraint_properties, utils::SerializeIdType(vertex_before_update.gid)));
            } else {
              it->second.emplace_back(utils::SerializeVertexAsKeyForUniqueConstraint(
                  constraint_label, constraint_properties, utils::SerializeIdType(vertex_before_update.gid)));
            }
          });
    }
  }
}

std::vector<std::pair<LabelId, std::set<PropertyId>>> DiskUniqueConstraints::ListConstraints() const {
  return std::vector<std::pair<LabelId, std::set<PropertyId>>>(constraints_.begin(), constraints_.end());
}

/// TODO: andi. Clear RocksDB instance.
void DiskUniqueConstraints::Clear() { constraints_.clear(); }

/// TODO: andi finish the implementation. FOur parameters sent which is not great, refactor
bool DiskUniqueConstraints::DifferentVertexExistsWithSameLabelAndPropertyValues(
    const std::vector<PropertyValue> property_values, const std::vector<std::vector<PropertyValue>> &unique_storage,
    const LabelId &constraint_label, const Gid &gid) const {
  /// TODO: function does too many things, refactor
  if (std::find(unique_storage.begin(), unique_storage.end(), property_values) != unique_storage.end()) {
    return true;
  }
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it = std::unique_ptr<rocksdb::Iterator>(kvstore_->db_->NewIterator(ro));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const std::string key = it->key().ToString();
    const std::vector<std::string> vertex_parts = utils::Split(key, "|");
    if (std::string local_gid = vertex_parts[1]; local_gid == utils::SerializeIdType(gid)) {
      continue;
    }
    LabelId local_constraint_label = utils::DeserializeConstraintLabelFromUniqueConstraintStorage(key);
    if (local_constraint_label != constraint_label) {
      continue;
    }

    PropertyStore property_store;
    property_store.SetBuffer(it->value().ToStringView());
    if (property_store.HasAllPropertyValues(property_values)) {
      return true;
    }
  }
  return false;
}

}  // namespace memgraph::storage
