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
#include "storage/v2/constraints/unique_constraints.hpp"
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

/// TODO: andi Timestamp yes/no?
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

std::optional<ConstraintViolation> DiskUniqueConstraints::Validate(
    const Vertex &vertex, std::vector<std::vector<PropertyValue>> &unique_storage) const {
  for (const auto &[label, properties] : constraints_) {
    /// TODO: andi Make use of prefix search to speed up the process.
    /// TODO: ugly check ,refactor this to make it more readable
    if (utils::Contains(vertex.labels, label) && vertex.properties.HasAllProperties(properties)) {
      if (auto property_values = vertex.properties.ExtractPropertyValues(properties);
          property_values.has_value() &&
          DifferentVertexExistsWithPropertyValues(*property_values, unique_storage, vertex.gid)) {
        return ConstraintViolation{ConstraintViolation::Type::UNIQUE, label, properties};
      } else {
        unique_storage.emplace_back(std::move(*property_values));
      }
    }
  }
  return std::nullopt;
}

std::vector<std::pair<LabelId, std::set<PropertyId>>> DiskUniqueConstraints::ListConstraints() const {
  return std::vector<std::pair<LabelId, std::set<PropertyId>>>(constraints_.begin(), constraints_.end());
}

/// TODO: andi. Clear RocksDB instance.
void DiskUniqueConstraints::Clear() { constraints_.clear(); }

/// TODO: andi finish the implementation. Three parameters sent which is not great, refactor
bool DiskUniqueConstraints::DifferentVertexExistsWithPropertyValues(
    const std::vector<PropertyValue> property_values, const std::vector<std::vector<PropertyValue>> &unique_storage,
    const Gid &gid) const {
  /// TODO: function does too many things, refactor
  if (std::find(unique_storage.begin(), unique_storage.end(), property_values) != unique_storage.end()) {
    return true;
  }
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it = std::unique_ptr<rocksdb::Iterator>(kvstore_->db_->NewIterator(ro, kvstore_->vertex_chandle));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const std::vector<std::string> vertex_parts = utils::Split(it->key().ToStringView(), "|");
    if (std::string local_gid = vertex_parts[1]; local_gid == utils::SerializeIdType(gid)) {
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
