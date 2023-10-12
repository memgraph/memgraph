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

#include <charconv>
#include <string>
#include <vector>

#include "kvstore/kvstore.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/disk/durable_metadata.hpp"
#include "utils/file.hpp"
#include "utils/rocksdb_serialization.hpp"
#include "utils/string.hpp"

constexpr const char *lastTransactionStartTimeStamp = "last_transaction_start_timestamp";
constexpr const char *vertex_count_descr = "vertex_count";
constexpr const char *edge_count_descr = "edge_count";
constexpr const char *label_index_str = "label_index";
constexpr const char *label_property_index_str = "label_property_index";
constexpr const char *existence_constraints_str = "existence_constraints";
constexpr const char *unique_constraints_str = "unique_constraints";

namespace memgraph::storage {

DurableMetadata::DurableMetadata(const Config &config)
    : durability_kvstore_(kvstore::KVStore(config.disk.durability_directory)), config_(config) {
  MG_ASSERT(utils::DirExists(config_.disk.durability_directory),
            "Durability directory for saving disk metadata does not exist.");
}

DurableMetadata::DurableMetadata(DurableMetadata &&other) noexcept
    : durability_kvstore_(std::move(other.durability_kvstore_)), config_(std::move(other.config_)) {}

void DurableMetadata::SaveBeforeClosingDB(uint64_t timestamp, uint64_t vertex_count, uint64_t edge_count) {
  durability_kvstore_.Put(lastTransactionStartTimeStamp, std::to_string(timestamp));
  durability_kvstore_.Put(vertex_count_descr, std::to_string(vertex_count));
  durability_kvstore_.Put(edge_count_descr, std::to_string(edge_count));
}

std::optional<uint64_t> DurableMetadata::LoadTimestampIfExists() const {
  return LoadPropertyIfExists(lastTransactionStartTimeStamp);
}

std::optional<uint64_t> DurableMetadata::LoadVertexCountIfExists() const {
  return LoadPropertyIfExists(vertex_count_descr);
}

std::optional<uint64_t> DurableMetadata::LoadEdgeCountIfExists() const {
  return LoadPropertyIfExists(edge_count_descr);
}

std::optional<uint64_t> DurableMetadata::LoadPropertyIfExists(const char *property) const {
  if (auto count = durability_kvstore_.Get(property); count.has_value()) {
    auto last_count = count.value();
    uint64_t count_to_return{0U};
    std::from_chars(last_count.data(), last_count.data() + last_count.size(), count_to_return);
    return count_to_return;
  }
  return {};
}

std::optional<std::vector<std::string>> DurableMetadata::LoadLabelIndexInfoIfExists() const {
  return LoadInfoFromAuxiliaryStorages(label_index_str);
}

std::optional<std::vector<std::string>> DurableMetadata::LoadLabelPropertyIndexInfoIfExists() const {
  return LoadInfoFromAuxiliaryStorages(label_property_index_str);
}

std::optional<std::vector<std::string>> DurableMetadata::LoadExistenceConstraintInfoIfExists() const {
  return LoadInfoFromAuxiliaryStorages(existence_constraints_str);
}

std::optional<std::vector<std::string>> DurableMetadata::LoadUniqueConstraintInfoIfExists() const {
  return LoadInfoFromAuxiliaryStorages(unique_constraints_str);
}

std::optional<std::vector<std::string>> DurableMetadata::LoadInfoFromAuxiliaryStorages(const char *property) const {
  if (auto maybe_props = durability_kvstore_.Get(property); maybe_props.has_value()) {
    return utils::Split(maybe_props.value(), "|");
  }
  return {};
}

bool DurableMetadata::PersistLabelIndexCreation(LabelId label) {
  if (auto label_index_store = durability_kvstore_.Get(label_index_str); label_index_store.has_value()) {
    std::string &value = label_index_store.value();
    value += "|" + utils::SerializeIdType(label);
    return durability_kvstore_.Put(label_index_str, value);
  }
  return durability_kvstore_.Put(label_index_str, utils::SerializeIdType(label));
}

bool DurableMetadata::PersistLabelIndexDeletion(LabelId label) {
  if (auto label_index_store = durability_kvstore_.Get(label_index_str); label_index_store.has_value()) {
    const std::string &value = label_index_store.value();
    std::vector<std::string> labels = utils::Split(value, "|");
    std::erase(labels, utils::SerializeIdType(label));
    if (labels.empty()) {
      return durability_kvstore_.Delete(label_index_str);
    }
    return durability_kvstore_.Put(label_index_str, utils::Join(labels, "|"));
  }
  return true;
}

bool DurableMetadata::PersistLabelPropertyIndexAndExistenceConstraintCreation(LabelId label, PropertyId property,
                                                                              const char *key) {
  if (auto label_property_index_store = durability_kvstore_.Get(key); label_property_index_store.has_value()) {
    std::string &value = label_property_index_store.value();
    value += "|" + utils::SerializeIdType(label) + "," + utils::SerializeIdType(property);
    return durability_kvstore_.Put(key, value);
  }
  return durability_kvstore_.Put(key, utils::SerializeIdType(label) + "," + utils::SerializeIdType(property));
}

bool DurableMetadata::PersistLabelPropertyIndexAndExistenceConstraintDeletion(LabelId label, PropertyId property,
                                                                              const char *key) {
  if (auto label_property_index_store = durability_kvstore_.Get(key); label_property_index_store.has_value()) {
    const std::string &value = label_property_index_store.value();
    std::vector<std::string> label_properties = utils::Split(value, "|");
    std::erase(label_properties, utils::SerializeIdType(label) + "," + utils::SerializeIdType(property));
    if (label_properties.empty()) {
      return durability_kvstore_.Delete(key);
    }
    return durability_kvstore_.Put(key, utils::Join(label_properties, "|"));
  }
  return true;
}

bool DurableMetadata::PersistUniqueConstraintCreation(LabelId label, const std::set<PropertyId> &properties) {
  const std::string entry = utils::GetKeyForUniqueConstraintsDurability(label, properties);

  if (auto unique_store = durability_kvstore_.Get(unique_constraints_str); unique_store.has_value()) {
    std::string &value = unique_store.value();
    value += "|" + entry;
    return durability_kvstore_.Put(unique_constraints_str, value);
  }
  return durability_kvstore_.Put(unique_constraints_str, entry);
}

bool DurableMetadata::PersistUniqueConstraintDeletion(LabelId label, const std::set<PropertyId> &properties) {
  const std::string entry = utils::GetKeyForUniqueConstraintsDurability(label, properties);

  if (auto unique_store = durability_kvstore_.Get(unique_constraints_str); unique_store.has_value()) {
    const std::string &value = unique_store.value();
    std::vector<std::string> unique_constraints = utils::Split(value, "|");
    std::erase(unique_constraints, entry);
    if (unique_constraints.empty()) {
      return durability_kvstore_.Delete(unique_constraints_str);
    }
    return durability_kvstore_.Put(unique_constraints_str, utils::Join(unique_constraints, "|"));
  }
  return true;
}

}  // namespace memgraph::storage
