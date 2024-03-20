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

#include <charconv>
#include <string>
#include <vector>

#include "kvstore/kvstore.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/disk/durable_metadata.hpp"
#include "utils/file.hpp"
#include "utils/rocksdb_serialization.hpp"
#include "utils/string.hpp"

namespace {
constexpr const char *kLastTransactionStartTimeStamp = "last_transaction_start_timestamp";
constexpr const char *kVertexCountDescr = "vertex_count";
constexpr const char *kEdgeDountDescr = "edge_count";
constexpr const char *kLabelIndexStr = "label_index";
constexpr const char *kLabelPropertyIndexStr = "label_property_index";
constexpr const char *kTextIndexStr = "text_index";
constexpr const char *kExistenceConstraintsStr = "existence_constraints";
constexpr const char *kUniqueConstraintsStr = "unique_constraints";
}  // namespace

namespace memgraph::storage {

DurableMetadata::DurableMetadata(const Config &config)
    : durability_kvstore_(kvstore::KVStore(config.disk.durability_directory)), config_(config) {
  MG_ASSERT(utils::DirExists(config_.disk.durability_directory),
            "Durability directory for saving disk metadata does not exist.");
}

DurableMetadata::DurableMetadata(DurableMetadata &&other) noexcept
    : durability_kvstore_(std::move(other.durability_kvstore_)), config_(std::move(other.config_)) {}

void DurableMetadata::SaveBeforeClosingDB(uint64_t timestamp, uint64_t vertex_count, uint64_t edge_count) {
  durability_kvstore_.Put(kLastTransactionStartTimeStamp, std::to_string(timestamp));
  durability_kvstore_.Put(kVertexCountDescr, std::to_string(vertex_count));
  durability_kvstore_.Put(kEdgeDountDescr, std::to_string(edge_count));
}

std::optional<uint64_t> DurableMetadata::LoadTimestampIfExists() const {
  return LoadPropertyIfExists(kLastTransactionStartTimeStamp);
}

std::optional<uint64_t> DurableMetadata::LoadVertexCountIfExists() const {
  return LoadPropertyIfExists(kVertexCountDescr);
}

std::optional<uint64_t> DurableMetadata::LoadEdgeCountIfExists() const { return LoadPropertyIfExists(kEdgeDountDescr); }

std::optional<uint64_t> DurableMetadata::LoadPropertyIfExists(const std::string &property) const {
  if (auto count = durability_kvstore_.Get(property); count.has_value()) {
    auto last_count = count.value();
    uint64_t count_to_return{0U};
    if (std::from_chars(last_count.data(), last_count.data() + last_count.size(), count_to_return).ec == std::errc()) {
      return count_to_return;
    }
  }
  return {};
}

std::optional<std::vector<std::string>> DurableMetadata::LoadLabelIndexInfoIfExists() const {
  return LoadInfoFromAuxiliaryStorages(kLabelIndexStr);
}

std::optional<std::vector<std::string>> DurableMetadata::LoadLabelPropertyIndexInfoIfExists() const {
  return LoadInfoFromAuxiliaryStorages(kLabelPropertyIndexStr);
}

std::optional<std::vector<std::string>> DurableMetadata::LoadExistenceConstraintInfoIfExists() const {
  return LoadInfoFromAuxiliaryStorages(kExistenceConstraintsStr);
}

std::optional<std::vector<std::string>> DurableMetadata::LoadUniqueConstraintInfoIfExists() const {
  return LoadInfoFromAuxiliaryStorages(kUniqueConstraintsStr);
}

std::optional<std::vector<std::string>> DurableMetadata::LoadInfoFromAuxiliaryStorages(
    const std::string &property) const {
  if (auto maybe_props = durability_kvstore_.Get(property); maybe_props.has_value()) {
    return utils::Split(maybe_props.value(), "|");
  }
  return {};
}

bool DurableMetadata::PersistLabelIndexCreation(LabelId label) {
  const auto serialized_label = label.ToString();
  if (auto label_index_store = durability_kvstore_.Get(kLabelIndexStr); label_index_store.has_value()) {
    std::string &value = label_index_store.value();
    value += "|";
    value += serialized_label;
    return durability_kvstore_.Put(kLabelIndexStr, value);
  }
  return durability_kvstore_.Put(kLabelIndexStr, serialized_label);
}

bool DurableMetadata::PersistLabelIndexDeletion(LabelId label) {
  const auto serialized_label = label.ToString();
  if (auto label_index_store = durability_kvstore_.Get(kLabelIndexStr); label_index_store.has_value()) {
    const std::string &value = label_index_store.value();
    std::vector<std::string> labels = utils::Split(value, "|");
    std::erase(labels, serialized_label);
    if (labels.empty()) {
      return durability_kvstore_.Delete(kLabelIndexStr);
    }
    return durability_kvstore_.Put(kLabelIndexStr, utils::Join(labels, "|"));
  }
  return true;
}

bool DurableMetadata::PersistLabelPropertyIndexAndExistenceConstraintCreation(LabelId label, PropertyId property,
                                                                              const std::string &key) {
  const std::string label_property_pair = label.ToString() + "," + property.ToString();
  if (auto label_property_index_store = durability_kvstore_.Get(key); label_property_index_store.has_value()) {
    std::string &value = label_property_index_store.value();
    value += "|";
    value += label_property_pair;
    return durability_kvstore_.Put(key, value);
  }
  return durability_kvstore_.Put(key, label_property_pair);
}

bool DurableMetadata::PersistLabelPropertyIndexAndExistenceConstraintDeletion(LabelId label, PropertyId property,
                                                                              const std::string &key) {
  const std::string label_property_pair = label.ToString() + "," + property.ToString();
  if (auto label_property_index_store = durability_kvstore_.Get(key); label_property_index_store.has_value()) {
    const std::string &value = label_property_index_store.value();
    std::vector<std::string> label_properties = utils::Split(value, "|");
    std::erase(label_properties, label_property_pair);
    if (label_properties.empty()) {
      return durability_kvstore_.Delete(key);
    }
    return durability_kvstore_.Put(key, utils::Join(label_properties, "|"));
  }
  return true;
}

bool DurableMetadata::PersistTextIndexCreation(const std::string &index_name, LabelId label) {
  const std::string index_name_label_pair = index_name + "," + label.ToString();
  if (auto text_index_store = durability_kvstore_.Get(kTextIndexStr); text_index_store.has_value()) {
    std::string &value = text_index_store.value();
    value += "|";
    value += index_name_label_pair;
    return durability_kvstore_.Put(kTextIndexStr, value);
  }
  return durability_kvstore_.Put(kTextIndexStr, index_name_label_pair);
}

bool DurableMetadata::PersistTextIndexDeletion(const std::string &index_name, LabelId label) {
  const std::string index_name_label_pair = index_name + "," + label.ToString();
  if (auto text_index_store = durability_kvstore_.Get(kTextIndexStr); text_index_store.has_value()) {
    const std::string &value = text_index_store.value();
    std::vector<std::string> text_indices = utils::Split(value, "|");
    std::erase(text_indices, index_name_label_pair);
    if (text_indices.empty()) {
      return durability_kvstore_.Delete(kTextIndexStr);
    }
    return durability_kvstore_.Put(kTextIndexStr, utils::Join(text_indices, "|"));
  }
  return true;
}

bool DurableMetadata::PersistUniqueConstraintCreation(LabelId label, const std::set<PropertyId> &properties) {
  const std::string entry = utils::GetKeyForUniqueConstraintsDurability(label, properties);

  if (auto unique_store = durability_kvstore_.Get(kUniqueConstraintsStr); unique_store.has_value()) {
    std::string &value = unique_store.value();
    value += "|" + entry;
    return durability_kvstore_.Put(kUniqueConstraintsStr, value);
  }
  return durability_kvstore_.Put(kUniqueConstraintsStr, entry);
}

bool DurableMetadata::PersistUniqueConstraintDeletion(LabelId label, const std::set<PropertyId> &properties) {
  const std::string entry = utils::GetKeyForUniqueConstraintsDurability(label, properties);

  if (auto unique_store = durability_kvstore_.Get(kUniqueConstraintsStr); unique_store.has_value()) {
    const std::string &value = unique_store.value();
    std::vector<std::string> unique_constraints = utils::Split(value, "|");
    std::erase(unique_constraints, entry);
    if (unique_constraints.empty()) {
      return durability_kvstore_.Delete(kUniqueConstraintsStr);
    }
    return durability_kvstore_.Put(kUniqueConstraintsStr, utils::Join(unique_constraints, "|"));
  }
  return true;
}

}  // namespace memgraph::storage
