// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"

namespace memgraph::storage {

enum class DescriptionTargetKind : uint8_t { DATABASE = 0, LABEL = 1, EDGE_TYPE = 2, PROPERTY = 3 };

struct DescriptionEntry {
  DescriptionTargetKind kind;
  std::string target_name;  // label/edge_type/property name, or "" for DATABASE
  std::string description;
};

class DescriptionStore {
 public:
  void SetLabel(LabelId id, std::string desc) { label_descriptions_[id.AsUint()] = std::move(desc); }

  void SetEdgeType(EdgeTypeId id, std::string desc) { edge_type_descriptions_[id.AsUint()] = std::move(desc); }

  void SetProperty(PropertyId id, std::string desc) { property_descriptions_[id.AsUint()] = std::move(desc); }

  void SetDatabase(std::string desc) { database_description_ = std::move(desc); }

  bool DeleteLabel(LabelId id) { return label_descriptions_.erase(id.AsUint()) > 0; }

  bool DeleteEdgeType(EdgeTypeId id) { return edge_type_descriptions_.erase(id.AsUint()) > 0; }

  bool DeleteProperty(PropertyId id) { return property_descriptions_.erase(id.AsUint()) > 0; }

  bool DeleteDatabase() {
    if (!database_description_) return false;
    database_description_.reset();
    return true;
  }

  std::optional<std::string> GetLabel(LabelId id) const {
    auto it = label_descriptions_.find(id.AsUint());
    if (it == label_descriptions_.end()) return std::nullopt;
    return it->second;
  }

  std::optional<std::string> GetEdgeType(EdgeTypeId id) const {
    auto it = edge_type_descriptions_.find(id.AsUint());
    if (it == edge_type_descriptions_.end()) return std::nullopt;
    return it->second;
  }

  std::optional<std::string> GetProperty(PropertyId id) const {
    auto it = property_descriptions_.find(id.AsUint());
    if (it == property_descriptions_.end()) return std::nullopt;
    return it->second;
  }

  std::optional<std::string> GetDatabase() const { return database_description_; }

  std::vector<DescriptionEntry> GetAll(NameIdMapper &mapper) const {
    std::vector<DescriptionEntry> result;
    result.reserve(label_descriptions_.size() + edge_type_descriptions_.size() + property_descriptions_.size() +
                   (database_description_ ? 1 : 0));

    for (auto const &[id, desc] : label_descriptions_) {
      result.push_back({DescriptionTargetKind::LABEL, mapper.IdToName(id), desc});
    }
    for (auto const &[id, desc] : edge_type_descriptions_) {
      result.push_back({DescriptionTargetKind::EDGE_TYPE, mapper.IdToName(id), desc});
    }
    for (auto const &[id, desc] : property_descriptions_) {
      result.push_back({DescriptionTargetKind::PROPERTY, mapper.IdToName(id), desc});
    }
    if (database_description_) {
      result.push_back({DescriptionTargetKind::DATABASE, "", *database_description_});
    }
    return result;
  }

  void Clear() {
    label_descriptions_.clear();
    edge_type_descriptions_.clear();
    property_descriptions_.clear();
    database_description_.reset();
  }

 private:
  std::unordered_map<uint64_t, std::string> label_descriptions_;
  std::unordered_map<uint64_t, std::string> edge_type_descriptions_;
  std::unordered_map<uint64_t, std::string> property_descriptions_;
  std::optional<std::string> database_description_;
};

}  // namespace memgraph::storage
