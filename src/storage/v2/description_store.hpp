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

#include <algorithm>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "storage/v2/id_types.hpp"

namespace memgraph::storage {

enum class DescriptionTargetKind : uint8_t { DATABASE, LABEL, EDGE_TYPE, LABEL_PROPERTY, EDGE_TYPE_PROPERTY, PROPERTY };

struct DescriptionEntry {
  DescriptionTargetKind kind;
  std::vector<LabelId> labels;
  EdgeTypeId edge_type{};
  PropertyId property{};
  std::string description;
};

class DescriptionStore {
 public:
  // LABEL: key is the sorted combo of label IDs — :Person:Student is its own entry, distinct from :Person or :Student.
  void SetLabel(std::span<LabelId const> labels, std::string_view desc) {
    label_descriptions_[SortedIds(labels)] = desc;
  }

  bool DeleteLabel(std::span<LabelId const> labels) { return label_descriptions_.erase(SortedIds(labels)) > 0; }

  std::optional<std::string> GetLabel(std::span<LabelId const> labels) const {
    auto it = label_descriptions_.find(SortedIds(labels));
    if (it == label_descriptions_.end()) return std::nullopt;
    return it->second;
  }

  void SetEdgeType(EdgeTypeId id, std::string_view desc) { edge_type_descriptions_[id] = desc; }

  bool DeleteEdgeType(EdgeTypeId id) { return edge_type_descriptions_.erase(id); }

  std::optional<std::string> GetEdgeType(EdgeTypeId id) const {
    auto it = edge_type_descriptions_.find(id);
    if (it == edge_type_descriptions_.end()) return std::nullopt;
    return it->second;
  }

  void SetLabelProperty(std::span<LabelId const> label_qualifier, PropertyId prop, std::string_view desc) {
    label_property_descriptions_[{SortedIds(label_qualifier), prop}] = desc;
  }

  bool DeleteLabelProperty(std::span<LabelId const> label_qualifier, PropertyId prop) {
    return label_property_descriptions_.erase({SortedIds(label_qualifier), prop}) > 0;
  }

  std::optional<std::string> GetLabelProperty(std::span<LabelId const> label_qualifier, PropertyId prop) const {
    auto it = label_property_descriptions_.find({SortedIds(label_qualifier), prop});
    if (it == label_property_descriptions_.end()) return std::nullopt;
    return it->second;
  }

  void SetEdgeTypeProperty(EdgeTypeId edge_type, PropertyId prop, std::string_view desc) {
    edge_type_property_descriptions_[{edge_type, prop}] = desc;
  }

  bool DeleteEdgeTypeProperty(EdgeTypeId edge_type, PropertyId prop) {
    return edge_type_property_descriptions_.erase({edge_type, prop});
  }

  std::optional<std::string> GetEdgeTypeProperty(EdgeTypeId edge_type, PropertyId prop) const {
    auto it = edge_type_property_descriptions_.find({edge_type, prop});
    if (it == edge_type_property_descriptions_.end()) return std::nullopt;
    return it->second;
  }

  void SetProperty(PropertyId prop, std::string_view desc) { property_descriptions_[prop] = desc; }

  bool DeleteProperty(PropertyId prop) { return property_descriptions_.erase(prop) > 0; }

  std::optional<std::string> GetProperty(PropertyId prop) const {
    auto it = property_descriptions_.find(prop);
    if (it == property_descriptions_.end()) return std::nullopt;
    return it->second;
  }

  void SetDatabase(std::string_view desc) { database_description_ = desc; }

  bool DeleteDatabase() {
    if (!database_description_) return false;
    database_description_.reset();
    return true;
  }

  std::optional<std::string> GetDatabase() const { return database_description_; }

  std::vector<DescriptionEntry> GetAll() const {
    std::vector<DescriptionEntry> result;
    const auto size = label_descriptions_.size() + edge_type_descriptions_.size() +
                      label_property_descriptions_.size() + edge_type_property_descriptions_.size() +
                      property_descriptions_.size() + (database_description_ ? 1 : 0);
    result.reserve(size);

    for (auto const &[ids, desc] : label_descriptions_) {
      result.push_back({.kind = DescriptionTargetKind::LABEL, .labels = ids, .description = desc});
    }
    for (auto const &[id, desc] : edge_type_descriptions_) {
      result.push_back({.kind = DescriptionTargetKind::EDGE_TYPE, .edge_type = id, .description = desc});
    }
    for (auto const &[key, desc] : label_property_descriptions_) {
      auto const &[label_ids, prop_id] = key;
      result.push_back({.kind = DescriptionTargetKind::LABEL_PROPERTY,
                        .labels = label_ids,
                        .property = prop_id,
                        .description = desc});
    }
    for (auto const &[key, desc] : edge_type_property_descriptions_) {
      auto const &[edge_type_id, prop_id] = key;
      result.push_back({.kind = DescriptionTargetKind::EDGE_TYPE_PROPERTY,
                        .edge_type = edge_type_id,
                        .property = prop_id,
                        .description = desc});
    }
    for (auto const &[prop_id, desc] : property_descriptions_) {
      result.push_back({.kind = DescriptionTargetKind::PROPERTY, .property = prop_id, .description = desc});
    }
    if (database_description_) {
      result.push_back({.kind = DescriptionTargetKind::DATABASE, .description = *database_description_});
    }
    return result;
  }

  void Clear() {
    label_descriptions_.clear();
    edge_type_descriptions_.clear();
    label_property_descriptions_.clear();
    edge_type_property_descriptions_.clear();
    property_descriptions_.clear();
    database_description_.reset();
  }

 private:
  static std::vector<LabelId> SortedIds(std::span<LabelId const> labels) {
    std::vector<LabelId> ids(labels.begin(), labels.end());
    std::ranges::sort(ids);
    return ids;
  }

  std::map<std::vector<LabelId>, std::string> label_descriptions_;
  std::map<EdgeTypeId, std::string> edge_type_descriptions_;
  std::map<std::pair<std::vector<LabelId>, PropertyId>, std::string> label_property_descriptions_;
  std::map<std::pair<EdgeTypeId, PropertyId>, std::string> edge_type_property_descriptions_;
  std::map<PropertyId, std::string> property_descriptions_;
  std::optional<std::string> database_description_;
};

}  // namespace memgraph::storage
