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

#include <map>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"

namespace memgraph::storage {

enum class DescriptionTargetKind : uint8_t { DATABASE, LABEL, EDGE_TYPE, PROPERTY };

struct DescriptionEntry {
  DescriptionTargetKind kind;
  // Only meaningful when kind == PROPERTY: true for label-scoped, false for edge-type-scoped.
  bool is_label_scoped{false};
  // formatted: "Person:Student", "Person(age)", "KNOWS(weight)", or "" for DATABASE
  std::string target_name;
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

  // EDGE_TYPE: single EdgeTypeId key.
  void SetEdgeType(EdgeTypeId id, std::string_view desc) { edge_type_descriptions_[id] = desc; }

  bool DeleteEdgeType(EdgeTypeId id) { return edge_type_descriptions_.erase(id) > 0; }

  std::optional<std::string> GetEdgeType(EdgeTypeId id) const {
    auto it = edge_type_descriptions_.find(id);
    if (it == edge_type_descriptions_.end()) return std::nullopt;
    return it->second;
  }

  // PROPERTY (label-scoped): key is (sorted label qualifier, PropertyId).
  // :Person(age) and :Student(age) are distinct entries.
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

  // PROPERTY (edge-type-scoped): key is (EdgeTypeId, PropertyId).
  // :KNOWS(weight) and :SENT(weight) are distinct entries.
  void SetEdgeTypeProperty(EdgeTypeId edge_type, PropertyId prop, std::string_view desc) {
    edge_type_property_descriptions_[{edge_type, prop}] = desc;
  }

  bool DeleteEdgeTypeProperty(EdgeTypeId edge_type, PropertyId prop) {
    return edge_type_property_descriptions_.erase({edge_type, prop}) > 0;
  }

  std::optional<std::string> GetEdgeTypeProperty(EdgeTypeId edge_type, PropertyId prop) const {
    auto it = edge_type_property_descriptions_.find({edge_type, prop});
    if (it == edge_type_property_descriptions_.end()) return std::nullopt;
    return it->second;
  }

  // DATABASE: single optional string.
  void SetDatabase(std::string_view desc) { database_description_ = desc; }

  bool DeleteDatabase() {
    if (!database_description_) return false;
    database_description_.reset();
    return true;
  }

  std::optional<std::string> GetDatabase() const { return database_description_; }

  std::vector<DescriptionEntry> GetAll(NameIdMapper &mapper) const {
    std::vector<DescriptionEntry> result;
    result.reserve(label_descriptions_.size() + edge_type_descriptions_.size() + label_property_descriptions_.size() +
                   edge_type_property_descriptions_.size() + (database_description_ ? 1 : 0));

    for (auto const &[ids, desc] : label_descriptions_) {
      std::string name;
      for (auto id : ids) {
        if (!name.empty()) name += ':';
        name += mapper.IdToName(id.AsUint());
      }
      result.push_back({DescriptionTargetKind::LABEL, false, std::move(name), desc});
    }
    for (auto const &[id, desc] : edge_type_descriptions_) {
      result.push_back({DescriptionTargetKind::EDGE_TYPE, false, mapper.IdToName(id.AsUint()), desc});
    }
    for (auto const &[key, desc] : label_property_descriptions_) {
      auto const &[label_ids, prop_id] = key;
      std::string name;
      for (auto id : label_ids) {
        if (!name.empty()) name += ':';
        name += mapper.IdToName(id.AsUint());
      }
      name += '(';
      name += mapper.IdToName(prop_id.AsUint());
      name += ')';
      result.push_back({DescriptionTargetKind::PROPERTY, true, std::move(name), desc});
    }
    for (auto const &[key, desc] : edge_type_property_descriptions_) {
      auto const &[edge_type_id, prop_id] = key;
      std::string name = mapper.IdToName(edge_type_id.AsUint());
      name += '(';
      name += mapper.IdToName(prop_id.AsUint());
      name += ')';
      result.push_back({DescriptionTargetKind::PROPERTY, false, std::move(name), desc});
    }
    if (database_description_) {
      result.push_back({DescriptionTargetKind::DATABASE, false, std::string{}, *database_description_});
    }
    return result;
  }

  void Clear() {
    label_descriptions_.clear();
    edge_type_descriptions_.clear();
    label_property_descriptions_.clear();
    edge_type_property_descriptions_.clear();
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
  // Label-scoped: :Person(age) and :Student(age) are distinct.
  std::map<std::pair<std::vector<LabelId>, PropertyId>, std::string> label_property_descriptions_;
  // Edge-type-scoped: :KNOWS(weight) and :SENT(weight) are distinct.
  std::map<std::pair<EdgeTypeId, PropertyId>, std::string> edge_type_property_descriptions_;
  std::optional<std::string> database_description_;
};

}  // namespace memgraph::storage
