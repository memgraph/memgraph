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
  std::string target_name;  // formatted: "Person:Student", "Person(age)", "age", or "" for DATABASE
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

  // PROPERTY: key is (sorted label qualifier, PropertyId).
  // Empty label qualifier = bare property with no label scope.
  // :Person(age) and :Label(age) are distinct entries.
  void SetProperty(std::span<LabelId const> label_qualifier, PropertyId prop, std::string_view desc) {
    property_descriptions_[{SortedIds(label_qualifier), prop}] = desc;
  }

  bool DeleteProperty(std::span<LabelId const> label_qualifier, PropertyId prop) {
    return property_descriptions_.erase({SortedIds(label_qualifier), prop}) > 0;
  }

  std::optional<std::string> GetProperty(std::span<LabelId const> label_qualifier, PropertyId prop) const {
    auto it = property_descriptions_.find({SortedIds(label_qualifier), prop});
    if (it == property_descriptions_.end()) return std::nullopt;
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
    result.reserve(label_descriptions_.size() + edge_type_descriptions_.size() + property_descriptions_.size() +
                   (database_description_ ? 1 : 0));

    for (auto const &[ids, desc] : label_descriptions_) {
      std::string name;
      for (auto id : ids) {
        if (!name.empty()) name += ':';
        name += mapper.IdToName(id.AsUint());
      }
      result.push_back({DescriptionTargetKind::LABEL, std::move(name), desc});
    }
    for (auto const &[id, desc] : edge_type_descriptions_) {
      result.push_back({DescriptionTargetKind::EDGE_TYPE, mapper.IdToName(id.AsUint()), desc});
    }
    for (auto const &[key, desc] : property_descriptions_) {
      auto const &[label_ids, prop_id] = key;
      std::string name;
      if (!label_ids.empty()) {
        for (auto id : label_ids) {
          if (!name.empty()) name += ':';
          name += mapper.IdToName(id.AsUint());
        }
        name += '(';
        name += mapper.IdToName(prop_id.AsUint());
        name += ')';
      } else {
        name = mapper.IdToName(prop_id.AsUint());
      }
      result.push_back({DescriptionTargetKind::PROPERTY, std::move(name), desc});
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
  static std::vector<LabelId> SortedIds(std::span<LabelId const> labels) {
    std::vector<LabelId> ids(labels.begin(), labels.end());
    std::ranges::sort(ids);
    return ids;
  }

  // Label combo key: sorted vector of LabelIds — :Person:Student is its own key.
  std::map<std::vector<LabelId>, std::string> label_descriptions_;
  std::map<EdgeTypeId, std::string> edge_type_descriptions_;
  // Property key: (sorted label qualifier, PropertyId); empty labels = bare property.
  std::map<std::pair<std::vector<LabelId>, PropertyId>, std::string> property_descriptions_;
  std::optional<std::string> database_description_;
};

}  // namespace memgraph::storage
