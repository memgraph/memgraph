// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file
#pragma once

#include <optional>
#include <ranges>

#include "query/db_accessor.hpp"
#include "storage/v2/enum_store.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/bound.hpp"
#include "utils/exceptions.hpp"
#include "utils/fnv.hpp"

namespace memgraph::query::plan {

/// A stand in class for `TDbAccessor` which provides memoized calls to
/// `VerticesCount`.
class VertexCountCache {
 public:
  explicit VertexCountCache(DbAccessor *db) : db_(db) {}

  auto NameToLabel(const std::string &name) { return db_->NameToLabel(name); }
  auto NameToProperty(const std::string &name) { return db_->NameToProperty(name); }
  auto NameToEdgeType(const std::string &name) { return db_->NameToEdgeType(name); }
  auto GetEnumValue(std::string_view name, std::string_view value)
      -> utils::BasicResult<storage::EnumStorageError, storage::Enum> {
    return db_->GetEnumValue(name, value);
  }

  int64_t VerticesCount() {
    if (!vertices_count_) vertices_count_ = db_->VerticesCount();
    return *vertices_count_;
  }

  int64_t VerticesCount(storage::LabelId label) {
    if (label_vertex_count_.find(label) == label_vertex_count_.end())
      label_vertex_count_[label] = db_->VerticesCount(label);
    return label_vertex_count_.at(label);
  }

  int64_t VerticesCount(storage::LabelId label, std::span<storage::PropertyPath const> properties) {
    auto key = std::make_pair(label, std::vector(properties.begin(), properties.end()));
    if (label_properties_vertex_count_.find(key) == label_properties_vertex_count_.end())
      label_properties_vertex_count_[key] = db_->VerticesCount(label, properties);
    return label_properties_vertex_count_.at(key);
  }

  int64_t VerticesCount(storage::LabelId label, std::span<storage::PropertyPath const> properties,
                        std::span<storage::PropertyValueRange const> bounds) {
    auto key = std::make_tuple(label, std::vector(properties.begin(), properties.end()),
                               std::vector(bounds.begin(), bounds.end()));
    auto it = label_properties_ranges_vertex_count_.find(key);
    if (it != label_properties_ranges_vertex_count_.end()) {
      return it->second;
    } else {
      auto const count = db_->VerticesCount(label, properties, bounds);
      label_properties_ranges_vertex_count_[key] = count;
      return count;
    }
  }

  std::optional<int64_t> VerticesPointCount(storage::LabelId label, storage::PropertyId property) {
    auto key = std::make_pair(label, property);
    auto it = label_property_vertex_point_count_.find(key);
    if (it == label_property_vertex_point_count_.end()) {
      auto val = db_->VerticesPointCount(label, property);
      label_property_vertex_point_count_.emplace(key, val);
      return val;
    }
    return it->second;
  }

  int64_t EdgesCount(storage::EdgeTypeId edge_type) {
    if (edge_type_edge_count_.find(edge_type) == edge_type_edge_count_.end())
      edge_type_edge_count_[edge_type] = db_->EdgesCount(edge_type);
    return edge_type_edge_count_.at(edge_type);
  }

  int64_t EdgesCount(storage::EdgeTypeId edge_type, storage::PropertyId property) {
    auto key = std::make_pair(edge_type, property);
    if (edge_type_property_edge_count_.find(key) == edge_type_property_edge_count_.end())
      edge_type_property_edge_count_[key] = db_->EdgesCount(edge_type, property);
    return edge_type_property_edge_count_.at(key);
  }

  int64_t EdgesCount(storage::EdgeTypeId edge_type, storage::PropertyId property, const storage::PropertyValue &value) {
    auto edge_type_prop = std::make_pair(edge_type, property);
    auto &value_edge_count = property_value_edge_count_[edge_type_prop];
    if (value_edge_count.find(value) == value_edge_count.end())
      value_edge_count[value] = db_->EdgesCount(edge_type, property, value);
    return value_edge_count.at(value);
  }

  int64_t EdgesCount(storage::EdgeTypeId edge_type, storage::PropertyId property,
                     const std::optional<utils::Bound<storage::PropertyValue>> &lower,
                     const std::optional<utils::Bound<storage::PropertyValue>> &upper) {
    auto edge_type_prop = std::make_pair(edge_type, property);
    auto &bounds_edge_count = property_bounds_edge_count_[edge_type_prop];
    BoundsKey bounds = std::make_pair(lower, upper);
    if (bounds_edge_count.find(bounds) == bounds_edge_count.end())
      bounds_edge_count[bounds] = db_->EdgesCount(edge_type, property, lower, upper);
    return bounds_edge_count.at(bounds);
  }

  int64_t EdgesCount(storage::PropertyId property) {
    if (edge_property_edge_count_.find(property) == edge_property_edge_count_.end())
      edge_property_edge_count_[property] = db_->EdgesCount(property);
    return edge_property_edge_count_.at(property);
  }

  int64_t EdgesCount(storage::PropertyId property, const storage::PropertyValue &value) {
    auto &value_edge_count = edge_property_value_edge_count_[property];
    // TODO: Why do we even need TypedValue in this whole file?
    TypedValue tv_value(value, db_->GetStorageAccessor()->GetNameIdMapper());
    if (value_edge_count.find(tv_value) == value_edge_count.end())
      value_edge_count[tv_value] = db_->EdgesCount(property, value);
    return value_edge_count.at(tv_value);
  }

  int64_t EdgesCount(storage::PropertyId property, const std::optional<utils::Bound<storage::PropertyValue>> &lower,
                     const std::optional<utils::Bound<storage::PropertyValue>> &upper) {
    auto &bounds_edge_count = global_property_bounds_edge_count_[property];
    BoundsKey bounds = std::make_pair(lower, upper);
    if (bounds_edge_count.find(bounds) == bounds_edge_count.end())
      bounds_edge_count[bounds] = db_->EdgesCount(property, lower, upper);
    return bounds_edge_count.at(bounds);
  }

  bool LabelIndexExists(storage::LabelId label) { return db_->LabelIndexExists(label); }

  bool LabelPropertyIndexExists(storage::LabelId label, std::span<storage::PropertyPath const> properties) {
    return db_->LabelPropertyIndexExists(label, properties);
  }

  auto RelevantLabelPropertiesIndicesInfo(std::span<storage::LabelId const> labels,
                                          std::span<storage::PropertyPath const> properties) const
      -> std::vector<storage::LabelPropertiesIndicesInfo> {
    return db_->RelevantLabelPropertiesIndicesInfo(labels, properties);
  }

  bool EdgeTypeIndexExists(storage::EdgeTypeId edge_type) { return db_->EdgeTypeIndexExists(edge_type); }

  bool EdgeTypePropertyIndexExists(storage::EdgeTypeId edge_type, storage::PropertyId property) {
    return db_->EdgeTypePropertyIndexExists(edge_type, property);
  }

  bool EdgePropertyIndexExists(storage::PropertyId property) { return db_->EdgePropertyIndexExists(property); }

  bool PointIndexExists(storage::LabelId label, storage::PropertyId prop) const {
    return db_->PointIndexExists(label, prop);
  }

  std::optional<storage::LabelIndexStats> GetIndexStats(const storage::LabelId &label) const {
    return db_->GetIndexStats(label);
  }

  std::optional<storage::LabelPropertyIndexStats> GetIndexStats(const storage::LabelId &label,
                                                                std::span<storage::PropertyId const> properties) const {
    return db_->GetIndexStats(label, properties);
  }

  operator DbAccessor const &() const { return *db_; }

  auto GetStorageAccessor() const -> storage::Storage::Accessor * { return db_->GetStorageAccessor(); }

 private:
  using LabelPropertyKey = std::pair<storage::LabelId, storage::PropertyId>;
  using LabelPropertiesKey = std::pair<storage::LabelId, std::vector<storage::PropertyPath>>;
  using LabelPropertiesRangesKey =
      std::tuple<storage::LabelId, std::vector<storage::PropertyPath>, std::vector<storage::PropertyValueRange>>;
  using EdgeTypePropertyKey = std::pair<storage::EdgeTypeId, storage::PropertyId>;

  struct LabelPropertyHash {
    size_t operator()(const LabelPropertyKey &key) const {
      auto const &[label_id, property_id] = key;
      std::size_t seed = 0;
      boost::hash_combine(seed, std::hash<storage::LabelId>{}(label_id));
      boost::hash_combine(seed, std::hash<storage::PropertyId>{}(property_id));
      return seed;
    }
  };

  struct LabelPropertiesHash {
    size_t operator()(const LabelPropertiesKey &key) const {
      auto const &[label_id, property_ids] = key;
      std::size_t seed = 0;
      std::vector<storage::PropertyId> flat{property_ids | std::views::join | ranges::to<std::vector>()};
      boost::hash_combine(seed, std::hash<storage::LabelId>{}(label_id));
      boost::hash_combine(seed, utils::FnvCollection<std::vector<storage::PropertyId>, storage::PropertyId>{}(flat));
      return seed;
    }
  };

  struct LabelPropertiesRangesHash {
    size_t operator()(LabelPropertiesRangesKey const &key) const noexcept {
      auto const &[label_id, property_ids, property_ranges] = key;
      std::size_t seed = 0;
      std::vector<storage::PropertyId> flat{property_ids | std::views::join | ranges::to<std::vector>()};
      boost::hash_combine(seed, std::hash<storage::LabelId>{}(label_id));
      boost::hash_combine(seed, utils::FnvCollection<std::vector<storage::PropertyId>, storage::PropertyId>{}(flat));
      boost::hash_combine(seed,
                          utils::FnvCollection<std::vector<storage::PropertyValueRange>, storage::PropertyValueRange>{}(
                              property_ranges));
      return seed;
    }
  };

  struct LabelPropertiesRangesEqual {
    bool operator()(LabelPropertiesRangesKey const &lhs, LabelPropertiesRangesKey const &rhs) const noexcept {
      return lhs == rhs;
    }
  };

  struct EdgeTypePropertyHash {
    size_t operator()(const EdgeTypePropertyKey &key) const {
      auto const &[edge_type_id, property_id] = key;
      std::size_t seed = 0;
      boost::hash_combine(seed, std::hash<storage::EdgeTypeId>{}(edge_type_id));
      boost::hash_combine(seed, std::hash<storage::PropertyId>{}(property_id));
      return seed;
    }
  };

  using BoundsKey = std::pair<std::optional<utils::Bound<storage::PropertyValue>>,
                              std::optional<utils::Bound<storage::PropertyValue>>>;

  struct BoundsHash {
    size_t operator()(const BoundsKey &key) const {
      auto const &[maybe_lower, maybe_upper] = key;

      auto const lower = maybe_lower ? maybe_lower->value() : storage::PropertyValue{};
      auto const upper = maybe_upper ? maybe_upper->value() : storage::PropertyValue{};
      std::hash<storage::PropertyValue> hasher;

      std::size_t seed = 0;
      boost::hash_combine(seed, hasher(lower));
      boost::hash_combine(seed, hasher(upper));
      return seed;
    }
  };

  DbAccessor *db_;
  std::optional<int64_t> vertices_count_;
  std::unordered_map<storage::LabelId, int64_t> label_vertex_count_;
  std::unordered_map<storage::EdgeTypeId, int64_t> edge_type_edge_count_;
  std::unordered_map<LabelPropertiesKey, int64_t, LabelPropertiesHash> label_properties_vertex_count_;
  std::unordered_map<LabelPropertiesRangesKey, int64_t, LabelPropertiesRangesHash, LabelPropertiesRangesEqual>
      label_properties_ranges_vertex_count_;
  std::unordered_map<LabelPropertyKey, std::optional<int64_t>, LabelPropertyHash> label_property_vertex_point_count_;
  std::unordered_map<EdgeTypePropertyKey, int64_t, EdgeTypePropertyHash> edge_type_property_edge_count_;
  std::unordered_map<storage::PropertyId, int64_t> edge_property_edge_count_;
  std::unordered_map<EdgeTypePropertyKey, std::unordered_map<storage::PropertyValue, int64_t>, EdgeTypePropertyHash>
      property_value_edge_count_;
  std::unordered_map<storage::PropertyId, std::unordered_map<query::TypedValue, int64_t, query::TypedValue::Hash,
                                                             query::TypedValue::BoolEqual>>
      edge_property_value_edge_count_;
  std::unordered_map<LabelPropertyKey, std::unordered_map<BoundsKey, int64_t, BoundsHash>, LabelPropertyHash>
      property_bounds_vertex_count_;
  std::unordered_map<EdgeTypePropertyKey, std::unordered_map<BoundsKey, int64_t, BoundsHash>, EdgeTypePropertyHash>
      property_bounds_edge_count_;
  std::unordered_map<storage::PropertyId, std::unordered_map<BoundsKey, int64_t, BoundsHash>>
      global_property_bounds_edge_count_;
};

}  // namespace memgraph::query::plan
