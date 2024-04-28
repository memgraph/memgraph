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

/// @file
#pragma once

#include <optional>

#include "query/typed_value.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/bound.hpp"
#include "utils/fnv.hpp"

namespace memgraph::query::plan {

/// A stand in class for `TDbAccessor` which provides memoized calls to
/// `VerticesCount`.
template <class TDbAccessor>
class VertexCountCache {
 public:
  explicit VertexCountCache(TDbAccessor *db) : db_(db) {}

  auto NameToLabel(const std::string &name) { return db_->NameToLabel(name); }
  auto NameToProperty(const std::string &name) { return db_->NameToProperty(name); }
  auto NameToEdgeType(const std::string &name) { return db_->NameToEdgeType(name); }

  int64_t VerticesCount() {
    if (!vertices_count_) vertices_count_ = db_->VerticesCount();
    return *vertices_count_;
  }

  int64_t VerticesCount(storage::LabelId label) {
    if (label_vertex_count_.find(label) == label_vertex_count_.end())
      label_vertex_count_[label] = db_->VerticesCount(label);
    return label_vertex_count_.at(label);
  }

  int64_t VerticesCount(storage::LabelId label, storage::PropertyId property) {
    auto key = std::make_pair(label, property);
    if (label_property_vertex_count_.find(key) == label_property_vertex_count_.end())
      label_property_vertex_count_[key] = db_->VerticesCount(label, property);
    return label_property_vertex_count_.at(key);
  }

  int64_t VerticesCount(storage::LabelId label, storage::PropertyId property, const storage::PropertyValue &value) {
    auto label_prop = std::make_pair(label, property);
    auto &value_vertex_count = property_value_vertex_count_[label_prop];
    // TODO: Why do we even need TypedValue in this whole file?
    TypedValue tv_value(value);
    if (value_vertex_count.find(tv_value) == value_vertex_count.end())
      value_vertex_count[tv_value] = db_->VerticesCount(label, property, value);
    return value_vertex_count.at(tv_value);
  }

  int64_t VerticesCount(storage::LabelId label, storage::PropertyId property,
                        const std::optional<utils::Bound<storage::PropertyValue>> &lower,
                        const std::optional<utils::Bound<storage::PropertyValue>> &upper) {
    auto label_prop = std::make_pair(label, property);
    auto &bounds_vertex_count = property_bounds_vertex_count_[label_prop];
    BoundsKey bounds = std::make_pair(lower, upper);
    if (bounds_vertex_count.find(bounds) == bounds_vertex_count.end())
      bounds_vertex_count[bounds] = db_->VerticesCount(label, property, lower, upper);
    return bounds_vertex_count.at(bounds);
  }

  bool LabelIndexExists(storage::LabelId label) { return db_->LabelIndexExists(label); }

  bool LabelPropertyIndexExists(storage::LabelId label, storage::PropertyId property) {
    return db_->LabelPropertyIndexExists(label, property);
  }

  bool EdgeTypeIndexExists(storage::EdgeTypeId edge_type) { return db_->EdgeTypeIndexExists(edge_type); }

  bool EdgeTypePropertyIndexExists(storage::EdgeTypeId edge_type, storage::PropertyId property) {
    return db_->EdgeTypePropertyIndexExists(edge_type, property);
  }

  std::optional<storage::LabelIndexStats> GetIndexStats(const storage::LabelId &label) const {
    return db_->GetIndexStats(label);
  }

  std::optional<storage::LabelPropertyIndexStats> GetIndexStats(const storage::LabelId &label,
                                                                const storage::PropertyId &property) const {
    return db_->GetIndexStats(label, property);
  }

 private:
  using LabelPropertyKey = std::pair<storage::LabelId, storage::PropertyId>;

  struct LabelPropertyHash {
    size_t operator()(const LabelPropertyKey &key) const {
      return utils::HashCombine<storage::LabelId, storage::PropertyId>{}(key.first, key.second);
    }
  };

  using BoundsKey = std::pair<std::optional<utils::Bound<storage::PropertyValue>>,
                              std::optional<utils::Bound<storage::PropertyValue>>>;

  struct BoundsHash {
    size_t operator()(const BoundsKey &key) const {
      const auto &maybe_lower = key.first;
      const auto &maybe_upper = key.second;
      query::TypedValue lower;
      query::TypedValue upper;
      if (maybe_lower) lower = TypedValue(maybe_lower->value());
      if (maybe_upper) upper = TypedValue(maybe_upper->value());
      query::TypedValue::Hash hash;
      return utils::HashCombine<size_t, size_t>{}(hash(lower), hash(upper));
    }
  };

  struct BoundsEqual {
    bool operator()(const BoundsKey &a, const BoundsKey &b) const {
      auto bound_equal = [](const auto &maybe_bound_a, const auto &maybe_bound_b) {
        if (maybe_bound_a && maybe_bound_b && maybe_bound_a->type() != maybe_bound_b->type()) return false;
        query::TypedValue bound_a;
        query::TypedValue bound_b;
        if (maybe_bound_a) bound_a = TypedValue(maybe_bound_a->value());
        if (maybe_bound_b) bound_b = TypedValue(maybe_bound_b->value());
        return query::TypedValue::BoolEqual{}(bound_a, bound_b);
      };
      return bound_equal(a.first, b.first) && bound_equal(a.second, b.second);
    }
  };

  TDbAccessor *db_;
  std::optional<int64_t> vertices_count_;
  std::unordered_map<storage::LabelId, int64_t> label_vertex_count_;
  std::unordered_map<LabelPropertyKey, int64_t, LabelPropertyHash> label_property_vertex_count_;
  std::unordered_map<
      LabelPropertyKey,
      std::unordered_map<query::TypedValue, int64_t, query::TypedValue::Hash, query::TypedValue::BoolEqual>,
      LabelPropertyHash>
      property_value_vertex_count_;
  std::unordered_map<LabelPropertyKey, std::unordered_map<BoundsKey, int64_t, BoundsHash, BoundsEqual>,
                     LabelPropertyHash>
      property_bounds_vertex_count_;
};

template <class TDbAccessor>
auto MakeVertexCountCache(TDbAccessor *db) {
  return VertexCountCache<TDbAccessor>(db);
}

}  // namespace memgraph::query::plan
