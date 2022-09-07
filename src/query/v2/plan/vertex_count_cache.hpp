// Copyright 2022 Memgraph Ltd.
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

#include "query/v2/bindings/typed_value.hpp"
#include "storage/v3/conversions.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/property_value.hpp"
#include "utils/bound.hpp"
#include "utils/fnv.hpp"

namespace memgraph::query::v2::plan {

/// A stand in class for `TDbAccessor` which provides memoized calls to
/// `VerticesCount`.
template <class TDbAccessor>
class VertexCountCache {
 public:
  VertexCountCache(TDbAccessor *db) : db_(db) {}

  auto NameToLabel(const std::string &name) { return db_->NameToLabel(name); }
  auto NameToProperty(const std::string &name) { return db_->NameToProperty(name); }
  auto NameToEdgeType(const std::string &name) { return db_->NameToEdgeType(name); }

  int64_t VerticesCount() {
    if (!vertices_count_) vertices_count_ = db_->VerticesCount();
    return *vertices_count_;
  }

  int64_t VerticesCount(storage::v3::LabelId label) {
    if (label_vertex_count_.find(label) == label_vertex_count_.end())
      label_vertex_count_[label] = db_->VerticesCount(label);
    return label_vertex_count_.at(label);
  }

  int64_t VerticesCount(storage::v3::LabelId label, storage::v3::PropertyId property) {
    auto key = std::make_pair(label, property);
    if (label_property_vertex_count_.find(key) == label_property_vertex_count_.end())
      label_property_vertex_count_[key] = db_->VerticesCount(label, property);
    return label_property_vertex_count_.at(key);
  }

  int64_t VerticesCount(storage::v3::LabelId label, storage::v3::PropertyId property,
                        const storage::v3::PropertyValue &value) {
    auto label_prop = std::make_pair(label, property);
    auto &value_vertex_count = property_value_vertex_count_[label_prop];
    // TODO: Why do we even need TypedValue in this whole file?
    auto tv_value(storage::v3::PropertyToTypedValue<TypedValue>(value));
    if (value_vertex_count.find(tv_value) == value_vertex_count.end())
      value_vertex_count[tv_value] = db_->VerticesCount(label, property, value);
    return value_vertex_count.at(tv_value);
  }

  int64_t VerticesCount(storage::v3::LabelId label, storage::v3::PropertyId property,
                        const std::optional<utils::Bound<storage::v3::PropertyValue>> &lower,
                        const std::optional<utils::Bound<storage::v3::PropertyValue>> &upper) {
    auto label_prop = std::make_pair(label, property);
    auto &bounds_vertex_count = property_bounds_vertex_count_[label_prop];
    BoundsKey bounds = std::make_pair(lower, upper);
    if (bounds_vertex_count.find(bounds) == bounds_vertex_count.end())
      bounds_vertex_count[bounds] = db_->VerticesCount(label, property, lower, upper);
    return bounds_vertex_count.at(bounds);
  }

  bool LabelIndexExists(storage::v3::LabelId label) { return db_->LabelIndexExists(label); }

  bool LabelPropertyIndexExists(storage::v3::LabelId label, storage::v3::PropertyId property) {
    return db_->LabelPropertyIndexExists(label, property);
  }

 private:
  typedef std::pair<storage::v3::LabelId, storage::v3::PropertyId> LabelPropertyKey;

  struct LabelPropertyHash {
    size_t operator()(const LabelPropertyKey &key) const {
      return utils::HashCombine<storage::v3::LabelId, storage::v3::PropertyId>{}(key.first, key.second);
    }
  };

  typedef std::pair<std::optional<utils::Bound<storage::v3::PropertyValue>>,
                    std::optional<utils::Bound<storage::v3::PropertyValue>>>
      BoundsKey;

  struct BoundsHash {
    size_t operator()(const BoundsKey &key) const {
      const auto &maybe_lower = key.first;
      const auto &maybe_upper = key.second;
      query::v2::TypedValue lower;
      query::v2::TypedValue upper;
      if (maybe_lower) lower = storage::v3::PropertyToTypedValue<TypedValue>(maybe_lower->value());
      if (maybe_upper) upper = storage::v3::PropertyToTypedValue<TypedValue>(maybe_upper->value());
      query::v2::TypedValue::Hash hash;
      return utils::HashCombine<size_t, size_t>{}(hash(lower), hash(upper));
    }
  };

  struct BoundsEqual {
    bool operator()(const BoundsKey &a, const BoundsKey &b) const {
      auto bound_equal = [](const auto &maybe_bound_a, const auto &maybe_bound_b) {
        if (maybe_bound_a && maybe_bound_b && maybe_bound_a->type() != maybe_bound_b->type()) return false;
        query::v2::TypedValue bound_a;
        query::v2::TypedValue bound_b;
        if (maybe_bound_a) bound_a = storage::v3::PropertyToTypedValue<TypedValue>(maybe_bound_a->value());
        if (maybe_bound_b) bound_b = storage::v3::PropertyToTypedValue<TypedValue>(maybe_bound_b->value());
        return query::v2::TypedValue::BoolEqual{}(bound_a, bound_b);
      };
      return bound_equal(a.first, b.first) && bound_equal(a.second, b.second);
    }
  };

  TDbAccessor *db_;
  std::optional<int64_t> vertices_count_;
  std::unordered_map<storage::v3::LabelId, int64_t> label_vertex_count_;
  std::unordered_map<LabelPropertyKey, int64_t, LabelPropertyHash> label_property_vertex_count_;
  std::unordered_map<
      LabelPropertyKey,
      std::unordered_map<query::v2::TypedValue, int64_t, query::v2::TypedValue::Hash, query::v2::TypedValue::BoolEqual>,
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

}  // namespace memgraph::query::v2::plan
