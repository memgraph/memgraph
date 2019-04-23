/// @file
#pragma once

#include <optional>

#include "storage/common/types/property_value.hpp"
#include "storage/common/types/types.hpp"
#include "utils/bound.hpp"
#include "utils/hashing/fnv.hpp"

namespace query::plan {

/// A stand in class for `TDbAccessor` which provides memoized calls to
/// `VerticesCount`.
template <class TDbAccessor>
class VertexCountCache {
 public:
  VertexCountCache(TDbAccessor *db) : db_(db) {}

  auto Label(const std::string &name) { return db_->Label(name); }
  auto Property(const std::string &name) { return db_->Property(name); }
  auto EdgeType(const std::string &name) { return db_->EdgeType(name); }

  int64_t VerticesCount() {
    if (!vertices_count_) vertices_count_ = db_->VerticesCount();
    return *vertices_count_;
  }

  int64_t VerticesCount(storage::Label label) {
    if (label_vertex_count_.find(label) == label_vertex_count_.end())
      label_vertex_count_[label] = db_->VerticesCount(label);
    return label_vertex_count_.at(label);
  }

  int64_t VerticesCount(storage::Label label, storage::Property property) {
    auto key = std::make_pair(label, property);
    if (label_property_vertex_count_.find(key) ==
        label_property_vertex_count_.end())
      label_property_vertex_count_[key] = db_->VerticesCount(label, property);
    return label_property_vertex_count_.at(key);
  }

  int64_t VerticesCount(storage::Label label, storage::Property property,
                        const PropertyValue &value) {
    auto label_prop = std::make_pair(label, property);
    auto &value_vertex_count = property_value_vertex_count_[label_prop];
    if (value_vertex_count.find(value) == value_vertex_count.end())
      value_vertex_count[value] = db_->VerticesCount(label, property, value);
    return value_vertex_count.at(value);
  }

  int64_t VerticesCount(
      storage::Label label, storage::Property property,
      const std::optional<utils::Bound<PropertyValue>> &lower,
      const std::optional<utils::Bound<PropertyValue>> &upper) {
    auto label_prop = std::make_pair(label, property);
    auto &bounds_vertex_count = property_bounds_vertex_count_[label_prop];
    BoundsKey bounds = std::make_pair(lower, upper);
    if (bounds_vertex_count.find(bounds) == bounds_vertex_count.end())
      bounds_vertex_count[bounds] =
          db_->VerticesCount(label, property, lower, upper);
    return bounds_vertex_count.at(bounds);
  }

  bool LabelPropertyIndexExists(storage::Label label,
                                storage::Property property) {
    return db_->LabelPropertyIndexExists(label, property);
  }

 private:
  typedef std::pair<storage::Label, storage::Property> LabelPropertyKey;

  struct LabelPropertyHash {
    size_t operator()(const LabelPropertyKey &key) const {
      return utils::HashCombine<storage::Label, storage::Property>{}(
          key.first, key.second);
    }
  };

  typedef std::pair<std::optional<utils::Bound<PropertyValue>>,
                    std::optional<utils::Bound<PropertyValue>>>
      BoundsKey;

  struct BoundsHash {
    size_t operator()(const BoundsKey &key) const {
      const auto &maybe_lower = key.first;
      const auto &maybe_upper = key.second;
      query::TypedValue lower(query::TypedValue::Null);
      query::TypedValue upper(query::TypedValue::Null);
      if (maybe_lower) lower = maybe_lower->value();
      if (maybe_upper) upper = maybe_upper->value();
      query::TypedValue::Hash hash;
      return utils::HashCombine<size_t, size_t>{}(hash(lower), hash(upper));
    }
  };

  struct BoundsEqual {
    bool operator()(const BoundsKey &a, const BoundsKey &b) const {
      auto bound_equal = [](const auto &maybe_bound_a,
                            const auto &maybe_bound_b) {
        if (maybe_bound_a && maybe_bound_b &&
            maybe_bound_a->type() != maybe_bound_b->type())
          return false;
        query::TypedValue bound_a(query::TypedValue::Null);
        query::TypedValue bound_b(query::TypedValue::Null);
        if (maybe_bound_a) bound_a = maybe_bound_a->value();
        if (maybe_bound_b) bound_b = maybe_bound_b->value();
        return query::TypedValue::BoolEqual{}(bound_a, bound_b);
      };
      return bound_equal(a.first, b.first) && bound_equal(a.second, b.second);
    }
  };

  TDbAccessor *db_;
  std::optional<int64_t> vertices_count_;
  std::unordered_map<storage::Label, int64_t> label_vertex_count_;
  std::unordered_map<LabelPropertyKey, int64_t, LabelPropertyHash>
      label_property_vertex_count_;
  std::unordered_map<
      LabelPropertyKey,
      std::unordered_map<query::TypedValue, int64_t, query::TypedValue::Hash,
                         query::TypedValue::BoolEqual>,
      LabelPropertyHash>
      property_value_vertex_count_;
  std::unordered_map<
      LabelPropertyKey,
      std::unordered_map<BoundsKey, int64_t, BoundsHash, BoundsEqual>,
      LabelPropertyHash>
      property_bounds_vertex_count_;
};

template <class TDbAccessor>
auto MakeVertexCountCache(TDbAccessor *db) {
  return VertexCountCache<TDbAccessor>(db);
}

}  // namespace query::plan
