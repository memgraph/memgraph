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

#pragma once

#include "storage/v2/id_types.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"

#include <cstdint>
#include <ranges>

namespace memgraph::storage {

/** Representation for a range of property values, which may be:
 * - BOUNDED: including only values between a lower and upper bounds. By setting
 *            both bounds to the same inclusive value, BOUNDED can also be used
 *            to check for exact property equality.
 * - IS_NOT_NULL: including every non-null value.
 * - INVALID: a range no property value can ever match, caused by different
 *            types for lower and upper bounds. For example:
 *            n.a > 42 and n.a < "hello".
 */
enum class PropertyRangeType { BOUNDED, IS_NOT_NULL, INVALID };
struct PropertyValueRange {
  using Type = PropertyRangeType;

  static auto Invalid(utils::Bound<PropertyValue> lower, utils::Bound<PropertyValue> upper) -> PropertyValueRange {
    return {Type::INVALID, std::move(lower), std::move(upper)};
  };

  static auto Bounded(std::optional<utils::Bound<PropertyValue>> lower,
                      std::optional<utils::Bound<PropertyValue>> upper) -> PropertyValueRange {
    return {Type::BOUNDED, std::move(lower), std::move(upper)};
  }
  static auto IsNotNull() -> PropertyValueRange { return {Type::IS_NOT_NULL, std::nullopt, std::nullopt}; }

  bool IsValueInRange(PropertyValue const &value) const {
    if (lower_) {
      if (lower_->IsInclusive()) {
        if (value < lower_->value()) return false;
      } else {
        if (value <= lower_->value()) return false;
      }
    }
    if (upper_) {
      if (upper_->IsInclusive()) {
        if (upper_->value() < value) return false;
      } else {
        if (upper_->value() <= value) return false;
      }
    }
    return true;
  }

  size_t hash() const noexcept;

  friend bool operator==(PropertyValueRange const &lhs, PropertyValueRange const &rhs) noexcept {
    return std::tie(lhs.type_, lhs.lower_, lhs.upper_) == std::tie(rhs.type_, rhs.lower_, rhs.upper_);
  }

  Type type_;
  std::optional<utils::Bound<PropertyValue>> lower_;
  std::optional<utils::Bound<PropertyValue>> upper_;

 private:
  PropertyValueRange(Type type, std::optional<utils::Bound<PropertyValue>> lower,
                     std::optional<utils::Bound<PropertyValue>> upper)
      : type_{type}, lower_{std::move(lower)}, upper_{std::move(upper)} {}
};

// These positions are in reference to the // labels + properties passed into
// `RelevantLabelPropertiesIndicesInfo`
struct LabelPropertiesIndicesInfo {
  std::size_t label_pos_;
  std::vector<int64_t> properties_pos_;  // -1 means missing
  LabelId label_;
  std::vector<PropertyPath> properties_;
};

// @TODO remove old implementation
using PropertiesIds = std::vector<PropertyId>;
using NestedPropertiesIds = std::vector<PropertyPath>;

struct IndexOrderedPropertyValues {
  IndexOrderedPropertyValues(std::vector<PropertyValue> value) : values_{std::move(value)} {}

  friend auto operator<=>(IndexOrderedPropertyValues const &, IndexOrderedPropertyValues const &) = default;

  std::vector<PropertyValue> values_;
};

/**
 * Due to the monotonic ordering of properties within the `PropertyStore`, we
 * are only able to read multiple values in a single pass if we do so in
 * increasing order of `PropertyId`.
 *
 * This class efficiently handles reading an abitrarily ordered list of
 * properties, rearranging the reads them so that the internal ordering
 * requirement is handled transparently.
 */
struct PropertiesPermutationHelper {
  using permutation_cycles = std::vector<std::vector<std::size_t>>;

  /**
   * @param properties The ids of the properties to be read, specified in the
   *                   required index order.
   * @TODO this remains whilst developing nested indices to ease, but
   * consider removing it afterwards.
   */
  explicit PropertiesPermutationHelper(std::span<PropertyPath const> properties);

  /**
   * @param properties The ids of the nested properties to be read, specified in the
   *                   required index order.
   */
  explicit PropertiesPermutationHelper(std::span<PropertyId const> properties);

  /** Rearranges a vector of monotonically ordered properties (as returned
   * by `Extract`) into the index order.
   */
  auto ApplyPermutation(std::vector<PropertyValue> values) const -> IndexOrderedPropertyValues;

  /* Extract one or more values from the given property store, returned in
   * increasing `PropertyId` order. Use `ApplyPermutation` to arrange the
   * properties into the index order.
   * @sa ApplyPermutation
   */
  auto Extract(PropertyStore const &properties) const -> std::vector<PropertyValue>;

  /** Compares the property with id `property_id` and value `value` against the
   * same property values in the `values` array. For every id in the index,
   * (which may occur multiple times for composite nested indices, such as `a.b`
   * and `a.c`) the vector will have a pair of the position of the property
   * within the values, and a boolean indicating whether the property matches.
   */
  auto MatchesValue(PropertyId property_id, PropertyValue const &value, IndexOrderedPropertyValues const &values) const
      -> std::vector<std::pair<std::ptrdiff_t, bool>>;

  /** Efficiently compares multiple values in the property store with the given
   * values. This returns a vector of boolean flags indicating per-element
   * equality.
   */
  auto MatchesValues(PropertyStore const &properties, IndexOrderedPropertyValues const &values) const
      -> std::vector<bool>;

  /** Returns an augmented view over the values in the given vector, where each
   * element is a tuple comprising: (position, [property id path], and value).
   */
  auto WithPropertyId(IndexOrderedPropertyValues const &values) const {
    return ranges::views::enumerate(sorted_properties_) | std::views::transform([&](auto &&p) {
             return std::tuple{p.first, p.second, std::cref(values.values_[position_lookup_[p.first]])};
           });
  }

 private:
  std::vector<PropertyPath> sorted_properties_;
  std::vector<PropertyId> sorted_properties_roots_;
  std::vector<std::size_t> position_lookup_;
  permutation_cycles cycles_;
};

class LabelPropertyIndex {
 public:
  // Because of composite index we need to track more info
  struct IndexInfo {
    NestedPropertiesIds const *new_properties_;
    PropertiesPermutationHelper const *helper_;

    friend auto operator<=>(IndexInfo const &, IndexInfo const &) = default;
  };
  using AbortableInfo =
      std::map<LabelId,
               std::map<NestedPropertiesIds const *, std::vector<std::pair<IndexOrderedPropertyValues, Vertex *>>>>;
  struct AbortProcessor {
    std::map<LabelId, std::map<PropertyId, std::vector<IndexInfo>>> l2p;
    std::map<PropertyId, std::map<LabelId, std::vector<IndexInfo>>> p2l;

    void collect_on_label_removal(LabelId label, Vertex *vertex) {
      const auto &it = l2p.find(label);
      if (it == l2p.end()) return;

      auto dedup = std::set<IndexInfo>{};
      for (const auto &[property, index_info] : it->second) {
        for (auto const &info : index_info) {
          dedup.insert(info);
        }
      }
      for (auto const &[properties, helper] : dedup) {
        auto current_values = helper->Extract(vertex->properties);
        // Only if current_values has at least one non-null value do we need to cleanup its index entry
        if (ranges::any_of(current_values, [](PropertyValue const &val) { return !val.IsNull(); })) {
          cleanup_collection[label][properties].emplace_back(
              helper->ApplyPermutation(std::move(current_values)).values_, vertex);
        }
      }
    }

    void collect_on_property_change(PropertyId propId, Vertex *vertex) {
      const auto &it = p2l.find(propId);
      if (it == p2l.end()) return;

      for (auto const &[label, index_info] : it->second) {
        if (!utils::Contains(vertex->labels, label)) continue;
        for (auto const &[properties, helper] : index_info) {
          auto current_values = helper->Extract(vertex->properties);
          // Only if current_values has at least one non-null value do we need to cleanup its index entry
          if (ranges::any_of(current_values, [](PropertyValue const &val) { return !val.IsNull(); })) {
            cleanup_collection[label][properties].emplace_back(
                helper->ApplyPermutation(std::move(current_values)).values_, vertex);
          }
        }
      }
    }

    // collection
    AbortableInfo cleanup_collection;

    void process(LabelPropertyIndex &index, uint64_t start_timestamp) {
      index.AbortEntries(cleanup_collection, start_timestamp);
    }
  };

  LabelPropertyIndex() = default;
  LabelPropertyIndex(const LabelPropertyIndex &) = delete;
  LabelPropertyIndex(LabelPropertyIndex &&) = delete;
  LabelPropertyIndex &operator=(const LabelPropertyIndex &) = delete;
  LabelPropertyIndex &operator=(LabelPropertyIndex &&) = delete;

  virtual ~LabelPropertyIndex() = default;

  virtual void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) = 0;

  // Not used for in-memory
  virtual void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update, const Transaction &tx) = 0;

  virtual void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                                   const Transaction &tx) = 0;

  virtual void AbortEntries(AbortableInfo const &, uint64_t start_timestamp) = 0;

  virtual bool DropIndex(LabelId label, std::vector<PropertyPath> const &properties) = 0;

  virtual bool IndexExists(LabelId label, std::span<PropertyPath const> properties) const = 0;

  virtual auto RelevantLabelPropertiesIndicesInfo(std::span<LabelId const> labels,
                                                  std::span<PropertyPath const> properties) const
      -> std::vector<LabelPropertiesIndicesInfo> = 0;

  virtual std::vector<std::pair<LabelId, std::vector<PropertyPath>>> ListIndices() const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyPath const> properties) const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyPath const> properties,
                                          std::span<PropertyValue const> values) const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyPath const> properties,
                                          std::span<PropertyValueRange const> bounds) const = 0;

  virtual void DropGraphClearIndices() = 0;
};

}  // namespace memgraph::storage

namespace std {
template <>
struct hash<memgraph::storage::PropertyValueRange> {
  size_t operator()(const memgraph::storage::PropertyValueRange &pvr) const noexcept { return pvr.hash(); }
};
}  // namespace std
