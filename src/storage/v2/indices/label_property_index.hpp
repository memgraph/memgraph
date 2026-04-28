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

#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/index_order.hpp"
#include "storage/v2/indices/label_properties_indices_info.hpp"
#include "storage/v2/indices/label_property_index_entry.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/bound.hpp"

#include <cstdint>
#include <range/v3/view/enumerate.hpp>
#include <range/v3/view/transform.hpp>
#include <ranges>

namespace memgraph::storage {

struct ActiveIndicesUpdater;
struct LabelPropertyIndexActiveIndices;
struct LabelPropertyIndexAbortProcessor;

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
   */
  explicit PropertiesPermutationHelper(std::span<PropertyPath const> properties);

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

  /**
   * Inplace update the `extracted_values` with the current value if relevant
   * - if outer_prop_id is relevant
   * - updates all positions in `values` with the correctly extracted nested values from `extracted_values`
   */
  void Update(PropertyId outer_prop_id, PropertyValue const &value, std::vector<PropertyValue> &extracted_values) const;

  /** Compares the property with id `property_id` and value `value` against the
   * same property values in the `values` array. For every id in the index,
   * (which may occur multiple times for composite nested indices, such as `a.b`
   * and `a.c`) the vector will have a pair of the position of the property
   * (in monotonic property id order), and a boolean indicating whether the
   * property matches.
   */
  auto MatchesValue(PropertyId outer_prop_id, PropertyValue const &value,
                    IndexOrderedPropertyValues const &cmp_values) const -> std::vector<std::pair<std::ptrdiff_t, bool>>;

  /** Efficiently compares multiple values in the property store with the given
   * values. This returns a vector of boolean flags indicating per-element
   * equality (in monotonic property id order.)
   */
  auto MatchesValues(PropertyStore const &properties, IndexOrderedPropertyValues const &values) const
      -> std::vector<bool>;

  /** Returns an augmented view over the values in the given vector, where each
   * element is a tuple comprising: (position, [property id path], and value).
   */
  auto WithPropertyId(IndexOrderedPropertyValues const &values) const {
    return ranges::views::enumerate(sorted_properties_) | ranges::views::transform([&](auto &&p) {
             return std::tuple{p.first, std::cref(p.second), std::cref(values.values_[position_lookup_[p.first]])};
           });
  }

 private:
  std::vector<PropertyPath> sorted_properties_;
  std::vector<std::size_t> position_lookup_;
  permutation_cycles cycles_;
  std::map<PropertyId, std::vector<std::size_t>> grouped_by_outer_prop_id_;
};

using LabelPropertyIndexAbortableInfo =
    std::map<LabelId, std::map<PropertiesPaths const *, std::vector<std::pair<IndexOrderedPropertyValues, Vertex *>>>>;

class LabelPropertyIndex {
 public:
  // Because of composite index we need to track more info
  struct IndexInfo {
    PropertiesPaths const *new_properties_;
    PropertiesPermutationHelper const *helper_;

    friend auto operator<=>(IndexInfo const &, IndexInfo const &) = default;
  };

  using AbortableInfo = LabelPropertyIndexAbortableInfo;
  using AbortProcessor = LabelPropertyIndexAbortProcessor;
  using ActiveIndices = LabelPropertyIndexActiveIndices;

  LabelPropertyIndex() = default;
  LabelPropertyIndex(const LabelPropertyIndex &) = delete;
  LabelPropertyIndex(LabelPropertyIndex &&) = delete;
  LabelPropertyIndex &operator=(const LabelPropertyIndex &) = delete;
  LabelPropertyIndex &operator=(LabelPropertyIndex &&) = delete;

  virtual ~LabelPropertyIndex() = default;

  struct DropResult {
    bool dropped_asc = false;
    bool dropped_desc = false;

    explicit operator bool() const { return dropped_asc || dropped_desc; }
  };

  // `order == nullopt` drops both ASC and DESC entries for (label, properties).
  virtual DropResult DropIndex(LabelId label, std::vector<PropertyPath> const &properties,
                               ActiveIndicesUpdater const &updater, std::optional<IndexOrder> order = std::nullopt) = 0;
  virtual void DropGraphClearIndices() = 0;
  virtual auto GetActiveIndices() const -> std::shared_ptr<ActiveIndices> = 0;
};

struct LabelPropertyIndexAbortProcessor {
  // TODO: this is a filter for only relevant indicies? If so it should be based off the ActiveIndices
  //       + via constructor
  std::map<LabelId, std::map<PropertyId, std::vector<LabelPropertyIndex::IndexInfo>>> l2p;
  std::map<PropertyId, std::map<LabelId, std::vector<LabelPropertyIndex::IndexInfo>>> p2l;

  void CollectOnLabelRemoval(LabelId label, Vertex *vertex);
  void CollectOnPropertyChange(PropertyId propId, Vertex *vertex);

  // collection
  LabelPropertyIndexAbortableInfo cleanup_collection;
};

struct LabelPropertyIndexActiveIndices {
  virtual ~LabelPropertyIndexActiveIndices() = default;

  virtual void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) = 0;

  virtual void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update, const Transaction &tx) = 0;

  virtual void UpdateOnSetProperty(PropertyId property, const PropertyValue &old_value, const PropertyValue &new_value,
                                   Vertex *vertex, const Transaction &tx) = 0;

  virtual bool IndexReady(LabelId label, std::span<PropertyPath const> properties) const = 0;

  virtual bool IndexExists(LabelId label, std::span<PropertyPath const> properties) const = 0;

  virtual auto RelevantLabelPropertiesIndicesInfo(std::span<LabelId const> labels,
                                                  std::span<PropertyPath const> properties) const
      -> std::vector<LabelPropertiesIndicesInfo> = 0;

  virtual auto ListIndices(uint64_t start_timestamp) const -> std::vector<LabelPropertyIndexEntry> = 0;

  virtual auto ApproximateVertexCount(LabelId label, std::span<PropertyPath const> properties) const -> uint64_t = 0;

  virtual auto ApproximateVertexCount(LabelId label, std::span<PropertyPath const> properties,
                                      std::span<PropertyValue const> values) const -> uint64_t = 0;

  virtual auto ApproximateVertexCount(LabelId label, std::span<PropertyPath const> properties,
                                      std::span<PropertyValueRange const> bounds) const -> uint64_t = 0;

  virtual auto GetAbortProcessor() const -> LabelPropertyIndexAbortProcessor = 0;

  virtual void AbortEntries(LabelPropertyIndexAbortableInfo const &info, uint64_t start_timestamp) = 0;
};

}  // namespace memgraph::storage

namespace std {
template <>
struct hash<memgraph::storage::PropertyValueRange> {
  size_t operator()(const memgraph::storage::PropertyValueRange &pvr) const noexcept { return pvr.hash(); }
};

}  // namespace std
