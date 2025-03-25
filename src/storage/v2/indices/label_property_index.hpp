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

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"

#include <cstdint>

namespace memgraph::storage {

/** Representation for a range of property values, which may be:
 * - BOUNDED: including only values between a lower and upper bounds
 * - IS_NOT_NULL: including every non-null value
 */
enum class PropertyRangeType { BOUNDED, IS_NOT_NULL, INVALID };
struct PropertyValueRange {
  using Type = PropertyRangeType;

  static auto InValid() -> PropertyValueRange { return {Type::INVALID, std::nullopt, std::nullopt}; };

  static auto Bounded(std::optional<utils::Bound<PropertyValue>> lower,
                      std::optional<utils::Bound<PropertyValue>> upper) -> PropertyValueRange {
    return {Type::BOUNDED, std::move(lower), std::move(upper)};
  }
  static auto IsNotNull() -> PropertyValueRange { return {Type::IS_NOT_NULL, std::nullopt, std::nullopt}; }

  bool value_valid(PropertyValue const &value) const {
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

  // TODO: make private?
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
  std::vector<PropertyId> properties_;
};

class LabelPropertyIndex {
 public:
  struct IndexStats {
    std::map<LabelId, std::vector<PropertyId>> l2p;
    std::map<PropertyId, std::vector<LabelId>> p2l;
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

  virtual bool DropIndex(LabelId label, std::vector<PropertyId> const &properties) = 0;

  virtual bool IndexExists(LabelId label, std::span<PropertyId const> properties) const = 0;

  virtual auto RelevantLabelPropertiesIndicesInfo(std::span<LabelId const> labels,
                                                  std::span<PropertyId const> properties) const
      -> std::vector<LabelPropertiesIndicesInfo> = 0;

  virtual std::vector<std::pair<LabelId, std::vector<PropertyId>>> ListIndices() const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyId const> properties) const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyId const> properties,
                                          std::span<PropertyValue const> values) const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyId const> properties,
                                          std::span<PropertyValueRange const> bounds) const = 0;

  virtual void DropGraphClearIndices() = 0;
};

}  // namespace memgraph::storage
