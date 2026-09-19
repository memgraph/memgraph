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

#include <array>
#include <initializer_list>
#include <limits>
#include <vector>

#include "storage/v2/property_value.hpp"

namespace memgraph::test::shapes {

/// A shape is a value chosen because a comparison, an encoding or an ordering
/// reaches it by a route no other value in its type reaches. A type's shapes
/// are its domain's boundaries, and for a container the nestings its elements
/// can be reached through.
///
/// A test that walks these gets its coverage from one place, so a type added to
/// `storage::PropertyValueType` is answered once rather than in each suite that
/// happens to enumerate values.

/// The doubles a comparison has to place without being able to order them by
/// magnitude.
inline constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
inline constexpr double kInfinity = std::numeric_limits<double>::infinity();

/// The integer above which no double holds an integer exactly, so a comparison
/// between the two answers on a rounded value unless it is written not to.
inline constexpr std::int64_t kExactlyRepresentable = std::int64_t{1} << 53;

/// Every type, in the order the enum declares them.
inline constexpr std::array kEveryType{
    storage::PropertyValueType::Null,
    storage::PropertyValueType::Bool,
    storage::PropertyValueType::Int,
    storage::PropertyValueType::Double,
    storage::PropertyValueType::String,
    storage::PropertyValueType::List,
    storage::PropertyValueType::Map,
    storage::PropertyValueType::TemporalData,
    storage::PropertyValueType::ZonedTemporalData,
    storage::PropertyValueType::Enum,
    storage::PropertyValueType::Point2d,
    storage::PropertyValueType::Point3d,
    storage::PropertyValueType::IntList,
    storage::PropertyValueType::DoubleList,
    storage::PropertyValueType::NumericList,
    storage::PropertyValueType::VectorIndexId,
};

/// The shapes of one type. Every value returned holds that type.
auto ShapesOfType(storage::PropertyValueType type) -> std::vector<storage::PropertyValue>;

/// Every type's shapes, in the order `kEveryType` gives.
auto EveryShape() -> std::vector<storage::PropertyValue>;

/// Every type's shapes but those of the named types. A property store takes
/// `{Null}`, since storing a Null removes the property and leaves nothing to
/// compare against.
auto EveryShapeExcept(std::initializer_list<storage::PropertyValueType> types) -> std::vector<storage::PropertyValue>;

}  // namespace memgraph::test::shapes
