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

#include <optional>
#include <span>
#include <string_view>
#include <utility>

#include "storage/v2/id_types.hpp"
#include "storage/v2/point.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/temporal.hpp"

namespace memgraph::storage {

/// A field a value of its own carries, as against a key a map holds. A moment has a year and
/// a point has a longitude, and both are spelled the way a map key is, so a name alone does
/// not say which of the two is meant.
///
/// A field is always a number or a piece of text, never a map, so a name denoting one is
/// always the last of a path.
enum class ValueField : uint8_t {
  Year,
  Month,
  Day,
  Hour,
  Minute,
  Second,
  Millisecond,
  Microsecond,
  Nanosecond,
  Timezone,
  X,
  Y,
  Z,
  Longitude,
  Latitude,
  Height,
  Crs,
  Srid,
};

/// Which field `name` asks for, or nothing when no value carries a field of that name. Says
/// nothing about whether the value in hand has one.
auto FieldNamed(std::string_view name) -> std::optional<ValueField>;

/// Every name that denotes a field of any value, for a caller holding property ids rather than
/// names: it can ask what each of these names is the id of and so learn which id denotes which
/// field. The union of what each kind of value carries.
auto FieldNames() -> std::span<std::pair<std::string_view, ValueField> const>;

/// Reads the field `name` denotes off a value, or nothing when a value of that kind carries no
/// field of that name.
///
/// For a reader that has the value in hand, which is most of them. Naming a field this way
/// costs what reading it costs, because only the names that kind of value carries are
/// considered; naming it against every name any value carries costs several times more.
/// @{
auto ReadFieldNamed(utils::Date const &date, std::string_view name) -> std::optional<PropertyValue>;
auto ReadFieldNamed(utils::LocalTime const &time, std::string_view name) -> std::optional<PropertyValue>;
auto ReadFieldNamed(utils::LocalDateTime const &moment, std::string_view name) -> std::optional<PropertyValue>;
auto ReadFieldNamed(utils::Duration const &duration, std::string_view name) -> std::optional<PropertyValue>;
auto ReadFieldNamed(utils::ZonedDateTime const &zoned, std::string_view name) -> std::optional<PropertyValue>;
auto ReadFieldNamed(Point2d const &point, std::string_view name) -> std::optional<PropertyValue>;
auto ReadFieldNamed(Point3d const &point, std::string_view name) -> std::optional<PropertyValue>;
/// @}

/// Reads `field` off `value`, or nothing when that value carries no such field. A point given
/// in coordinates has no longitude, so asking for one reads as nothing rather than as an
/// answer of its own.
auto ReadField(PropertyValue const &value, ValueField field) -> std::optional<PropertyValue>;

/// Reads what `path` names out of `value`, which is the record's own property the path starts
/// at. Walks the maps the path names, then reads the last segment as a key of a map or, on
/// anything else, as `field`. Nothing when the path names nothing.
///
/// `field` is which field the last segment names, where it names one. Only the value in hand
/// settles whether the segment is a key or a field, so both have to be on offer here. Work out
/// `field` once for a path rather than per record: it takes the name behind a property id.
auto ReadPropertyPath(PropertyValue const &value, std::span<PropertyId const> path, std::optional<ValueField> field)
    -> std::optional<PropertyValue>;

}  // namespace memgraph::storage
