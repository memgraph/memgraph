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

#include "storage/v2/value_field.hpp"

#include <array>
#include <chrono>

#include "storage/v2/point.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/logging.hpp"
#include "utils/temporal.hpp"

namespace memgraph::storage {

namespace {

auto DateField(utils::Date const &date, ValueField field) -> std::optional<PropertyValue> {
  switch (field) {
    case ValueField::Year:
      return PropertyValue{static_cast<int64_t>(date.year)};
    case ValueField::Month:
      return PropertyValue{static_cast<int64_t>(date.month)};
    case ValueField::Day:
      return PropertyValue{static_cast<int64_t>(date.day)};
    default:
      return std::nullopt;
  }
}

auto LocalTimeField(utils::LocalTime const &time, ValueField field) -> std::optional<PropertyValue> {
  switch (field) {
    case ValueField::Hour:
      return PropertyValue{static_cast<int64_t>(time.hour)};
    case ValueField::Minute:
      return PropertyValue{static_cast<int64_t>(time.minute)};
    case ValueField::Second:
      return PropertyValue{static_cast<int64_t>(time.second)};
    case ValueField::Millisecond:
      return PropertyValue{static_cast<int64_t>(time.millisecond)};
    case ValueField::Microsecond:
      return PropertyValue{static_cast<int64_t>(time.microsecond)};
    default:
      return std::nullopt;
  }
}

/// A duration's fields are the whole days it spans and then what is left over, so its `day` is
/// a count of days and not a day of the month as a date's is.
auto DurationField(utils::Duration const &duration, ValueField field) -> std::optional<PropertyValue> {
  switch (field) {
    case ValueField::Day:
      return PropertyValue{duration.Days()};
    case ValueField::Hour:
      return PropertyValue{duration.SubDaysAsHours()};
    case ValueField::Minute:
      return PropertyValue{duration.SubDaysAsMinutes()};
    case ValueField::Second:
      return PropertyValue{duration.SubDaysAsSeconds()};
    case ValueField::Millisecond:
      return PropertyValue{duration.SubDaysAsMilliseconds()};
    case ValueField::Microsecond:
      return PropertyValue{duration.SubDaysAsMicroseconds()};
    case ValueField::Nanosecond:
      return PropertyValue{duration.SubDaysAsNanoseconds()};
    default:
      return std::nullopt;
  }
}

/// Read in the zone the moment is written in, which is what makes a year a year where it was
/// recorded rather than where it is read.
auto ZonedField(utils::ZonedDateTime const &zoned, ValueField field) -> std::optional<PropertyValue> {
  switch (field) {
    case ValueField::Year:
      return PropertyValue{zoned.LocalYear()};
    case ValueField::Month:
      return PropertyValue{static_cast<int64_t>(zoned.LocalMonth())};
    case ValueField::Day:
      return PropertyValue{static_cast<int64_t>(zoned.LocalDay())};
    case ValueField::Hour:
      return PropertyValue{zoned.LocalHour()};
    case ValueField::Minute:
      return PropertyValue{zoned.LocalMinute()};
    case ValueField::Second:
      return PropertyValue{zoned.LocalSecond()};
    case ValueField::Millisecond:
      return PropertyValue{zoned.LocalMillisecond()};
    case ValueField::Microsecond:
      return PropertyValue{zoned.LocalMicrosecond()};
    case ValueField::Timezone:
      return PropertyValue{zoned.GetTimezone().ToString()};
    default:
      return std::nullopt;
  }
}

/// Longitude and latitude name a geographic pair; a cartesian point has only x and y, which
/// is what the guards below are for.
auto PointField(CoordinateReferenceSystem crs, double x, double y, std::optional<double> z, ValueField field)
    -> std::optional<PropertyValue> {
  auto const geographic = crs == CoordinateReferenceSystem::WGS84_2d || crs == CoordinateReferenceSystem::WGS84_3d;
  switch (field) {
    case ValueField::X:
      return PropertyValue{x};
    case ValueField::Y:
      return PropertyValue{y};
    case ValueField::Z:
      if (!z) return std::nullopt;
      return PropertyValue{*z};
    case ValueField::Longitude:
      if (!geographic) return std::nullopt;
      return PropertyValue{x};
    case ValueField::Latitude:
      if (!geographic) return std::nullopt;
      return PropertyValue{y};
    case ValueField::Height:
      if (!geographic || !z) return std::nullopt;
      return PropertyValue{*z};
    case ValueField::Crs:
      return PropertyValue{std::string{CrsToString(crs)}};
    case ValueField::Srid:
      return PropertyValue{static_cast<int64_t>(CrsToSrid(crs).value_of())};
    default:
      return std::nullopt;
  }
}

}  // namespace

auto FieldNames() -> std::span<std::pair<std::string_view, ValueField> const> {
  using namespace std::string_view_literals;
  static constexpr auto kNames = std::array{
      std::pair{"year"sv, ValueField::Year},
      std::pair{"month"sv, ValueField::Month},
      std::pair{"day"sv, ValueField::Day},
      std::pair{"hour"sv, ValueField::Hour},
      std::pair{"minute"sv, ValueField::Minute},
      std::pair{"second"sv, ValueField::Second},
      std::pair{"millisecond"sv, ValueField::Millisecond},
      std::pair{"microsecond"sv, ValueField::Microsecond},
      std::pair{"nanosecond"sv, ValueField::Nanosecond},
      std::pair{"timezone"sv, ValueField::Timezone},
      std::pair{"x"sv, ValueField::X},
      std::pair{"y"sv, ValueField::Y},
      std::pair{"z"sv, ValueField::Z},
      std::pair{"longitude"sv, ValueField::Longitude},
      std::pair{"latitude"sv, ValueField::Latitude},
      std::pair{"height"sv, ValueField::Height},
      std::pair{"crs"sv, ValueField::Crs},
      std::pair{"srid"sv, ValueField::Srid},
  };
  return kNames;
}

auto FieldNamed(std::string_view name) -> std::optional<ValueField> {
  for (auto const &[known, field] : FieldNames()) {
    if (known == name) return field;
  }
  return std::nullopt;
}

auto ReadPropertyPath(PropertyValue const &value, std::span<PropertyId const> path, std::optional<ValueField> field)
    -> std::optional<PropertyValue> {
  DMG_ASSERT(!path.empty(), "A path names at least the record's own property");
  // A path of one property is that property, which is the value in hand.
  if (path.size() == 1) return value;

  // Every segment but the last names a key, so those are walked as maps.
  auto const *current = &value;
  for (auto const segment : path.subspan(1, path.size() - 2)) {
    if (!current->IsMap()) return std::nullopt;
    auto const &map = current->ValueMap();
    auto const at = map.find(segment);
    if (at == map.end()) return std::nullopt;
    current = &at->second;
  }

  if (current->IsMap()) {
    auto const &map = current->ValueMap();
    auto const at = map.find(path.back());
    if (at == map.end()) return std::nullopt;
    return PropertyValue{at->second};
  }
  if (!field) return std::nullopt;
  return ReadField(*current, *field);
}

auto ReadField(PropertyValue const &value, ValueField field) -> std::optional<PropertyValue> {
  if (value.IsTemporalData()) {
    auto const &temporal = value.ValueTemporalData();
    auto const micros = std::chrono::microseconds{temporal.microseconds};
    switch (temporal.type) {
      case TemporalType::Date:
        return DateField(utils::Date{micros}, field);
      case TemporalType::LocalTime:
        return LocalTimeField(utils::LocalTime{temporal.microseconds}, field);
      case TemporalType::LocalDateTime: {
        // A moment carries both a date's fields and a time's, so whichever of the two has the
        // field asked for answers.
        auto const local = utils::LocalDateTime{temporal.microseconds};
        if (auto const date = DateField(local.date(), field)) return date;
        return LocalTimeField(local.local_time(), field);
      }
      case TemporalType::Duration:
        return DurationField(utils::Duration{temporal.microseconds}, field);
    }
  }
  if (value.IsZonedTemporalData()) {
    auto const &zoned = value.ValueZonedTemporalData();
    return ZonedField(utils::ZonedDateTime{zoned.microseconds, zoned.timezone}, field);
  }
  if (value.IsPoint2d()) {
    auto const &point = value.ValuePoint2d();
    return PointField(point.crs(), point.x(), point.y(), std::nullopt, field);
  }
  if (value.IsPoint3d()) {
    auto const &point = value.ValuePoint3d();
    return PointField(point.crs(), point.x(), point.y(), point.z(), field);
  }
  return std::nullopt;
}

}  // namespace memgraph::storage
