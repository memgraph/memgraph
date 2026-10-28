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
#include <vector>

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

using NamedField = std::pair<std::string_view, ValueField>;

namespace {

using namespace std::string_view_literals;

/// What each kind of value carries. The one place these names are written: a reader holding a
/// value walks only its own kind's names, and the whole set below is their union.
constexpr auto kDateNames = std::array{
    NamedField{"year"sv, ValueField::Year},
    NamedField{"month"sv, ValueField::Month},
    NamedField{"day"sv, ValueField::Day},
};
constexpr auto kLocalTimeNames = std::array{
    NamedField{"hour"sv, ValueField::Hour},
    NamedField{"minute"sv, ValueField::Minute},
    NamedField{"second"sv, ValueField::Second},
    NamedField{"millisecond"sv, ValueField::Millisecond},
    NamedField{"microsecond"sv, ValueField::Microsecond},
};
constexpr auto kDurationNames = std::array{
    NamedField{"day"sv, ValueField::Day},
    NamedField{"hour"sv, ValueField::Hour},
    NamedField{"minute"sv, ValueField::Minute},
    NamedField{"second"sv, ValueField::Second},
    NamedField{"millisecond"sv, ValueField::Millisecond},
    NamedField{"microsecond"sv, ValueField::Microsecond},
    NamedField{"nanosecond"sv, ValueField::Nanosecond},
};
/// A zoned moment carries a date's and a time's fields, and the zone besides.
constexpr auto kZoneNames = std::array{
    NamedField{"timezone"sv, ValueField::Timezone},
};
constexpr auto kPointNames = std::array{
    NamedField{"x"sv, ValueField::X},
    NamedField{"y"sv, ValueField::Y},
    NamedField{"z"sv, ValueField::Z},
    NamedField{"longitude"sv, ValueField::Longitude},
    NamedField{"latitude"sv, ValueField::Latitude},
    NamedField{"height"sv, ValueField::Height},
    NamedField{"crs"sv, ValueField::Crs},
    NamedField{"srid"sv, ValueField::Srid},
};

auto FieldIn(std::span<NamedField const> names, std::string_view name) -> std::optional<ValueField> {
  for (auto const &[known, field] : names) {
    if (known == name) return field;
  }
  return std::nullopt;
}

}  // namespace

auto FieldNames() -> std::span<NamedField const> {
  // Their union, so a name reaches a caller holding ids by being carried by some value rather
  // than by being written down a second time. A name two kinds share appears once.
  static auto const kAll = [] {
    auto all = std::vector<NamedField>{};
    for (auto const &names : {std::span<NamedField const>{kDateNames},
                              std::span<NamedField const>{kLocalTimeNames},
                              std::span<NamedField const>{kDurationNames},
                              std::span<NamedField const>{kZoneNames},
                              std::span<NamedField const>{kPointNames}}) {
      for (auto const &named : names) {
        if (!FieldIn(all, named.first)) all.push_back(named);
      }
    }
    return all;
  }();
  return kAll;
}

auto ReadFieldNamed(utils::Date const &date, std::string_view name) -> std::optional<PropertyValue> {
  auto const field = FieldIn(kDateNames, name);
  return field ? DateField(date, *field) : std::nullopt;
}

auto ReadFieldNamed(utils::LocalTime const &time, std::string_view name) -> std::optional<PropertyValue> {
  auto const field = FieldIn(kLocalTimeNames, name);
  return field ? LocalTimeField(time, *field) : std::nullopt;
}

auto ReadFieldNamed(utils::LocalDateTime const &moment, std::string_view name) -> std::optional<PropertyValue> {
  if (auto const date = FieldIn(kDateNames, name)) return DateField(moment.date(), *date);
  auto const time = FieldIn(kLocalTimeNames, name);
  return time ? LocalTimeField(moment.local_time(), *time) : std::nullopt;
}

auto ReadFieldNamed(utils::Duration const &duration, std::string_view name) -> std::optional<PropertyValue> {
  auto const field = FieldIn(kDurationNames, name);
  return field ? DurationField(duration, *field) : std::nullopt;
}

auto ReadFieldNamed(utils::ZonedDateTime const &zoned, std::string_view name) -> std::optional<PropertyValue> {
  if (auto const date = FieldIn(kDateNames, name)) return ZonedField(zoned, *date);
  if (auto const time = FieldIn(kLocalTimeNames, name)) return ZonedField(zoned, *time);
  auto const zone = FieldIn(kZoneNames, name);
  return zone ? ZonedField(zoned, *zone) : std::nullopt;
}

auto ReadFieldNamed(Point2d const &point, std::string_view name) -> std::optional<PropertyValue> {
  auto const field = FieldIn(kPointNames, name);
  return field ? PointField(point.crs(), point.x(), point.y(), std::nullopt, *field) : std::nullopt;
}

auto ReadFieldNamed(Point3d const &point, std::string_view name) -> std::optional<PropertyValue> {
  auto const field = FieldIn(kPointNames, name);
  return field ? PointField(point.crs(), point.x(), point.y(), point.z(), *field) : std::nullopt;
}

auto FieldNamed(std::string_view name) -> std::optional<ValueField> { return FieldIn(FieldNames(), name); }

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
