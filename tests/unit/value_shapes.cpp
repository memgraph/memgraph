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

#include "tests/unit/value_shapes.hpp"

#include <algorithm>
#include <string>

#include "storage/v2/id_types.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/temporal.hpp"

namespace memgraph::test::shapes {

namespace {

using storage::Enum;
using storage::EnumTypeId;
using storage::EnumValueId;
using storage::Point2d;
using storage::Point3d;
using storage::PropertyId;
using storage::PropertyValue;
using storage::PropertyValueType;
using storage::TemporalData;
using storage::TemporalType;
using storage::ZonedTemporalData;
using storage::ZonedTemporalType;
using enum storage::CoordinateReferenceSystem;

auto Boxed(std::vector<PropertyValue> elements) -> PropertyValue { return PropertyValue(std::move(elements)); }

auto Nulls() -> std::vector<PropertyValue> { return {PropertyValue()}; }

auto Booleans() -> std::vector<PropertyValue> { return {PropertyValue(false), PropertyValue(true)}; }

auto Integers() -> std::vector<PropertyValue> {
  return {
      PropertyValue(std::int64_t{0}),
      PropertyValue(std::int64_t{1}),
      PropertyValue(std::int64_t{-1}),
      PropertyValue(kExactlyRepresentable),
      PropertyValue(kExactlyRepresentable + 1),
      PropertyValue(-(kExactlyRepresentable + 1)),
      PropertyValue(std::numeric_limits<std::int64_t>::min()),
      PropertyValue(std::numeric_limits<std::int64_t>::max()),
  };
}

auto Doubles() -> std::vector<PropertyValue> {
  return {
      PropertyValue(0.0),
      PropertyValue(-0.0),
      PropertyValue(1.0),
      PropertyValue(-1.0),
      PropertyValue(static_cast<double>(kExactlyRepresentable)),
      PropertyValue(kInfinity),
      PropertyValue(-kInfinity),
      PropertyValue(kNaN),
      PropertyValue(std::numeric_limits<double>::lowest()),
      PropertyValue(std::numeric_limits<double>::max()),
      PropertyValue(std::numeric_limits<double>::denorm_min()),
  };
}

auto Strings() -> std::vector<PropertyValue> {
  // A store writes a string's length in as few bytes as it fits in, so the
  // lengths either side of each width are reached rather than one middling one.
  return {
      PropertyValue(std::string{}),
      PropertyValue(std::string{"a"}),
      PropertyValue(std::string{"A"}),
      PropertyValue(std::string(255, 'x')),
      PropertyValue(std::string(256, 'x')),
      PropertyValue(std::string(300, 'x')),
      PropertyValue(std::string{"a\0b", 3}),
      PropertyValue(std::string{"ćevapčići"}),
  };
}

auto Lists() -> std::vector<PropertyValue> {
  return {
      Boxed({}),
      Boxed({PropertyValue(std::int64_t{1})}),
      Boxed({PropertyValue(std::int64_t{1}), PropertyValue(std::int64_t{2})}),
      Boxed({PropertyValue(std::int64_t{2})}),
      Boxed({PropertyValue(1.0), PropertyValue(kNaN)}),
      Boxed({PropertyValue(std::string{"a"})}),
      Boxed({PropertyValue()}),
      Boxed({PropertyValue(), PropertyValue(std::int64_t{1})}),
      Boxed({Boxed({PropertyValue(kNaN)})}),
      Boxed({PropertyValue(PropertyValue::map_t{{PropertyId::FromInt(1), PropertyValue(kNaN)}})}),
  };
}

auto Maps() -> std::vector<PropertyValue> {
  return {
      PropertyValue(PropertyValue::map_t{}),
      PropertyValue(PropertyValue::map_t{{PropertyId::FromInt(1), PropertyValue(std::int64_t{1})}}),
      PropertyValue(PropertyValue::map_t{{PropertyId::FromInt(1), PropertyValue(std::int64_t{2})}}),
      PropertyValue(PropertyValue::map_t{{PropertyId::FromInt(2), PropertyValue(std::int64_t{1})}}),
      PropertyValue(PropertyValue::map_t{{PropertyId::FromInt(1), PropertyValue(kNaN)}}),
      PropertyValue(PropertyValue::map_t{{PropertyId::FromInt(1), PropertyValue()}}),
      PropertyValue(PropertyValue::map_t{{PropertyId::FromInt(1), PropertyValue(std::int64_t{1})},
                                         {PropertyId::FromInt(2), PropertyValue(std::string{"a"})}}),
  };
}

auto Temporals() -> std::vector<PropertyValue> {
  // The four kinds share one type, so a range over one of them is fenced by
  // where that kind sits inside the type rather than by the type itself.
  return {
      PropertyValue(TemporalData{TemporalType::Date, 0}),
      PropertyValue(TemporalData{TemporalType::Date, 1}),
      PropertyValue(TemporalData{TemporalType::LocalTime, 1}),
      PropertyValue(TemporalData{TemporalType::LocalDateTime, 1}),
      PropertyValue(TemporalData{TemporalType::Duration, 1}),
      PropertyValue(TemporalData{TemporalType::Duration, -1}),
  };
}

auto ZonedTemporals() -> std::vector<PropertyValue> {
  // A zone is kept either as a name or as an offset, and the two are laid out
  // differently.
  return {
      PropertyValue(
          ZonedTemporalData{ZonedTemporalType::ZonedDateTime, utils::AsSysTime(0), utils::Timezone("Etc/UTC")}),
      PropertyValue(ZonedTemporalData{
          ZonedTemporalType::ZonedDateTime, utils::AsSysTime(1), utils::Timezone("America/Los_Angeles")}),
      PropertyValue(ZonedTemporalData{
          ZonedTemporalType::ZonedDateTime, utils::AsSysTime(1), utils::Timezone(std::chrono::minutes{-330})}),
  };
}

auto Enums() -> std::vector<PropertyValue> {
  return {
      PropertyValue(Enum{EnumTypeId{0}, EnumValueId{0}}),
      PropertyValue(Enum{EnumTypeId{0}, EnumValueId{1}}),
      PropertyValue(Enum{EnumTypeId{1}, EnumValueId{0}}),
      PropertyValue(Enum{EnumTypeId{2}, EnumValueId{10'000}}),
  };
}

auto Points2d() -> std::vector<PropertyValue> {
  return {
      PropertyValue(Point2d{WGS84_2d, 1.0, 2.0}),
      PropertyValue(Point2d{Cartesian_2d, 1.0, 2.0}),
      PropertyValue(Point2d{Cartesian_2d, 0.0, 0.0}),
      PropertyValue(Point2d{WGS84_2d, 1.0, kNaN}),
  };
}

auto Points3d() -> std::vector<PropertyValue> {
  return {
      PropertyValue(Point3d{WGS84_3d, 1.0, 2.0, 3.0}),
      PropertyValue(Point3d{Cartesian_3d, 1.0, 2.0, 3.0}),
      PropertyValue(Point3d{Cartesian_3d, 0.0, 0.0, 0.0}),
      PropertyValue(Point3d{WGS84_3d, 1.0, 2.0, kNaN}),
  };
}

auto IntLists() -> std::vector<PropertyValue> {
  auto const tagged = [](std::vector<PropertyValue> elements) {
    return PropertyValue{storage::IntListTag{}, std::move(elements)};
  };
  return {
      tagged({}),
      tagged({PropertyValue(std::int64_t{1})}),
      tagged({PropertyValue(std::int64_t{1}), PropertyValue(std::int64_t{2})}),
      tagged({PropertyValue(std::int64_t{2})}),
  };
}

auto DoubleLists() -> std::vector<PropertyValue> {
  auto const tagged = [](std::vector<PropertyValue> elements) {
    return PropertyValue{storage::DoubleListTag{}, std::move(elements)};
  };
  return {
      tagged({}),
      tagged({PropertyValue(1.0)}),
      tagged({PropertyValue(1.0), PropertyValue(2.0)}),
      tagged({PropertyValue(1.0), PropertyValue(kNaN)}),
  };
}

auto NumericLists() -> std::vector<PropertyValue> {
  auto const tagged = [](std::vector<PropertyValue> elements) {
    return PropertyValue{storage::NumericListTag{}, std::move(elements)};
  };
  return {
      tagged({PropertyValue(std::int64_t{1}), PropertyValue(2.5)}),
      tagged({PropertyValue(std::int64_t{1}), PropertyValue(kNaN)}),
      tagged({PropertyValue(2.5), PropertyValue(std::int64_t{1})}),
  };
}

auto VectorIndexIds() -> std::vector<PropertyValue> {
  // A coordinate is a float, which is the one place a NaN is reached at a width
  // other than a double's.
  using Data = PropertyValue::VectorIndexIdData;
  using Ids = utils::small_vector<std::uint64_t>;
  using Coordinates = utils::small_vector<float>;
  auto constexpr kNaNCoordinate = std::numeric_limits<float>::quiet_NaN();

  return {
      PropertyValue(Data{.ids = Ids{}, .vector = Coordinates{}}),
      PropertyValue(Data{.ids = Ids{0}, .vector = Coordinates{0.0F, 1.0F}}),
      PropertyValue(Data{.ids = Ids{0, 1}, .vector = Coordinates{1.0F, 2.0F}}),
      PropertyValue(Data{.ids = Ids{0}, .vector = Coordinates{1.0F, kNaNCoordinate}}),
      PropertyValue(Data{.ids = Ids{0}, .vector = Coordinates{std::numeric_limits<float>::infinity()}}),
  };
}

}  // namespace

auto ShapesOfType(PropertyValueType type) -> std::vector<PropertyValue> {
  switch (type) {
    case PropertyValueType::Null:
      return Nulls();
    case PropertyValueType::Bool:
      return Booleans();
    case PropertyValueType::Int:
      return Integers();
    case PropertyValueType::Double:
      return Doubles();
    case PropertyValueType::String:
      return Strings();
    case PropertyValueType::List:
      return Lists();
    case PropertyValueType::Map:
      return Maps();
    case PropertyValueType::TemporalData:
      return Temporals();
    case PropertyValueType::ZonedTemporalData:
      return ZonedTemporals();
    case PropertyValueType::Enum:
      return Enums();
    case PropertyValueType::Point2d:
      return Points2d();
    case PropertyValueType::Point3d:
      return Points3d();
    case PropertyValueType::IntList:
      return IntLists();
    case PropertyValueType::DoubleList:
      return DoubleLists();
    case PropertyValueType::NumericList:
      return NumericLists();
    case PropertyValueType::VectorIndexId:
      return VectorIndexIds();
  }
}

auto EveryShape() -> std::vector<PropertyValue> { return EveryShapeExcept({}); }

auto EveryShapeExcept(std::initializer_list<PropertyValueType> types) -> std::vector<PropertyValue> {
  auto result = std::vector<PropertyValue>{};
  for (auto const type : kEveryType) {
    if (std::ranges::find(types, type) != types.end()) continue;
    auto group = ShapesOfType(type);
    result.insert(result.end(), std::make_move_iterator(group.begin()), std::make_move_iterator(group.end()));
  }
  return result;
}

}  // namespace memgraph::test::shapes
