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

#include "tests/property_based/value_generators.hpp"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "tests/unit/value_shapes.hpp"

namespace memgraph::test::generators {

namespace {

using storage::PropertyId;
using storage::PropertyValue;
using storage::PropertyValueType;

/// One of a type's shapes. The group is a value here, so the generator owns it
/// and nothing it reads can go out of scope under it.
auto FromShapes(PropertyValueType type) -> rc::Gen<PropertyValue> {
  return rc::gen::elementOf(shapes::ShapesOfType(type));
}

template <typename T, typename Wrap>
auto Composed(Wrap wrap) -> rc::Gen<PropertyValue> {
  return rc::gen::map(rc::gen::arbitrary<T>(), std::move(wrap));
}

auto Lists(int depth) -> rc::Gen<PropertyValue> {
  return rc::gen::map(rc::gen::container<std::vector<PropertyValue>>(AnyValue(depth - 1)),
                      [](std::vector<PropertyValue> elements) { return PropertyValue(std::move(elements)); });
}

auto Maps(int depth) -> rc::Gen<PropertyValue> {
  // A key is drawn from a small range so that two drawn maps sometimes share
  // keys and sometimes do not, which is what makes matching by key rather than
  // by position worth checking.
  auto entries = rc::gen::container<std::vector<std::pair<int, PropertyValue>>>(
      rc::gen::pair(rc::gen::inRange(1, 5), AnyValue(depth - 1)));

  return rc::gen::map(std::move(entries), [](std::vector<std::pair<int, PropertyValue>> drawn) {
    auto built = PropertyValue::map_t{};
    for (auto &&[key, value] : drawn) built.insert_or_assign(PropertyId::FromInt(key), std::move(value));
    return PropertyValue(std::move(built));
  });
}

/// The types a container's element may take once there is no depth left, which
/// is every type that cannot nest further.
auto ScalarTypes() -> std::vector<PropertyValueType> {
  auto types = std::vector<PropertyValueType>{};
  std::ranges::copy_if(shapes::kEveryType, std::back_inserter(types), [](auto type) {
    return type != PropertyValueType::List && type != PropertyValueType::Map;
  });
  return types;
}

}  // namespace

auto ValueOfType(PropertyValueType type, int depth) -> rc::Gen<PropertyValue> {
  switch (type) {
    case PropertyValueType::Null:
      return rc::gen::just(PropertyValue());
    case PropertyValueType::Bool:
      return Composed<bool>([](bool drawn) { return PropertyValue(drawn); });
    case PropertyValueType::Int:
      return rc::gen::oneOf(FromShapes(type),
                            Composed<std::int64_t>([](std::int64_t drawn) { return PropertyValue(drawn); }));
    case PropertyValueType::Double:
      // Nothing drawn at random is a NaN or an infinity, so the shapes are the
      // only source of the values a comparison cannot order by magnitude.
      return rc::gen::oneOf(FromShapes(type), Composed<double>([](double drawn) { return PropertyValue(drawn); }));
    case PropertyValueType::String:
      return rc::gen::oneOf(FromShapes(type),
                            Composed<std::string>([](std::string drawn) { return PropertyValue(std::move(drawn)); }));
    case PropertyValueType::List:
      return depth > 0 ? Lists(depth) : rc::gen::just(PropertyValue(std::vector<PropertyValue>{}));
    case PropertyValueType::Map:
      return depth > 0 ? Maps(depth) : rc::gen::just(PropertyValue(PropertyValue::map_t{}));
    case PropertyValueType::TemporalData:
    case PropertyValueType::ZonedTemporalData:
    case PropertyValueType::Enum:
    case PropertyValueType::Point2d:
    case PropertyValueType::Point3d:
    case PropertyValueType::IntList:
    case PropertyValueType::DoubleList:
    case PropertyValueType::NumericList:
    case PropertyValueType::VectorIndexId:
      // Each of these is a fixed handful of interesting values rather than a
      // domain worth sampling, and the shapes already name them.
      return FromShapes(type);
  }
}

auto AnyScalar() -> rc::Gen<PropertyValue> {
  return rc::gen::mapcat(rc::gen::elementOf(ScalarTypes()),
                         [](PropertyValueType type) { return ValueOfType(type, 0); });
}

auto AnyValue(int depth) -> rc::Gen<PropertyValue> {
  if (depth <= 0) return AnyScalar();
  return rc::gen::mapcat(
      rc::gen::elementOf(std::vector<PropertyValueType>(shapes::kEveryType.begin(), shapes::kEveryType.end())),
      [depth](PropertyValueType type) { return ValueOfType(type, depth); });
}

}  // namespace memgraph::test::generators
