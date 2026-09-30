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

#include "tests/property_based/typed_value_generators.hpp"

#include <algorithm>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "tests/unit/typed_value_shapes.hpp"

namespace memgraph::test::generators {

namespace {

using query::TypedValue;

/// One of a type's shapes. No accessor is passed, so asking for a type that
/// needs one would answer with nothing; `GraphFreeTypes` is what keeps that from
/// being asked.
auto FromShapes(TypedValue::Type type) -> rc::Gen<TypedValue> {
  return rc::gen::elementOf(shapes::ShapesOfType(type, nullptr));
}

template <typename T, typename Wrap>
auto Composed(Wrap wrap) -> rc::Gen<TypedValue> {
  return rc::gen::map(rc::gen::arbitrary<T>(), std::move(wrap));
}

auto Lists(int depth) -> rc::Gen<TypedValue> {
  return rc::gen::map(rc::gen::container<std::vector<TypedValue>>(AnyTypedValue(depth - 1)),
                      [](std::vector<TypedValue> elements) { return TypedValue(std::move(elements)); });
}

auto Maps(int depth) -> rc::Gen<TypedValue> {
  // A key drawn from a small set so that two drawn maps sometimes share keys and
  // sometimes do not, which is what makes matching by key worth checking.
  auto entries = rc::gen::container<std::vector<std::pair<std::string, TypedValue>>>(
      rc::gen::pair(rc::gen::element<std::string>("a", "b", "c", "d"), AnyTypedValue(depth - 1)));

  return rc::gen::map(std::move(entries), [](std::vector<std::pair<std::string, TypedValue>> drawn) {
    auto built = std::map<std::string, TypedValue>{};
    for (auto &&[key, value] : drawn) built.insert_or_assign(key, std::move(value));
    return TypedValue(std::move(built));
  });
}

bool NestsFurther(TypedValue::Type type) { return type == TypedValue::Type::List || type == TypedValue::Type::Map; }

}  // namespace

auto GraphFreeTypes() -> std::vector<TypedValue::Type> {
  auto types = std::vector<TypedValue::Type>{};
  std::ranges::copy_if(shapes::kEveryTypedValueType, std::back_inserter(types), [](auto type) {
    auto const holds_the_graph =
        std::ranges::find(shapes::kGraphTypedValueTypes, type) != shapes::kGraphTypedValueTypes.end();
    auto const has_no_shapes =
        std::ranges::find(shapes::kUnshapedTypedValueTypes, type) != shapes::kUnshapedTypedValueTypes.end();
    return !holds_the_graph && !has_no_shapes;
  });
  return types;
}

auto TypedValueOfType(TypedValue::Type type, int depth) -> rc::Gen<TypedValue> {
  switch (type) {
    case TypedValue::Type::Null:
      return rc::gen::just(TypedValue());
    case TypedValue::Type::Bool:
      return Composed<bool>([](bool drawn) { return TypedValue(drawn); });
    case TypedValue::Type::Int:
      return rc::gen::oneOf(FromShapes(type),
                            Composed<std::int64_t>([](std::int64_t drawn) { return TypedValue(drawn); }));
    case TypedValue::Type::Double:
      // Nothing drawn at random is a NaN or an infinity, so the shapes are the
      // only source of the values a comparison cannot order by magnitude.
      return rc::gen::oneOf(FromShapes(type), Composed<double>([](double drawn) { return TypedValue(drawn); }));
    case TypedValue::Type::String:
      return rc::gen::oneOf(FromShapes(type),
                            Composed<std::string>([](std::string drawn) { return TypedValue(std::move(drawn)); }));
    case TypedValue::Type::List:
      return depth > 0 ? Lists(depth) : rc::gen::just(TypedValue(std::vector<TypedValue>{}));
    case TypedValue::Type::Map:
      return depth > 0 ? Maps(depth) : rc::gen::just(TypedValue(std::map<std::string, TypedValue>{}));
    default:
      // Each remaining type is a fixed handful of interesting values rather than
      // a domain worth sampling, and the shapes already name them.
      return FromShapes(type);
  }
}

auto AnyTypedValue(int depth) -> rc::Gen<TypedValue> {
  auto types = GraphFreeTypes();
  if (depth <= 0) std::erase_if(types, NestsFurther);

  return rc::gen::mapcat(rc::gen::elementOf(std::move(types)),
                         [depth](TypedValue::Type type) { return TypedValueOfType(type, depth); });
}

}  // namespace memgraph::test::generators
