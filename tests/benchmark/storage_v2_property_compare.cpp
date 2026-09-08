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

// Benchmarks the two comparisons an index makes over stored values: the one it
// walks the skip list with, which compares decoded values, and the one that
// confirms the entry it lands on, which compares the stored bytes without
// decoding them. Both run on every lookup.
//
// There is a case per type, so a change that costs something to tell types
// apart shows up on the types that gain nothing from it. The two values in each
// pair differ, as real index keys do: comparing a value with itself can exit
// early on a length or an equality check, which would measure the shortcut
// rather than the comparison.

#include <chrono>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <benchmark/benchmark.h>

#include "storage/v2/enum.hpp"
#include "storage/v2/point.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/temporal.hpp"

namespace {

using memgraph::storage::CoordinateReferenceSystem;
using memgraph::storage::Enum;
using memgraph::storage::EnumTypeId;
using memgraph::storage::EnumValueId;
using memgraph::storage::IntListTag;
using memgraph::storage::Point2d;
using memgraph::storage::Point3d;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyStore;
using memgraph::storage::PropertyValue;
using memgraph::storage::TemporalData;
using memgraph::storage::TemporalType;
using memgraph::storage::ZonedTemporalData;
using memgraph::storage::ZonedTemporalType;

/// Holds each element as a value of its own. This is what a query hands the
/// store, and the only representation a list with a non-numeric element can
/// use.
struct Boxed {};

/// Builds a list of the given elements.
///
/// The element type is deduced. The representation defaults to boxed; passing
/// `IntListTag` packs the elements instead.
template <typename Representation = Boxed, typename... Ts>
PropertyValue ListOf(Ts &&...values) {
  auto elements = std::vector<PropertyValue>{};
  elements.reserve(sizeof...(Ts));
  (elements.emplace_back(std::forward<Ts>(values)), ...);
  if constexpr (std::is_same_v<Representation, Boxed>) {
    return PropertyValue{std::move(elements)};
  } else {
    return PropertyValue{Representation{}, std::move(elements)};
  }
}

/// The two operands a comparison is asked about.
using Pair = std::pair<PropertyValue, PropertyValue>;

/// One type per pair of values.
///
/// A shape is a type rather than a string, so a misspelled name fails to
/// compile instead of registering a benchmark that aborts when it runs.
struct Int {
  static Pair Make() { return {PropertyValue{int64_t{7}}, PropertyValue{int64_t{9}}}; }
};

struct Double {
  static Pair Make() { return {PropertyValue{1.5}, PropertyValue{2.5}}; }
};

struct Bool {
  static Pair Make() { return {PropertyValue{false}, PropertyValue{true}}; }
};

struct String {
  static Pair Make() { return {PropertyValue{"alpha"}, PropertyValue{"bravo"}}; }
};

struct LongString {
  static Pair Make() { return {PropertyValue{std::string(64, 'a') + "1"}, PropertyValue{std::string(64, 'a') + "2"}}; }
};

struct Temporal {
  static Pair Make() {
    return {PropertyValue{TemporalData{TemporalType::Date, 10}}, PropertyValue{TemporalData{TemporalType::Date, 20}}};
  }
};

/// Both in the same zone, so the comparison is decided by the instant rather
/// than by the zone that precedes it.
struct ZonedTemporal {
  static Pair Make() {
    auto const zone = memgraph::utils::Timezone{std::chrono::minutes{60}};
    auto const at = [&zone](int64_t microseconds) {
      return PropertyValue{
          ZonedTemporalData{ZonedTemporalType::ZonedDateTime, memgraph::utils::AsSysTime(microseconds), zone}};
    };
    return {at(10), at(20)};
  }
};

struct Enumeration {
  static Pair Make() {
    return {PropertyValue{Enum{EnumTypeId{2}, EnumValueId{10}}}, PropertyValue{Enum{EnumTypeId{2}, EnumValueId{20}}}};
  }
};

/// Same reference system, so the comparison reaches the coordinates.
struct Point2 {
  static Pair Make() {
    return {PropertyValue{Point2d{CoordinateReferenceSystem::Cartesian_2d, 1.0, 2.0}},
            PropertyValue{Point2d{CoordinateReferenceSystem::Cartesian_2d, 1.0, 3.0}}};
  }
};

struct Point3 {
  static Pair Make() {
    return {PropertyValue{Point3d{CoordinateReferenceSystem::Cartesian_3d, 1.0, 2.0, 3.0}},
            PropertyValue{Point3d{CoordinateReferenceSystem::Cartesian_3d, 1.0, 2.0, 4.0}}};
  }
};

/// Same keys, so the comparison reaches the value that differs rather than
/// deciding on a key.
struct Map {
  static Pair Make() {
    auto const of = [](int64_t value) {
      return PropertyValue{PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue{int64_t{7}}},
                                                {PropertyId::FromUint(2), PropertyValue{value}}}};
    };
    return {of(8), of(9)};
  }
};

/// Same length, so the comparison has to reach the element that differs rather
/// than deciding on the lengths.
struct List {
  static Pair Make() { return {ListOf(1, 2, 3, 4), ListOf(1, 2, 3, 5)}; }
};

/// Different lengths, but an element separates them before the lengths are
/// reached.
struct ListOfUnlikeLengths {
  static Pair Make() { return {ListOf(1, 2), ListOf(1, 2, 3, 4)}; }
};

struct ListOfStrings {
  static Pair Make() { return {ListOf("a", "b"), ListOf("a", "c")}; }
};

/// One list packs its elements, the other boxes them. This is what a stored
/// column meets when a query hands it a list, and the only case compared
/// element by element rather than by representation.
struct PackedAgainstBoxed {
  static Pair Make() { return {ListOf<IntListTag>(1, 2, 3, 4), ListOf(1, 2, 3, 5)}; }
};

struct PackedAgainstBoxedUnlikeLengths {
  static Pair Make() { return {ListOf<IntListTag>(1, 2), ListOf(1, 2, 3, 4)}; }
};

/// Different types, which are ordered by type alone without either value being
/// read.
struct UnlikeTypes {
  static Pair Make() { return {PropertyValue{int64_t{7}}, PropertyValue{"seven"}}; }
};

// The comparison an index walks its entries with.
template <typename Shape>
void Ordering(benchmark::State &state) {
  auto const [lhs, rhs] = Shape::Make();
  for (auto _ : state) benchmark::DoNotOptimize(lhs <=> rhs);
  state.SetItemsProcessed(state.iterations());
}

// The same question asked of the stored bytes, which is how a scan confirms the
// entry it landed on.
template <typename Shape>
void EncodedSameness(benchmark::State &state) {
  auto const [lhs, rhs] = Shape::Make();
  auto const property = PropertyId::FromUint(1);
  auto store = PropertyStore{};
  store.SetProperty(property, lhs);
  for (auto _ : state) benchmark::DoNotOptimize(store.IsPropertyEqual(property, rhs));
  state.SetItemsProcessed(state.iterations());
}

// The same, but asked about the value the store already holds, which is what a
// lookup does when the candidate matches.
//
// Kept separate from the case above because the two differ when a mismatch is
// found early: a candidate of another length or another type is rejected before
// the bytes are compared, while a matching value is compared to the end. For
// fixed-width types the two cost the same.
template <typename Shape>
void EncodedSamenessHit(benchmark::State &state) {
  auto const [lhs, rhs] = Shape::Make();
  auto const property = PropertyId::FromUint(1);
  auto store = PropertyStore{};
  store.SetProperty(property, lhs);
  for (auto _ : state) benchmark::DoNotOptimize(store.IsPropertyEqual(property, lhs));
  state.SetItemsProcessed(state.iterations());
}

// The label comes from the same token that selects the shape, so the two cannot
// disagree.
#define SHAPE(bench, shape) BENCHMARK_TEMPLATE(bench, shape)->Name(#bench "/" #shape)->Unit(benchmark::kNanosecond)

#define FOR_EACH_SHAPE(bench)                    \
  SHAPE(bench, Int);                             \
  SHAPE(bench, Double);                          \
  SHAPE(bench, Bool);                            \
  SHAPE(bench, String);                          \
  SHAPE(bench, LongString);                      \
  SHAPE(bench, Temporal);                        \
  SHAPE(bench, ZonedTemporal);                   \
  SHAPE(bench, Enumeration);                     \
  SHAPE(bench, Point2);                          \
  SHAPE(bench, Point3);                          \
  SHAPE(bench, Map);                             \
  SHAPE(bench, List);                            \
  SHAPE(bench, ListOfUnlikeLengths);             \
  SHAPE(bench, ListOfStrings);                   \
  SHAPE(bench, PackedAgainstBoxed);              \
  SHAPE(bench, PackedAgainstBoxedUnlikeLengths); \
  SHAPE(bench, UnlikeTypes);

FOR_EACH_SHAPE(Ordering)
FOR_EACH_SHAPE(EncodedSameness)
FOR_EACH_SHAPE(EncodedSamenessHit)

}  // namespace

BENCHMARK_MAIN();
